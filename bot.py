"""
Telegram Link Checker Bot — v2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Send a .txt file → get back only the alive links.
Fast async checking, live progress bar, inline controls.

Setup:
  pip install python-telegram-bot==13.15 aiohttp aiofiles
  export TELEGRAM_BOT_TOKEN=your_token_here
  python link_checker_bot.py

Env vars (optional):
  ALLOWED_USERS          comma-separated user IDs (leave empty = allow everyone)
  LINK_CHECKER_CONCURRENCY  parallel requests (default 300)
  REQUEST_TIMEOUT        seconds per request (default 15)
  PROGRESS_UPDATE_SECS   seconds between Telegram edits (default 3)
"""

import os
import re
import uuid
import json
import threading
import time
import asyncio
import aiohttp
import aiofiles
import logging
from pathlib import Path

from telegram import (
    Update, Bot,
    InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.ext import (
    Updater, CommandHandler, MessageHandler,
    Filters, CallbackContext, CallbackQueryHandler,
)
from telegram.error import RetryAfter, TimedOut

# ── logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────────────────────
_raw_allowed = os.environ.get("ALLOWED_USERS", "")
ALLOWED_USERS: set[int] = (
    {int(x) for x in _raw_allowed.split(",") if x.strip().isdigit()}
    if _raw_allowed.strip()
    else set()          # empty = allow everyone
)

CONCURRENCY        = int(os.environ.get("LINK_CHECKER_CONCURRENCY", "300"))
REQUEST_TIMEOUT    = int(os.environ.get("REQUEST_TIMEOUT", "15"))
PROGRESS_INTERVAL  = float(os.environ.get("PROGRESS_UPDATE_SECS", "3"))
JOB_DIR            = Path("jobs")
JOB_DIR.mkdir(exist_ok=True)

# active jobs registry  {job_id: LinkCheckerJob}
JOBS: dict[str, "LinkCheckerJob"] = {}
JOBS_LOCK = threading.Lock()

# ── helpers ───────────────────────────────────────────────────────────────────
URL_RE   = re.compile(r"https?://[^\s]+")
DOMAIN_RE = re.compile(r"([a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)+\.[a-z]{2,})(/[^\s]*)?")


def extract_url(line: str) -> str | None:
    m = URL_RE.search(line)
    if m:
        return m.group(0).rstrip(".,;\"'")
    m = DOMAIN_RE.search(line)
    if m:
        return m.group(0).rstrip(".,;\"'")
    return None


def to_http(url: str) -> str:
    return url if url.startswith(("http://", "https://")) else "https://" + url


def progress_bar(done: int, total: int, width: int = 20) -> str:
    if total == 0:
        return "[" + "░" * width + "] 0%"
    filled = int(width * done / total)
    bar    = "█" * filled + "░" * (width - filled)
    pct    = int(done * 100 / total)
    return f"[{bar}] {pct}%"


def safe_edit(bot: Bot, chat_id: int, msg_id: int, text: str):
    """Edit a message, ignoring rate-limit & identical-text errors."""
    try:
        bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
    except RetryAfter as e:
        time.sleep(e.retry_after + 0.5)
        try:
            bot.edit_message_text(chat_id=chat_id, message_id=msg_id, text=text)
        except Exception:
            pass
    except Exception:
        pass


def allowed(uid: int) -> bool:
    return not ALLOWED_USERS or uid in ALLOWED_USERS


# ── job class ─────────────────────────────────────────────────────────────────
class LinkCheckerJob(threading.Thread):
    def __init__(self, bot: Bot, chat_id: int, input_path: Path, verify_tls: bool = True):
        super().__init__(daemon=True)
        self.bot        = bot
        self.chat_id    = chat_id
        self.input_path = input_path
        self.verify_tls = verify_tls
        self.job_id     = uuid.uuid4().hex[:8]      # short for readability
        self.job_path   = JOB_DIR / self.job_id
        self.job_path.mkdir()
        self.stop_file  = self.job_path / "STOP"
        self.state_file = self.job_path / "state.json"
        self.out_path   = self.job_path / "alive.txt"
        self.out_path.write_text("", encoding="utf-8")
        self._done      = 0
        self._total     = 0
        self._alive     = 0
        self._msg_id    = None
        self._lock      = asyncio.Lock()            # used inside async context

    # ── public properties (thread-safe) ──────────────────────────────────────
    @property
    def is_stopped(self) -> bool:
        return self.stop_file.exists()

    def request_stop(self):
        self.stop_file.write_text("1")

    def status_text(self) -> str:
        d, t, a = self._done, self._total, self._alive
        bar = progress_bar(d, t)
        return (
            f"🔍 Job `{self.job_id}`\n"
            f"{bar}\n"
            f"`{d}/{t}` checked  •  ✅ `{a}` alive"
        )

    # ── thread entry ─────────────────────────────────────────────────────────
    def run(self):
        with JOBS_LOCK:
            JOBS[self.job_id] = self
        try:
            asyncio.run(self._main())
        except Exception as e:
            log.exception("Job %s crashed", self.job_id)
            self.bot.send_message(self.chat_id, f"❌ Job `{self.job_id}` crashed:\n{e}", parse_mode="Markdown")
        finally:
            with JOBS_LOCK:
                JOBS.pop(self.job_id, None)

    # ── async core ────────────────────────────────────────────────────────────
    async def _main(self):
        # --- parse urls -------------------------------------------------
        raw = self.input_path.read_text(encoding="utf-8", errors="ignore").splitlines()
        seen: dict[str, list[str]] = {}     # normalised_url -> original lines
        order: list[str] = []
        for line in raw:
            if not line.strip():
                continue
            url = extract_url(line)
            if not url:
                continue
            key = url.lower()
            if key not in seen:
                seen[key] = []
                order.append(key)
            seen[key].append(line)

        self._total = len(order)
        if self._total == 0:
            self.bot.send_message(self.chat_id, "⚠️ No URLs found in that file.")
            return

        # --- send initial progress message ------------------------------
        msg = self.bot.send_message(
            self.chat_id,
            f"🚀 Job `{self.job_id}` started — {self._total} unique URLs\n{progress_bar(0, self._total)}",
            parse_mode="Markdown",
            reply_markup=self._buttons(),
        )
        self._msg_id = msg.message_id

        # --- run async checks -------------------------------------------
        timeout  = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
        sem      = asyncio.Semaphore(CONCURRENCY)
        last_upd = time.time()

        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def worker(key: str):
                nonlocal last_upd
                if self.is_stopped:
                    return
                async with sem:
                    if self.is_stopped:
                        return
                    ok = await self._check(session, seen[key][0])
                    if ok:
                        async with self._lock:
                            self._alive += 1
                            async with aiofiles.open(self.out_path, "a", encoding="utf-8") as f:
                                for ln in seen[key]:
                                    await f.write(ln.rstrip("\n") + "\n")
                    self._done += 1
                    now = time.time()
                    if now - last_upd >= PROGRESS_INTERVAL:
                        last_upd = now
                        safe_edit(self.bot, self.chat_id, self._msg_id,
                                  self.status_text())
                        self._save_state()

            tasks = [asyncio.create_task(worker(k)) for k in order]
            await asyncio.gather(*tasks)

        # --- finalise ---------------------------------------------------
        self._save_state()
        stopped = self.is_stopped
        alive   = self._alive

        # final progress edit
        final_txt = (
            f"{'🛑 Stopped' if stopped else '✅ Done'} — Job `{self.job_id}`\n"
            f"{progress_bar(self._done, self._total)}\n"
            f"`{self._done}/{self._total}` checked  •  ✅ `{alive}` alive"
        )
        safe_edit(self.bot, self.chat_id, self._msg_id, final_txt)

        if alive == 0:
            self.bot.send_message(self.chat_id, "😔 No alive links found.")
            return

        # send results file
        try:
            with open(self.out_path, "rb") as f:
                self.bot.send_document(
                    self.chat_id,
                    document=f,
                    filename=f"alive_{self.job_id}.txt",
                    caption=f"✅ {alive} alive link{'s' if alive != 1 else ''} — Job `{self.job_id}`",
                    parse_mode="Markdown",
                )
        except Exception as e:
            log.error("Send doc failed: %s", e)
            self.bot.send_message(self.chat_id, f"⚠️ Couldn't send file: {e}")

    # ── url check ────────────────────────────────────────────────────────────
    async def _check(self, session: aiohttp.ClientSession, raw_url: str) -> bool:
        url = to_http(raw_url)
        for attempt_url in self._url_variants(url):
            try:
                async with session.head(
                    attempt_url, allow_redirects=True,
                    ssl=self.verify_tls,
                ) as r:
                    if r.status < 400:
                        return True
                    if r.status == 405:
                        # HEAD not allowed, try GET
                        async with session.get(
                            attempt_url, allow_redirects=True,
                            ssl=self.verify_tls,
                        ) as r2:
                            if r2.status < 400:
                                return True
            except Exception:
                pass
        return False

    @staticmethod
    def _url_variants(url: str) -> list[str]:
        """Try https first, then http fallback."""
        if url.startswith("https://"):
            return [url, "http://" + url[8:]]
        return [url]

    # ── helpers ──────────────────────────────────────────────────────────────
    def _save_state(self):
        self.state_file.write_text(
            json.dumps({"done": self._done, "total": self._total, "alive": self._alive}),
            encoding="utf-8",
        )

    def _buttons(self) -> InlineKeyboardMarkup:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("⏹ Stop",        callback_data=f"stop:{self.job_id}"),
            InlineKeyboardButton("📊 Status",      callback_data=f"status:{self.job_id}"),
            InlineKeyboardButton("📥 Get results", callback_data=f"get:{self.job_id}"),
        ]])


# ── telegram handlers ─────────────────────────────────────────────────────────
HELP = (
    "👋 *Link Checker Bot*\n\n"
    "Send me a `.txt` file — one URL (or line containing a URL) per line.\n"
    "I'll check each one and return only the alive ones.\n\n"
    "*Commands:*\n"
    "`/start` — this message\n"
    "`/jobs` — list running jobs\n"
    "`/stop <id>` — stop a job\n"
    "`/status <id>` — get job status\n"
    "`/get <id>` — download current results\n\n"
    "_Tip: add `insecure` anywhere in the file caption to skip TLS verification._"
)


def start(update: Update, context: CallbackContext):
    if not allowed(update.effective_user.id):
        return update.message.reply_text("⛔ Not authorised.")
    update.message.reply_text(HELP, parse_mode="Markdown")


def jobs_cmd(update: Update, context: CallbackContext):
    if not allowed(update.effective_user.id):
        return
    with JOBS_LOCK:
        if not JOBS:
            return update.message.reply_text("No running jobs.")
        lines = [f"• `{jid}` — {j._done}/{j._total} ({j._alive} alive)" for jid, j in JOBS.items()]
    update.message.reply_text("*Running jobs:*\n" + "\n".join(lines), parse_mode="Markdown")


def handle_document(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if not allowed(uid):
        return update.message.reply_text("⛔ Not authorised.")

    doc      = update.message.document
    caption  = update.message.caption or ""
    chat_id  = update.effective_chat.id
    fname    = doc.file_name or "input.txt"

    save_path = JOB_DIR / f"upload_{uuid.uuid4().hex}_{fname}"
    context.bot.get_file(doc.file_id).download(str(save_path))

    verify_tls = "insecure" not in caption.lower()
    job = LinkCheckerJob(context.bot, chat_id, save_path, verify_tls=verify_tls)
    job.start()

    update.message.reply_text(
        f"✅ Job `{job.job_id}` queued for *{fname}*\n_Progress will appear above as I check…_",
        parse_mode="Markdown",
    )


def stop_cmd(update: Update, context: CallbackContext):
    if not allowed(update.effective_user.id):
        return
    if not context.args:
        return update.message.reply_text("Usage: `/stop <job_id>`", parse_mode="Markdown")
    jid = context.args[0]
    with JOBS_LOCK:
        job = JOBS.get(jid)
    if job:
        job.request_stop()
        update.message.reply_text(f"🛑 Stop requested for `{jid}`.", parse_mode="Markdown")
    else:
        jpath = JOB_DIR / jid
        if jpath.exists():
            (jpath / "STOP").write_text("1")
            update.message.reply_text(f"🛑 Stop signal written for `{jid}`.", parse_mode="Markdown")
        else:
            update.message.reply_text("❓ Unknown job id.")


def status_cmd(update: Update, context: CallbackContext):
    if not allowed(update.effective_user.id):
        return
    if not context.args:
        return update.message.reply_text("Usage: `/status <job_id>`", parse_mode="Markdown")
    jid = context.args[0]
    with JOBS_LOCK:
        job = JOBS.get(jid)
    if job:
        return update.message.reply_text(job.status_text(), parse_mode="Markdown")
    state_file = JOB_DIR / jid / "state.json"
    if state_file.exists():
        st = json.loads(state_file.read_text())
        d, t, a = st.get("done", 0), st.get("total", 0), st.get("alive", 0)
        update.message.reply_text(
            f"Job `{jid}` (finished)\n{progress_bar(d,t)}\n`{d}/{t}` • ✅ `{a}` alive",
            parse_mode="Markdown",
        )
    else:
        update.message.reply_text("❓ Unknown job id.")


def get_cmd(update: Update, context: CallbackContext):
    if not allowed(update.effective_user.id):
        return
    if not context.args:
        return update.message.reply_text("Usage: `/get <job_id>`", parse_mode="Markdown")
    jid = context.args[0]
    out = JOB_DIR / jid / "alive.txt"
    if not out.exists() or out.stat().st_size == 0:
        return update.message.reply_text("No results yet (or file is empty).")
    with open(out, "rb") as f:
        update.message.reply_document(document=f, filename=f"alive_{jid}.txt")


def button_callback(update: Update, context: CallbackContext):
    q = update.callback_query
    q.answer()
    parts = (q.data or "").split(":", 1)
    if len(parts) != 2:
        return
    action, jid = parts

    if action == "stop":
        with JOBS_LOCK:
            job = JOBS.get(jid)
        if job:
            job.request_stop()
            q.edit_message_text(f"🛑 Stop requested for job `{jid}`.", parse_mode="Markdown")
        else:
            (JOB_DIR / jid / "STOP").write_text("1")
            q.edit_message_text(f"🛑 Stop signal sent for `{jid}`.", parse_mode="Markdown")

    elif action == "status":
        with JOBS_LOCK:
            job = JOBS.get(jid)
        if job:
            q.edit_message_text(job.status_text(), parse_mode="Markdown", reply_markup=job._buttons())
        else:
            state_file = JOB_DIR / jid / "state.json"
            if state_file.exists():
                st = json.loads(state_file.read_text())
                d, t, a = st.get("done", 0), st.get("total", 0), st.get("alive", 0)
                q.edit_message_text(
                    f"Job `{jid}` (finished)\n{progress_bar(d,t)}\n`{d}/{t}` • ✅ `{a}` alive",
                    parse_mode="Markdown",
                )
            else:
                q.edit_message_text("❓ Unknown job.")

    elif action == "get":
        out = JOB_DIR / jid / "alive.txt"
        if not out.exists() or out.stat().st_size == 0:
            q.edit_message_text("No results yet.")
            return
        with open(out, "rb") as f:
            context.bot.send_document(q.message.chat_id, document=f, filename=f"alive_{jid}.txt")


# ── main ─────────────────────────────────────────────────────────────────────
def main():
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        print("❌  Set TELEGRAM_BOT_TOKEN first.")
        return

    updater = Updater(token=token, use_context=True)
    dp      = updater.dispatcher

    dp.add_handler(CommandHandler("start",  start))
    dp.add_handler(CommandHandler("jobs",   jobs_cmd))
    dp.add_handler(CommandHandler("stop",   stop_cmd))
    dp.add_handler(CommandHandler("status", status_cmd))
    dp.add_handler(CommandHandler("get",    get_cmd))
    dp.add_handler(MessageHandler(Filters.document, handle_document))
    dp.add_handler(CallbackQueryHandler(button_callback))

    log.info("Bot starting…")
    updater.start_polling(drop_pending_updates=True)
    updater.idle()


if __name__ == "__main__":
    main()
