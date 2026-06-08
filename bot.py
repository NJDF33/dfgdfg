import os
import re
import uuid
import json
import threading
import time
from pathlib import Path
import requests
from telegram import Update, Bot, InputFile
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, CallbackContext

# Configuration
ALLOWED_USERS = {7734779979, 8353705144}
JOB_DIR = Path("jobs")
JOB_DIR.mkdir(exist_ok=True)


def extract_url_from_line(line: str):
    # Prefer explicit http/https URLs
    m = re.search(r"https?://\S+", line)
    if m:
        return m.group(0).rstrip('\n')
    # Fallback: find domain-like pattern with optional path
    m = re.search(r"([a-zA-Z0-9.-]+\.[a-z]{2,6}(?:/[^:\s]*)?)", line)
    if m:
        return m.group(1).rstrip('\n')
    return None


def normalize_for_request(url: str):
    if url.startswith("http://") or url.startswith("https://"):
        return url
    # try https first
    return "https://" + url


class LinkCheckerJob(threading.Thread):
    def __init__(self, bot: Bot, chat_id: int, input_path: Path, verify_tls: bool = True):
        super().__init__(daemon=True)
        self.bot = bot
        self.chat_id = chat_id
        self.input_path = input_path
        self.verify_tls = verify_tls
        self.job_id = uuid.uuid4().hex
        self.job_path = JOB_DIR / self.job_id
        self.job_path.mkdir()
        self.stop_requested = False
        self.state_file = self.job_path / "state.json"

    def run(self):
        try:
            self._run()
        except Exception as e:
            self.bot.send_message(self.chat_id, f"Job {self.job_id} failed: {e}")

    def _run(self):
        lines = self.input_path.read_text(encoding='utf-8', errors='ignore').splitlines()
        entries = []  # tuples (original_line, extracted_url)
        for ln in lines:
            if not ln.strip():
                continue
            url = extract_url_from_line(ln)
            if url:
                entries.append((ln, url))

        # dedup by normalized url only, but keep original lines for output
        mapping = {}
        order = []
        for orig, url in entries:
            key = url.lower()
            if key not in mapping:
                mapping[key] = {
                    'url': url,
                    'lines': [orig]
                }
                order.append(key)
            else:
                mapping[key]['lines'].append(orig)

        total = len(order)
        if total == 0:
            self.bot.send_message(self.chat_id, f"Job {self.job_id}: no URLs found in file.")
            return

        msg = self.bot.send_message(self.chat_id, f"Job {self.job_id} started: 0/{total} (0%)")
        results = []
        processed = 0

        # Load checkpoint if exists
        if self.state_file.exists():
            state = json.loads(self.state_file.read_text())
            processed = state.get('processed', 0)
            results = state.get('results', [])

        for idx in range(processed, total):
            # check for external stop flag
            if (self.job_path / 'STOP').exists():
                self._save_state(idx, results)
                self.bot.send_message(self.chat_id, f"Job {self.job_id} stopped at {idx}/{total}.")
                self._send_partial_results(results)
                return

            key = order[idx]
            url = mapping[key]['url']
            ok = self._check_url(url)
            if ok:
                # include all original lines for this url
                for l in mapping[key]['lines']:
                    results.append(l)

            processed = idx + 1
            pct = int(processed * 100 / total)
            try:
                self.bot.edit_message_text(chat_id=self.chat_id, message_id=msg.message_id,
                                           text=f"Job {self.job_id} progress: {processed}/{total} ({pct}%)")
            except Exception:
                # ignore edit failures
                pass
            # checkpoint after each item
            self._save_state(processed, results)

        # finished
        out_path = self.job_path / "alive.txt"
        out_path.write_text('\n'.join(results), encoding='utf-8')
        self.bot.send_document(self.chat_id, document=InputFile(str(out_path)), filename=f"alive_{self.job_id}.txt")
        self.bot.send_message(self.chat_id, f"Job {self.job_id} completed: {len(results)} alive links.")

    def _check_url(self, url: str) -> bool:
        req_url = normalize_for_request(url)
        try:
            # try as-is (https), then http fallback
            r = requests.get(req_url, timeout=15, allow_redirects=True, verify=self.verify_tls)
            if r.status_code < 400:
                return True
        except Exception:
            # try http if we tried https
            if req_url.startswith('https://'):
                try:
                    r = requests.get('http://' + req_url[len('https://'):], timeout=15, allow_redirects=True,
                                     verify=self.verify_tls)
                    if r.status_code < 400:
                        return True
                except Exception:
                    return False
            return False
        return False

    def _save_state(self, processed, results):
        self.state_file.write_text(json.dumps({'processed': processed, 'results': results}), encoding='utf-8')

    def _send_partial_results(self, results):
        if not results:
            return
        out_path = self.job_path / "alive_partial.txt"
        out_path.write_text('\n'.join(results), encoding='utf-8')
        self.bot.send_document(self.chat_id, document=InputFile(str(out_path)), filename=f"alive_partial_{self.job_id}.txt")


def start(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        update.message.reply_text("You are not allowed to use this bot.")
        return
    update.message.reply_text("Send a text file with lines to check. Use /status, /stop <jobid>, /resume <jobid>.")


def handle_document(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        update.message.reply_text("You are not allowed to use this bot.")
        return

    doc = update.message.document
    file_name = doc.file_name or 'input.txt'
    chat_id = update.effective_chat.id
    # Only accept text-like files
    file_path = JOB_DIR / f"upload_{uuid.uuid4().hex}_{file_name}"
    f = context.bot.get_file(doc.file_id)
    f.download(str(file_path))

    # start job
    verify_tls = True
    # if user provided /insecure in caption, allow ignoring certs
    caption = (doc.file_name or '')
    if update.message.caption and 'insecure' in update.message.caption.lower():
        verify_tls = False

    job = LinkCheckerJob(context.bot, chat_id, file_path, verify_tls=verify_tls)
    job.start()
    update.message.reply_text(f"Started job {job.job_id} for file {file_name}.")


def stop_cmd(update: Update, context: CallbackContext):
    # create a stop file for a job
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        update.message.reply_text("You are not allowed to use this bot.")
        return
    if not context.args:
        update.message.reply_text("Usage: /stop <jobid>")
        return
    jobid = context.args[0]
    jpath = JOB_DIR / jobid
    if not jpath.exists():
        update.message.reply_text("Unknown job id")
        return
    (jpath / 'STOP').write_text('1')
    update.message.reply_text(f"Stop requested for job {jobid}.")


def status_cmd(update: Update, context: CallbackContext):
    uid = update.effective_user.id
    if uid not in ALLOWED_USERS:
        update.message.reply_text("You are not allowed to use this bot.")
        return
    if not context.args:
        update.message.reply_text("Usage: /status <jobid>")
        return
    jobid = context.args[0]
    jpath = JOB_DIR / jobid
    state_file = jpath / 'state.json'
    if not state_file.exists():
        update.message.reply_text("No state for that job id.")
        return
    st = json.loads(state_file.read_text())
    update.message.reply_text(f"Job {jobid}: processed {st.get('processed',0)}, results {len(st.get('results',[]))}")


def main():
    token = os.environ.get('TELEGRAM_BOT_TOKEN')
    if not token:
        print('Set TELEGRAM_BOT_TOKEN environment variable.')
        return
    updater = Updater(token=token, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler('start', start))
    dp.add_handler(CommandHandler('stop', stop_cmd))
    dp.add_handler(CommandHandler('status', status_cmd))
    dp.add_handler(MessageHandler(Filters.document, handle_document))
    updater.start_polling()
    print('Bot started')
    updater.idle()


if __name__ == '__main__':
    main()
