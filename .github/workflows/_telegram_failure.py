"""
Telegram-Failure-Notifier für GitHub Actions.

Wird aus allen Workflow-Jobs aufgerufen wenn der Job fehlschlägt (`if: failure()`).
Erwartet folgende Env-Vars:
  - TELEGRAM_BOT_TOKEN
  - TELEGRAM_CHAT_ID
  - GH_RUN_URL     (github.server_url/github.repository/actions/runs/github.run_id)
  - JOB_NAME       (Name des Jobs, z.B. 'full-run', 'brief-only', 'cleanup-duplikate')

Zentral, damit Failure-Handling konsistent ist und nicht 6x per Copy-Paste existiert.
"""
import json
import os
import urllib.request


def main() -> None:
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    run_url = os.environ.get("GH_RUN_URL", "")
    job_name = os.environ.get("JOB_NAME", "(unknown)")

    if not token or not chat_id:
        print("Telegram-Alert übersprungen: TELEGRAM_BOT_TOKEN/CHAT_ID nicht gesetzt")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    msg = f"❌ Edikte-Monitor Fehler\nJob: {job_name}\n{run_url}"
    data = json.dumps({"chat_id": chat_id, "text": msg}).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"Telegram-Alert gesendet (Job: {job_name})")
    except Exception as exc:
        print(f"Telegram-Alert fehlgeschlagen: {exc}")


if __name__ == "__main__":
    main()
