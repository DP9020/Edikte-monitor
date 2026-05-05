"""
Workflow-level Telegram-Failure-Notifier für GitHub Actions.

Wird vom `notify-failure`-Job am Ende des Workflows aufgerufen. Anders als
`_telegram_failure.py` (per-Job, läuft nur wenn der Job-Step selbst abgeschlossen
ist) deckt dieses Skript auch Setup-Cancellations und Job-Timeouts ab, bei denen
keine Workflow-Steps ausgeführt werden.

Erwartet folgende Env-Vars:
  - TELEGRAM_BOT_TOKEN
  - TELEGRAM_CHAT_ID
  - GH_RUN_URL
  - FULL_RUN_RESULT, BRIEF_ONLY_RESULT, GDRIVE_SYNC_RESULT,
    CLEANUP_RESULT, WOCHENBERICHT_RESULT (jeweils success/failure/cancelled/skipped)

Filtert brief-only-Cancellations heraus (das ist erwartet: ein neuer brief-only-Tick
darf seinen Vorgänger ersetzen, das ist kein Fehler).
"""
import json
import os
import urllib.request


JOB_LABELS = {
    "FULL_RUN_RESULT": "full-run",
    "BRIEF_ONLY_RESULT": "brief-only",
    "GDRIVE_SYNC_RESULT": "gdrive-sync",
    "CLEANUP_RESULT": "cleanup-duplikate",
    "WOCHENBERICHT_RESULT": "wochenbericht",
}

# Wird ein Job in der Setup-Phase gecancelt, kommt 'cancelled' rein. Für
# brief-only ist das normal (Concurrency-Cancel durch Nachfolger). Für andere
# Jobs ist es ein echter Alarm.
NOISY_CANCELS = {"brief-only"}


def main() -> None:
    failed_jobs: list[str] = []
    for env_key, label in JOB_LABELS.items():
        result = os.environ.get(env_key, "skipped")
        if result == "failure":
            failed_jobs.append(f"{label}: failure")
        elif result == "cancelled" and label not in NOISY_CANCELS:
            failed_jobs.append(f"{label}: cancelled")

    if not failed_jobs:
        print("Keine relevanten Failures – kein Alert gesendet.")
        return

    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", "")
    run_url = os.environ.get("GH_RUN_URL", "")

    if not token or not chat_id:
        print(f"Telegram-Alert übersprungen ({failed_jobs}): TELEGRAM_BOT_TOKEN/CHAT_ID nicht gesetzt")
        return

    lines = ["Edikte-Monitor Workflow-Fehler"]
    lines.extend(f"- {entry}" for entry in failed_jobs)
    lines.append(run_url)
    msg = "\n".join(lines)

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = json.dumps({"chat_id": chat_id, "text": msg}).encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(req, timeout=10)
        print(f"Telegram-Alert gesendet: {failed_jobs}")
    except Exception as exc:
        print(f"Telegram-Alert fehlgeschlagen: {exc}")


if __name__ == "__main__":
    main()
