"""
Schickt eine Test-Mail an SMTP_USER (sich selbst), um die komplette
Brevo-SMTP-Pipeline zu validieren — inklusive DOCX-Anhang und
Multipart-Encoding.

Nutzt dieselbe Helper-Funktion wie main.py (Codepfad-Identitaet).
Verbraucht 1 Brevo-Credit.
"""
from __future__ import annotations

import base64
import os
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

# main.py hat keine top-level side effects, die einen Notion-Login erfordern;
# Helper-Imports sind sicher.
from main import _send_via_smtp  # type: ignore


def main() -> int:
    smtp_key   = os.environ.get("BREVO_SMTP_KEY", "").strip()
    smtp_login = os.environ.get("BREVO_SMTP_LOGIN", "").strip()
    sender     = os.environ.get("SMTP_USER", "").strip()
    if not (smtp_key and smtp_login and sender):
        print("FAIL: BREVO_SMTP_KEY / BREVO_SMTP_LOGIN / SMTP_USER fehlt")
        return 1

    print("Sende Test-Mail an", sender, "...")
    # Echter, leerer DOCX-Container – Antiviren-Scanner und Office-Clients
    # akzeptieren das Attachment, ohne es als kaputt zu markieren. Frühere
    # Stub-Bytes ("PK\\x03\\x04mail-pipeline-test-payload") wurden von
    # Brevo-Filter teilweise zurückgewiesen → Test-grün war trügerisch.
    import io
    from docx import Document  # type: ignore

    _buf = io.BytesIO()
    Document().save(_buf)
    real_docx = _buf.getvalue()
    attachments_b64 = [(base64.b64encode(real_docx).decode("utf-8"), "Mail-Test.docx")]

    ok, reason = _send_via_smtp(
        host="smtp-relay.brevo.com", port=587,
        username=smtp_login, password=smtp_key,
        absender=sender, to_email=sender, to_name="Edikte-Monitor Test",
        subject="Edikte-Monitor: SMTP-Pipeline Test",
        body_text="Wenn diese Mail ankommt, funktioniert die Brevo-SMTP-Pipeline.\nDer DOCX-Anhang ist ein Pseudo-File und kann ignoriert werden.",
        attachments_b64=attachments_b64,
    )
    if ok:
        print("OK: Mail wurde an Brevo uebergeben.")
        return 0
    print(f"FAIL: {reason}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
