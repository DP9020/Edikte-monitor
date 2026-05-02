"""
Brevo SMTP Healthcheck — testet AUTH gegen smtp-relay.brevo.com:587 ohne
eine echte Mail zu senden.

Liest aus der Umgebung:
  BREVO_SMTP_KEY     SMTP-Key (beginnt mit xsmtpsib-)
  SMTP_USER          Absender-Adresse (auch Default fuer SMTP-Login)
  BREVO_SMTP_LOGIN   optional; Brevo-Account-Login wenn != SMTP_USER

Nutzt EHLO + STARTTLS + LOGIN; wenn LOGIN klappt, ist der Pfad gruen.
Es wird KEIN MAIL FROM/RCPT TO gesendet, also kein Credit verbraucht.

Exit-Code 0 = OK, 1 = Problem.
"""
from __future__ import annotations

import os
import smtplib
import sys


def main() -> int:
    smtp_key   = os.environ.get("BREVO_SMTP_KEY", "").strip()
    sender     = os.environ.get("SMTP_USER", "").strip()
    smtp_login = os.environ.get("BREVO_SMTP_LOGIN", "").strip() or sender

    print("=" * 60)
    print("Brevo SMTP Healthcheck")
    print("=" * 60)

    if not smtp_key:
        print("[FAIL] BREVO_SMTP_KEY ist leer / nicht gesetzt")
        return 1
    if not sender:
        print("[FAIL] SMTP_USER ist leer / nicht gesetzt")
        return 1

    masked = f"{smtp_key[:10]}...{smtp_key[-4:]}" if len(smtp_key) > 16 else "***"
    print(f"SMTP-Key (maskiert): {masked}  (Laenge: {len(smtp_key)})")
    print(f"Absender (SMTP_USER): {sender}")
    print(f"SMTP-Login:           {smtp_login}")
    print(f"Host:                 smtp-relay.brevo.com:587")
    print()

    print("[1/2] Verbinde + STARTTLS + EHLO ...")
    try:
        with smtplib.SMTP("smtp-relay.brevo.com", 587, timeout=20) as srv:
            code, banner = srv.ehlo()
            print(f"      EHLO HTTP-aequivalent: {code}")
            srv.starttls()
            srv.ehlo()
            print("      STARTTLS OK")

            print("\n[2/2] AUTH LOGIN ...")
            try:
                srv.login(smtp_login, smtp_key)
                print("      [OK] Login erfolgreich.")
            except smtplib.SMTPAuthenticationError as exc:
                err = exc.smtp_error.decode("utf-8", errors="replace") if isinstance(exc.smtp_error, bytes) else str(exc.smtp_error)
                print(f"      [FAIL] AUTH abgelehnt — Code {exc.smtp_code}")
                print(f"             {err}")
                print()
                print("=" * 60)
                print("ERGEBNIS")
                print("=" * 60)
                print("[FAIL] Brevo-Login fehlgeschlagen.")
                print("       Mögliche Ursachen:")
                print("       - SMTP-Key falsch oder widerrufen (im Brevo-Dashboard pruefen)")
                print("       - SMTP-Login falsch (sollte = Brevo-Account-Email sein)")
                print(f"       - Aktuell verwendet als Login: {smtp_login}")
                print("       - Wenn Login ungleich SMTP_USER: BREVO_SMTP_LOGIN setzen")
                return 1
    except Exception as exc:
        print(f"      [FAIL] Verbindungsfehler: {type(exc).__name__}: {exc}")
        return 1

    print()
    print("=" * 60)
    print("ERGEBNIS")
    print("=" * 60)
    print("[OK] Brevo SMTP-Pfad ist autorisiert und funktionsfaehig.")
    print("=> Mail-Versand sollte ueber Brevo klappen.")
    print()
    print("HINWEIS: Sender-Verifikation (dass " + sender + " bei Brevo")
    print("authentifiziert ist) wird vom Server beim ersten Send geprueft,")
    print("nicht beim Login. Falls beim ersten echten Send eine 550-Antwort")
    print("kommt: in Brevo > Senders, Domains & Dedicated IPs > Senders pruefen.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
