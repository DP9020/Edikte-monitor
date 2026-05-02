"""
SendGrid Healthcheck — diagnostiziert Auth-Probleme ohne eine echte Mail zu senden.

Liest SENDGRID_API_KEY und SMTP_USER aus der Umgebung und prueft:
  1. /v3/scopes           — ist der API-Key gueltig? Welche Scopes hat er?
  2. /v3/user/account     — Account-Typ und -Status
  3. /v3/verified_senders — ist SMTP_USER als Single-Sender verifiziert?
  4. /v3/whitelabel/domains — Domain-Authentifizierung (falls genutzt)

Exit-Code 0 = alles gruen, 1 = Problem gefunden.
"""
from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request


def _get(path: str, api_key: str, timeout: int = 15) -> tuple[int, str]:
    req = urllib.request.Request(
        f"https://api.sendgrid.com{path}",
        headers={"Authorization": f"Bearer {api_key}", "Accept": "application/json"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.getcode(), r.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return exc.code, body
    except Exception as exc:
        return -1, f"{type(exc).__name__}: {exc}"


def main() -> int:
    api_key = os.environ.get("SENDGRID_API_KEY", "").strip()
    sender  = os.environ.get("SMTP_USER", "").strip()

    print("=" * 60)
    print("SendGrid Healthcheck")
    print("=" * 60)

    if not api_key:
        print("[FAIL] SENDGRID_API_KEY ist leer / nicht gesetzt")
        return 1
    if not sender:
        print("[WARN] SMTP_USER ist leer / nicht gesetzt — Sender-Check uebersprungen")

    masked = f"{api_key[:6]}...{api_key[-4:]}" if len(api_key) > 12 else "***"
    print(f"API-Key (maskiert): {masked}  (Laenge: {len(api_key)})")
    print(f"Absender (SMTP_USER): {sender or '<leer>'}")
    print()

    # 1) Scopes  ───────────────────────────────────────────────────────────
    print("[1/4] GET /v3/scopes")
    status, body = _get("/v3/scopes", api_key)
    print(f"      HTTP {status}")
    if status == 401:
        print("      => API-KEY UNGUELTIG ODER WIDERRUFEN")
        print(f"      Body: {body[:400]}")
        return 1
    if status == 403:
        print("      => API-KEY hat keine Scopes / Account gesperrt")
        print(f"      Body: {body[:400]}")
        return 1
    if status != 200:
        print(f"      => Unerwarteter Status, Body: {body[:400]}")
        return 1
    try:
        scopes = json.loads(body).get("scopes", [])
    except Exception:
        scopes = []
    has_mail_send = "mail.send" in scopes
    print(f"      Anzahl Scopes: {len(scopes)}")
    print(f"      mail.send vorhanden: {has_mail_send}")
    if not has_mail_send:
        print("      [FAIL] Key hat KEIN 'mail.send' Scope — neuer Key noetig mit Mail-Send-Berechtigung")
        return 1

    # 2) Account ───────────────────────────────────────────────────────────
    print("\n[2/4] GET /v3/user/account")
    status, body = _get("/v3/user/account", api_key)
    print(f"      HTTP {status}")
    if status == 200:
        try:
            acc = json.loads(body)
            print(f"      Type: {acc.get('type')}  Reputation: {acc.get('reputation')}")
        except Exception:
            print(f"      Body: {body[:300]}")
    else:
        print(f"      Body: {body[:300]}")

    # 3) Verified senders ──────────────────────────────────────────────────
    print("\n[3/4] GET /v3/verified_senders")
    status, body = _get("/v3/verified_senders", api_key)
    print(f"      HTTP {status}")
    sender_verified = False
    sender_listed = False
    if status == 200:
        try:
            results = json.loads(body).get("results", [])
            print(f"      {len(results)} Single-Sender-Eintrag(e):")
            for s in results:
                from_email = s.get("from_email", "")
                verified   = s.get("verified", False)
                marker = "OK" if verified else "PENDING"
                print(f"        [{marker}] {from_email}")
                if sender and from_email.lower() == sender.lower():
                    sender_listed = True
                    sender_verified = bool(verified)
        except Exception as exc:
            print(f"      Parse-Fehler: {exc}")
            print(f"      Body: {body[:400]}")
    else:
        print(f"      Body: {body[:400]}")

    # 4) Domain Authentication ─────────────────────────────────────────────
    print("\n[4/4] GET /v3/whitelabel/domains")
    status, body = _get("/v3/whitelabel/domains", api_key)
    print(f"      HTTP {status}")
    domain_match = False
    if status == 200:
        try:
            domains = json.loads(body)
            print(f"      {len(domains)} authentifizierte Domain(s):")
            for d in domains:
                domain = d.get("domain", "")
                valid  = d.get("valid", False)
                marker = "OK" if valid else "INVALID"
                print(f"        [{marker}] {domain}")
                if sender and "@" in sender:
                    sender_domain = sender.split("@", 1)[1].lower()
                    if domain.lower() == sender_domain and valid:
                        domain_match = True
        except Exception as exc:
            print(f"      Parse-Fehler: {exc}")
    else:
        print(f"      Body: {body[:300]}")

    # Verdict ───────────────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("ERGEBNIS")
    print("=" * 60)
    if sender:
        if sender_verified or domain_match:
            via = "Domain-Auth" if domain_match else "Single-Sender"
            print(f"[OK] Absender '{sender}' ist via {via} authentifiziert.")
            print("[OK] API-Key ist gueltig und hat mail.send Scope.")
            print("=> SendGrid-seitig sollte Mail-Versand funktionieren.")
            return 0
        if sender_listed:
            print(f"[FAIL] Absender '{sender}' ist als Single-Sender eingetragen, aber NICHT verifiziert.")
            print("       => In SendGrid auf den Verify-Link klicken (Mail an Absender).")
            return 1
        print(f"[FAIL] Absender '{sender}' ist WEDER als Single-Sender eingetragen NOCH per Domain authentifiziert.")
        print("       => SendGrid erlaubt keinen Versand von dieser Adresse.")
        print("       => Sender Authentication > Single Sender Verification > Add Sender")
        return 1
    print("[?] Kein Sender konfiguriert, Diagnose unvollstaendig.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
