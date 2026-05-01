"""
models.py — Provider-Konfigurationen + unified Call-Interface für die Eval.

5 Konfigurationen total:
- status_quo_text:    OpenAI gpt-4o-mini  (Call A in Production)
- status_quo_vision:  OpenAI gpt-4o       (Call B in Production)
- nim_qwen:           NIM   qwen/qwen3-coder-480b-a35b-instruct
- nim_glm5:           NIM   z-ai/glm5
- nim_deepseek_v32:   NIM   deepseek-ai/deepseek-v3.2
- nim_deepseek_v4pro: NIM   deepseek-ai/deepseek-v4-pro

Keine Vision-Variante bei NIM (Phase 0 / Phase 2 Befund).
Jede Config hat dieselbe API-Form: call(messages, max_tokens, temperature).
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

# Production-Prompts EXAKT aus main.py:1143–1162 und main.py:2971–2990.
# Müssen wortgleich bleiben, damit der Vergleich mit Status Quo fair ist.

PROMPT_TEXT = """Du analysierst Texte aus österreichischen Gerichts-Gutachten für Zwangsversteigerungen.

Extrahiere genau diese Felder und antworte NUR mit validem JSON, ohne Erklärungen:

{
  "eigentümer_name": "Vollständiger Name der verpflichteten Partei (Immobilieneigentümer). Nur der Name, keine Adresse, kein Geburtsdatum. Mehrere Eigentümer mit ' | ' trennen.",
  "eigentümer_adresse": "Straße und Hausnummer der verpflichteten Partei (Wohnadresse für Briefversand, NICHT die Liegenschaftsadresse)",
  "eigentümer_plz_ort": "PLZ und Ort der verpflichteten Partei, z.B. '1010 Wien' oder 'D-88250 Weingarten'",
  "gläubiger": ["Liste der betreibenden Banken/Gläubiger. Nur echte Kreditgeber (Banken, Sparkassen, etc.). KEINE Anwälte, Gerichte, Sachverständige, Hausverwaltungen (WEG/EG/EGT), Aktenzeichen."],
  "forderung_betrag": "Forderungshöhe falls angegeben, z.B. 'EUR 150.000'"
}

Wichtige Regeln:
- 'Verpflichtete Partei' = Eigentümer/Schuldner → das ist eigentümer_name
- 'Betreibende Partei' = Gläubiger/Bank → das ist gläubiger
- Anwälte (RA, Rechtsanwalt, vertreten durch) sind KEINE Gläubiger
- Sachverständige, Hilfskräfte, Mitarbeiter des SV sind KEIN Eigentümer
- WEG, EG, EGT, EigG, Eigentümergemeinschaft sind KEINE Gläubiger
- Wenn ein Feld nicht gefunden wird: null
- Geburtsdaten NICHT im Namen mitgeben"""

PROMPT_VISION = """Du analysierst Bilder aus österreichischen Gerichts-Gutachten für Zwangsversteigerungen.
Es gibt zwei Dokumenttypen – analysiere BEIDE:

1. Professionelles Gutachten (Wien-Stil): Enthält Abschnitte 'Verpflichtete Partei' (= Eigentümer) und 'Betreibende Partei' (= Gläubiger).
2. Grundbuchauszug (Kärnten-Stil): Enthält Abschnitte '** B **' oder 'B-Blatt' (= Eigentümer mit Anteilen) und '** C **' oder 'C-Blatt' (= Pfandrechte/Gläubiger). Der Eigentümer steht nach 'Eigentumsrecht' oder 'Anteil' in Sektion B.

Extrahiere genau diese Felder und antworte NUR mit validem JSON, ohne Erklärungen:

{
  "eigentümer_name": "Vollständiger Name des Immobilieneigentümers. Nur der Name, keine Adresse, kein Geburtsdatum. Mehrere Eigentümer mit ' | ' trennen.",
  "eigentümer_adresse": "Straße und Hausnummer des Eigentümers (Wohnadresse für Briefversand, NICHT die Liegenschaftsadresse)",
  "eigentümer_plz_ort": "PLZ und Ort des Eigentümers, z.B. '1010 Wien'",
  "gläubiger": ["Liste der betreibenden Banken/Gläubiger. Nur echte Kreditgeber (Banken, Sparkassen, Raiffeisen etc.). KEINE Anwälte, Gerichte, WEG/EG/Hausverwaltungen."],
  "forderung_betrag": "Forderungshöhe falls angegeben, z.B. 'EUR 150.000'"
}

Wichtige Regeln:
- Sachverständige, Hilfskräfte des SV, Anwälte sind KEINE Eigentümer
- WEG, EG, EGT, Eigentümergemeinschaft sind KEINE Gläubiger
- Wenn ein Feld nicht gefunden wird: null"""


@dataclass
class ProviderConfig:
    id: str                 # interne Kennung (für Datei-Pfade)
    label: str              # menschenlesbar
    base_url: str
    api_key_env: str        # Name der ENV-Variable
    model: str
    supports_vision: bool = False
    # Cold-Start-Faktor: erster Call kann lange dauern (NIM Free Tier 60–90s+)
    timeout_warmup: int = 180
    timeout_call: int = 120


def _key(config: ProviderConfig) -> str:
    val = os.environ.get(config.api_key_env, "")
    if not val:
        raise RuntimeError(f"Env {config.api_key_env} nicht gesetzt.")
    return val


def _make_client(config: ProviderConfig):
    from openai import OpenAI
    return OpenAI(
        api_key=_key(config),
        base_url=config.base_url,
        timeout=config.timeout_call,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass
class CallResult:
    raw_text: str | None      # Roh-Antwort (Content-String)
    latency_ms: int           # Wallclock-ms (ohne Cold-Start)
    prompt_tokens: int | None
    completion_tokens: int | None
    error: str | None         # None = ok


def call_text(
    config: ProviderConfig,
    pdf_text_snippet: str,
    *,
    temperature: float = 0.0,
    max_tokens: int = 400,
) -> CallResult:
    """Einheitlicher Text-Extraction-Call (Call A)."""
    try:
        client = _make_client(config)
        t0 = time.perf_counter()
        kwargs: dict[str, Any] = {
            "model": config.model,
            "messages": [
                {"role": "system", "content": PROMPT_TEXT},
                {"role": "user",   "content": pdf_text_snippet},
            ],
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        # JSON-Mode nur dort wo wir wissen, dass es geht (OpenAI-Modelle).
        # NIM unterstützt response_format teilweise — testen wir erst ohne;
        # bei flakey Output schalten wir später ein.
        if config.id.startswith("status_quo"):
            kwargs["response_format"] = {"type": "json_object"}

        resp = client.chat.completions.create(**kwargs)
        dt   = int((time.perf_counter() - t0) * 1000)
        msg  = resp.choices[0].message
        text = (msg.content or "").strip()
        usage = getattr(resp, "usage", None)
        return CallResult(
            raw_text=text,
            latency_ms=dt,
            prompt_tokens=getattr(usage, "prompt_tokens", None),
            completion_tokens=getattr(usage, "completion_tokens", None),
            error=None,
        )
    except Exception as exc:
        return CallResult(
            raw_text=None,
            latency_ms=0,
            prompt_tokens=None,
            completion_tokens=None,
            error=f"{type(exc).__name__}: {exc}",
        )


def call_vision(
    config: ProviderConfig,
    images_b64: list[str],
    *,
    temperature: float = 0.0,
    max_tokens: int = 500,
) -> CallResult:
    """Einheitlicher Vision-Call (Call B). Nur für Status-Quo-Vision."""
    if not config.supports_vision:
        return CallResult(None, 0, None, None, "no_vision_support")

    try:
        client = _make_client(config)
        content: list[dict] = [{"type": "text", "text": "Analysiere dieses Gutachten:"}]
        for img_b64 in images_b64:
            content.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{img_b64}",
                    "detail": "high",
                },
            })

        t0 = time.perf_counter()
        resp = client.chat.completions.create(
            model=config.model,
            messages=[
                {"role": "system", "content": PROMPT_VISION},
                {"role": "user",   "content": content},
            ],
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )
        dt = int((time.perf_counter() - t0) * 1000)
        msg = resp.choices[0].message
        text = (msg.content or "").strip()
        usage = getattr(resp, "usage", None)
        return CallResult(
            raw_text=text,
            latency_ms=dt,
            prompt_tokens=getattr(usage, "prompt_tokens", None),
            completion_tokens=getattr(usage, "completion_tokens", None),
            error=None,
        )
    except Exception as exc:
        return CallResult(None, 0, None, None, f"{type(exc).__name__}: {exc}")


_WARMUP_DUMMY = """Im Folgenden steht ein synthetischer Edikt-Auszug zur Modell-Aufwärmung.
Sachverständigengutachten zur Liegenschaft EZ 9999 Grundbuch 12345
Verpflichtete Partei: Max Mustermann, geb. 01.01.1970, Beispielstraße 10, 1010 Wien
Betreibende Partei: Beispielbank AG, vertreten durch Dr. Test, Rechtsanwalt
Hereinbringung von EUR 100.000 samt Anhang
Versteigerungstermin: 01.06.2026
PFANDRECHT Höchstbetrag EUR 200.000
""" * 30   # ~3000–4000 Tokens, ähnlich realer Edikte


def liveness_check(config: ProviderConfig, *, timeout_s: int = 8) -> CallResult:
    """Schneller PONG-Check (max ~timeout_s). Erkennt komplett tote Modelle.
    Anders als Warmup: kurzer Prompt + harter Timeout, schlägt schnell fehl.
    """
    try:
        from openai import OpenAI
        client = OpenAI(api_key=_key(config), base_url=config.base_url, timeout=timeout_s)
        t0 = time.perf_counter()
        resp = client.chat.completions.create(
            model=config.model,
            messages=[{"role": "user", "content": "PONG"}],
            max_tokens=3,
            temperature=0,
        )
        dt = int((time.perf_counter() - t0) * 1000)
        return CallResult(
            raw_text=(resp.choices[0].message.content or "").strip()[:30],
            latency_ms=dt,
            prompt_tokens=None, completion_tokens=None,
            error=None,
        )
    except Exception as exc:
        return CallResult(None, 0, None, None, f"liveness_{type(exc).__name__}: {exc}"[:100])


def warmup(config: ProviderConfig) -> CallResult:
    """Schickt einen real-ähnlichen ~4k-Token-Prompt und verwirft die Antwort.
    Wichtig: ein 5-Token-PONG warmt das Modell NICHT für unsere echten Edikt-
    Prompts vor (im Smoke-Run gemessen: GLM5/DeepSeek brauchen >50s beim
    ersten echten Call trotz PONG-Warmup).
    """
    try:
        client = _make_client(config)
        client = client.with_options(timeout=config.timeout_warmup)
        t0 = time.perf_counter()
        resp = client.chat.completions.create(
            model=config.model,
            messages=[
                {"role": "system", "content": PROMPT_TEXT},
                {"role": "user",   "content": _WARMUP_DUMMY},
            ],
            max_tokens=200,
            temperature=0,
        )
        dt = int((time.perf_counter() - t0) * 1000)
        return CallResult(
            raw_text=(resp.choices[0].message.content or "").strip()[:60],
            latency_ms=dt,
            prompt_tokens=None, completion_tokens=None,
            error=None,
        )
    except Exception as exc:
        return CallResult(None, 0, None, None, f"warmup_{type(exc).__name__}: {exc}")


# ---------------------------------------------------------------------------
# Vordefinierte Configs
# ---------------------------------------------------------------------------

_OPENAI_BASE = "https://api.openai.com/v1"
_NIM_BASE    = os.environ.get("NVIDIA_BASE_URL", "https://integrate.api.nvidia.com/v1")

CONFIGS: dict[str, ProviderConfig] = {
    "status_quo_text": ProviderConfig(
        id="status_quo_text",
        label="OpenAI gpt-4o-mini (Status Quo Call A)",
        base_url=_OPENAI_BASE,
        api_key_env="OPENAI_API_KEY",
        model="gpt-4o-mini",
        supports_vision=False,
    ),
    "status_quo_vision": ProviderConfig(
        id="status_quo_vision",
        label="OpenAI gpt-4o Vision (Status Quo Call B)",
        base_url=_OPENAI_BASE,
        api_key_env="OPENAI_API_KEY",
        model="gpt-4o",
        supports_vision=True,
    ),
    "nim_qwen": ProviderConfig(
        id="nim_qwen",
        label="NIM Qwen 3 Coder 480B",
        base_url=_NIM_BASE,
        api_key_env="NVIDIA_API_KEY",
        model="qwen/qwen3-coder-480b-a35b-instruct",
    ),
    "nim_glm5": ProviderConfig(
        id="nim_glm5",
        label="NIM GLM-5 (z-ai/glm5)",
        base_url=_NIM_BASE,
        api_key_env="NVIDIA_API_KEY",
        model="z-ai/glm5",
    ),
    "nim_deepseek_v32": ProviderConfig(
        id="nim_deepseek_v32",
        label="NIM DeepSeek V3.2",
        base_url=_NIM_BASE,
        api_key_env="NVIDIA_API_KEY",
        model="deepseek-ai/deepseek-v3.2",
    ),
    "nim_deepseek_v4pro": ProviderConfig(
        id="nim_deepseek_v4pro",
        label="NIM DeepSeek V4-Pro",
        base_url=_NIM_BASE,
        api_key_env="NVIDIA_API_KEY",
        model="deepseek-ai/deepseek-v4-pro",
    ),
}


# Welche Configs für welche Modality
TEXT_CONFIGS   = [c for c in CONFIGS.values() if c.id != "status_quo_vision"]
VISION_CONFIGS = [CONFIGS["status_quo_vision"]]    # NIM hat (noch) kein Vision
