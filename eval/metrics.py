"""
metrics.py — Deterministische Metriken D1-D11 aus eval/01-EVAL-CRITERIA.md.

Jede Metrik konsumiert (output_dict, ground_truth_dict) und liefert ein
Ergebnis-Dict. Aggregation erfolgt im runner.

Exporte:
- evaluate(output_text, ground_truth) -> dict mit allen Metrik-Werten
- aggregate(results: list[dict]) -> Modell-Summary (Mittelwerte / Anteile)
"""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Any


# ---------------------------------------------------------------------------
# Normalisierung
# ---------------------------------------------------------------------------

def _norm(s: str | None) -> str:
    if s is None:
        return ""
    s = str(s)
    # Unicode-NFKC + lowercase
    s = unicodedata.normalize("NFKC", s).lower().strip()
    # Whitespace collapse
    s = re.sub(r"\s+", " ", s)
    # Geburtsdatum-Stripping ("geb. 01.01.1980", "* 1.1.80", "(geb. ..)")
    s = re.sub(r"\(?geb\.?\s*\d{1,2}\.\d{1,2}\.\d{2,4}\)?", "", s).strip()
    s = re.sub(r"\(?\*\s*\d{1,2}\.\d{1,2}\.\d{2,4}\)?", "", s).strip()
    # Trailing-Punkte / Kommas
    s = s.strip(" .,;:")
    return s


def _norm_name(s: str | None) -> str:
    """Name-spezifische Normalisierung: tokens sortiert (für Reihenfolge-Toleranz)."""
    n = _norm(s)
    if not n:
        return ""
    # Komma-Separation („Mustermann, Hans") → Whitespace
    n = n.replace(",", " ")
    n = re.sub(r"\s+", " ", n).strip()
    # Tokens alphabetisch sortieren — toleriert "Hans Müller" vs "Müller Hans"
    tokens = sorted(t for t in n.split(" ") if t)
    return " ".join(tokens)


def _norm_address(s: str | None) -> str:
    n = _norm(s)
    # "Hauptstr." vs "Hauptstraße" — strasse-Suffix-Normalisierung
    n = re.sub(r"\bstr\.?\b", "straße", n)
    # Hausnummer-Whitespace: "5 a" → "5a"
    n = re.sub(r"(\d+)\s*([a-z])\b", r"\1\2", n)
    return n


def _norm_plz(s: str | None) -> str:
    n = _norm(s)
    n = re.sub(r"^([a-z]{1,3})-(\d)", r"\1-\2", n)   # "D- 12345" -> "d-12345"
    return n


def _norm_glaeubiger(items: Any) -> set[str]:
    if not items:
        return set()
    if isinstance(items, str):
        # Falls aus Notion mit ' | ' kommt
        items = [x.strip() for x in items.split("|")]
    out = set()
    for it in items:
        if not it:
            continue
        n = _norm(str(it))
        # Erste Bank, Erste-Bank, Erste Bank AG → kanonischer Stamm
        n = re.sub(r"\bag\b|\.|,|gmbh", "", n).strip()
        n = re.sub(r"\s+", " ", n)
        if n:
            out.add(n)
    return out


def _extract_amount(s: str | None) -> float | None:
    if not s:
        return None
    s = str(s)
    m = re.search(r"([\d\.\,]+)", s)
    if not m:
        return None
    raw = m.group(1)
    # Deutsche Notation: "150.000,50" → 150000.50
    if raw.count(",") == 1 and raw.count(".") >= 1:
        raw = raw.replace(".", "").replace(",", ".")
    elif raw.count(",") == 1:
        raw = raw.replace(",", ".")
    else:
        raw = raw.replace(".", "")
    try:
        return float(raw)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Levenshtein (kleine eigene Implementierung — keine externe Dep nötig)
# ---------------------------------------------------------------------------

def levenshtein(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i] + [0] * len(b)
        for j, cb in enumerate(b, 1):
            cost = 0 if ca == cb else 1
            curr[j] = min(prev[j] + 1, curr[j-1] + 1, prev[j-1] + cost)
        prev = curr
    return prev[-1]


# ---------------------------------------------------------------------------
# Pro-Item-Bewertung
# ---------------------------------------------------------------------------

def parse_output(raw_text: str | None) -> tuple[dict | None, str | None]:
    """Versucht das Modell-Output als JSON zu parsen.
    Robust gegen Markdown-Fences und Pre-/Suffix-Geschwätz."""
    if raw_text is None:
        return None, "no_output"
    s = raw_text.strip()
    # Markdown-Fences entfernen
    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
    s = re.sub(r"\s*```$", "", s)
    s = s.strip()
    # Falls kein reines JSON: erstes {...} extrahieren
    if not s.startswith("{"):
        m = re.search(r"\{[\s\S]*\}", s)
        if not m:
            return None, "no_json_object"
        s = m.group(0)
    try:
        data = json.loads(s)
        if not isinstance(data, dict):
            return None, "json_not_object"
        return data, None
    except json.JSONDecodeError as e:
        return None, f"json_decode_{e.__class__.__name__}"


def evaluate(raw_text: str | None, ground_truth: dict) -> dict:
    """Bewertet ein einzelnes Modell-Output gegen Ground Truth.
    Liefert ein Dict mit allen D1-D11 Werten + Helpers für Aggregation.
    """
    parsed, parse_err = parse_output(raw_text)

    res: dict[str, Any] = {
        # D1
        "json_valid": parsed is not None,
        "json_error": parse_err,
        # Schemen-Felder
        "schema_compliant": False,
    }

    if parsed is None:
        # Alle weiteren Metriken nicht berechenbar
        for k in ("name_exact","name_recall","addr_exact","plz_exact",
                  "glaubiger_precision","glaubiger_recall","glaubiger_f1",
                  "forderung_exact",
                  "halluc_name","halluc_addr","fp_glaubiger"):
            res[k] = None
        return res

    # D2 Schema
    expected_keys = {"eigentümer_name","eigentümer_adresse","eigentümer_plz_ort",
                     "gläubiger","forderung_betrag"}
    has_keys = expected_keys.issubset(parsed.keys())
    glaeu = parsed.get("gläubiger")
    glaeu_is_list = (glaeu is None) or isinstance(glaeu, list)
    res["schema_compliant"] = has_keys and glaeu_is_list

    gt = ground_truth or {}

    # ── D3 Name-Exact (nach Sort-Normalisierung) ─────────────────────────────
    o_name = _norm_name(parsed.get("eigentümer_name"))
    g_name = _norm_name(gt.get("eigentümer_name"))
    res["name_exact"]  = (o_name == g_name) if g_name else None
    # ── D4 Name-Recall (Substring nach simpler Norm) ─────────────────────────
    o_simple = _norm(parsed.get("eigentümer_name"))
    g_simple = _norm(gt.get("eigentümer_name"))
    if g_simple:
        # Recall: jeder Token aus GT muss in Output vorkommen
        gt_tokens = [t for t in g_simple.replace(",", " ").split() if len(t) > 2]
        if gt_tokens:
            hits = sum(1 for t in gt_tokens if t in o_simple)
            res["name_recall"] = hits / len(gt_tokens)
        else:
            res["name_recall"] = None
    else:
        res["name_recall"] = None

    # ── D5 Adresse (Levenshtein <= 2 nach Norm) ─────────────────────────────
    o_addr = _norm_address(parsed.get("eigentümer_adresse"))
    g_addr = _norm_address(gt.get("eigentümer_adresse"))
    if g_addr:
        res["addr_exact"] = levenshtein(o_addr, g_addr) <= 2
    else:
        res["addr_exact"] = None

    # ── D6 PLZ/Ort exact ─────────────────────────────────────────────────────
    o_plz = _norm_plz(parsed.get("eigentümer_plz_ort"))
    g_plz = _norm_plz(gt.get("eigentümer_plz_ort"))
    if g_plz:
        res["plz_exact"] = (o_plz == g_plz)
    else:
        res["plz_exact"] = None

    # ── D7 Gläubiger F1 ─────────────────────────────────────────────────────
    o_set = _norm_glaeubiger(parsed.get("gläubiger"))
    g_set = _norm_glaeubiger(gt.get("gläubiger"))
    if g_set or o_set:
        tp = len(o_set & g_set)
        precision = tp / len(o_set) if o_set else 0.0
        recall    = tp / len(g_set) if g_set else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) else 0.0
        res["glaubiger_precision"] = precision
        res["glaubiger_recall"]    = recall
        res["glaubiger_f1"]        = f1
        # D11: Anzahl FP (im Output, nicht in GT)
        res["fp_glaubiger"] = len(o_set - g_set)
    else:
        res["glaubiger_precision"] = None
        res["glaubiger_recall"]    = None
        res["glaubiger_f1"]        = None
        res["fp_glaubiger"]        = 0

    # ── D8 Forderungsbetrag exakt ────────────────────────────────────────────
    o_amt = _extract_amount(parsed.get("forderung_betrag"))
    g_amt = _extract_amount(gt.get("forderung_betrag"))
    if g_amt is not None:
        res["forderung_exact"] = (o_amt is not None) and abs(o_amt - g_amt) < 0.5
    else:
        res["forderung_exact"] = None

    # ── D9 Halluzination-Name ────────────────────────────────────────────────
    # GT null -> Output non-null = halluziniert
    res["halluc_name"] = (not gt.get("eigentümer_name")) and bool(parsed.get("eigentümer_name"))

    # ── D10 Halluzination-Adresse ────────────────────────────────────────────
    res["halluc_addr"] = (not gt.get("eigentümer_adresse")) and bool(parsed.get("eigentümer_adresse"))

    return res


# ---------------------------------------------------------------------------
# Aggregation über alle Calls eines Modells
# ---------------------------------------------------------------------------

def aggregate(per_call: list[dict]) -> dict:
    """Aggregiert eine Liste von evaluate()-Outputs zu Modell-Stats."""
    n = len(per_call)
    if n == 0:
        return {"n": 0}

    def _ratio_of(key: str, predicate=lambda v: v is True) -> float:
        applicable = [r for r in per_call if r.get(key) is not None]
        if not applicable:
            return 0.0
        return sum(1 for r in applicable if predicate(r[key])) / len(applicable)

    def _mean(key: str) -> float | None:
        vals = [r[key] for r in per_call if isinstance(r.get(key), (int, float))]
        return (sum(vals) / len(vals)) if vals else None

    return {
        "n": n,
        "json_valid_rate":   _ratio_of("json_valid"),
        "schema_compliant":  _ratio_of("schema_compliant"),
        "name_exact":        _ratio_of("name_exact"),
        "name_recall_mean":  _mean("name_recall"),
        "addr_exact":        _ratio_of("addr_exact"),
        "plz_exact":         _ratio_of("plz_exact"),
        "glaubiger_f1_mean": _mean("glaubiger_f1"),
        "forderung_exact":   _ratio_of("forderung_exact"),
        "halluc_name_rate":  _ratio_of("halluc_name"),
        "halluc_addr_rate":  _ratio_of("halluc_addr"),
        "fp_glaubiger_mean": _mean("fp_glaubiger"),
    }


def composite_score(agg: dict) -> float | None:
    """Gewichteter Score laut 01-EVAL-CRITERIA.md §4."""
    keys_required = ["name_exact","addr_exact","plz_exact",
                     "glaubiger_f1_mean","forderung_exact",
                     "halluc_name_rate","halluc_addr_rate"]
    if not all(agg.get(k) is not None for k in keys_required):
        return None
    halluc_term = max(0.0, 1.0 - agg["halluc_name_rate"] - agg["halluc_addr_rate"])
    return (
          0.35 * agg["name_exact"]
        + 0.30 * agg["addr_exact"]
        + 0.10 * agg["plz_exact"]
        + 0.10 * (agg["glaubiger_f1_mean"] or 0.0)
        + 0.05 * agg["forderung_exact"]
        + 0.10 * halluc_term
    )


def knockouts(agg: dict) -> list[str]:
    """Liefert Liste von verletzten Knock-Out-Schwellen laut §3."""
    failed: list[str] = []
    if agg["json_valid_rate"] < 0.99:
        failed.append(f"D1_json<99% ({agg['json_valid_rate']:.1%})")
    if agg["name_exact"] < 0.85:
        failed.append(f"D3_name<85% ({agg['name_exact']:.1%})")
    if agg["addr_exact"] < 0.90:
        failed.append(f"D5_addr<90% ({agg['addr_exact']:.1%})")
    if agg["halluc_name_rate"] > 0.03:
        failed.append(f"D9_halluc_name>3% ({agg['halluc_name_rate']:.1%})")
    if agg["halluc_addr_rate"] > 0.03:
        failed.append(f"D10_halluc_addr>3% ({agg['halluc_addr_rate']:.1%})")
    return failed
