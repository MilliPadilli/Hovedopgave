# python/compact_core/core.py
from __future__ import annotations
from typing import Any, Dict, Optional
import os, json, time, re
from decimal import Decimal
from datetime import datetime
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# ========= ENV =========
MODEL_REGION = os.getenv("MODEL_REGION", "eu-west-1")
MODEL_ID = os.getenv("MODEL_ID")
TEMPERATURE = float(os.getenv("TEMPERATURE", "0"))
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "2000"))
CURRENCY_DEFAULT = os.getenv("CURRENCY_DEFAULT", "EUR")
STRICT_SCHEMA = os.getenv("STRICT_SCHEMA", "true").lower() == "true"

# ========= AWS clients (let retries) =========
bedrock = boto3.client(
    "bedrock-runtime",
    region_name=MODEL_REGION,
    config=Config(retries={"max_attempts": 3})
)

ALLOWED_CHARGE_TYPES = [
    "Transportation",
    "Print label",
    "Discount",
    "Fuel surcharge",
    "Other surcharge",
    "Duties & taxes",
    "VAT",
    "Mixed"
]

# ========= Compact Structure SCHEMA =========
COMPACT_JSON_SCHEMA = {
    "type": "object",
    "required": ["header", "lines", "meta"],
    "properties": {
        "header": {
            "type": "object",
            "required": [
                "invoice_number",
                "invoice_date",
                "invoice_currency",
                "supplier_name"
            ],
            "properties": {
                # From invoice
                "invoice_number": {"type": "string"},
                "invoice_date": {
                    "type": "string",
                    "pattern": r"^\d{4}-\d{2}-\d{2}$"
                },
                "invoice_currency": {
                    "type": "string",
                    "minLength": 3,
                    "maxLength": 3
                },
                "invoice_amount_net": {"type": "number"},   # from invoice
                "vat_on_invoice": {"type": "number"},       # from invoice if present
                "supplier_name": {"type": "string"},        # from invoice

                # Backend/lookup fields – LLM must leave empty if not on invoice
                "supplier_legal_entity": {"type": "string"},
                "vendor_id": {"type": "string"},
                "supplier_tax_id": {"type": "string"}
            },
            "additionalProperties": True
        },
        "lines": {
            "type": "array",
            "items": {
                "type": "object",
                "required": [
                    "invoice_number",
                    "invoice_currency",
                    "charge_type",
                    "net_charges",
                    "source_refs"
                ],
                "properties": {
                    # Repeated / join fields
                    "invoice_number": {"type": "string"},
                    "invoice_amount_net": {"type": "number"},
                    "invoice_currency": {
                        "type": "string",
                        "minLength": 3,
                        "maxLength": 3
                    },

                    # From invoice line table
                    "our_account_number": {"type": "string"},
                    "shipment_id": {"type": "string"},
                    "tracking_id": {"type": "string"},
                    "reference_1": {"type": "string"},
                    "reference_2": {"type": "string"},
                    "shipment_date": {
                        "type": "string",
                        "pattern": r"^\d{4}-\d{2}-\d{2}$"
                    },
                    "billed_weight": {"type": "number"},
                    "charge_type": {
                        "type": "string",
                        "enum": ALLOWED_CHARGE_TYPES
                    },
                    "net_charges": {"type": "number"},
                    "country": {"type": "string"},

                    # Lookup / backend-calculated fields (LLM should leave blank)
                    "product": {"type": "string"},
                    "retailer_name": {"type": "string"},
                    "period": {"type": "string"},

                    # Extra for debugging / provenance
                    "source_refs": {
                        "type": "object",
                        "required": ["page", "table_index", "row_index"],
                        "properties": {
                            "page": {"type": "integer", "minimum": 1},
                            "table_index": {"type": "integer", "minimum": 0},
                            "row_index": {"type": "integer", "minimum": 0}
                        },
                        "additionalProperties": False
                    },
                    "extra": {
                        "type": "object",
                        "additionalProperties": {
                            "type": ["string", "number", "boolean"]
                        }
                    }
                },
                "additionalProperties": True
            }
        },
        "meta": {
            "type": "object",
            "required": ["source_object_key", "normalization_notes"],
            "properties": {
                "source_object_key": {"type": "string"},
                "normalization_notes": {
                    "type": "array",
                    "items": {"type": "string"}
                }
            },
            "additionalProperties": True
        }
    },
    "additionalProperties": False
}

COMPACT_SCHEMA_EXAMPLE = r"""
{
  "header": {
    "invoice_number": "",
    "invoice_date": "YYYY-MM-DD",
    "invoice_currency": "EUR",
    "invoice_amount_net": 0.0,
    "vat_on_invoice": 0.0,
    "supplier_name": "",
    "supplier_legal_entity": "",
    "vendor_id": "",
    "supplier_tax_id": ""
  },
  "lines": [
    {
      "invoice_number": "",
      "invoice_amount_net": 0.0,
      "invoice_currency": "EUR",
      "our_account_number": "",
      "shipment_id": "",
      "tracking_id": "",
      "reference_1": "",
      "reference_2": "",
      "shipment_date": "YYYY-MM-DD",
      "billed_weight": 0.0,
      "charge_type": "Transportation",
      "net_charges": 0.0,
      "product": "",
      "country": "",
      "retailer_name": "",
      "period": "",
      "source_refs": {
        "page": 1,
        "table_index": 0,
        "row_index": 0
      },
      "extra": {}
    }
  ],
  "meta": {
    "source_object_key": "",
    "normalization_notes": []
  }
}
""".strip()

COMPACT_SCHEMA_RULES = (
    "Output ONLY valid JSON (no markdown). "
    "Use the exact keys and structure shown. "
    "Dates must be 'YYYY-MM-DD'. "
    "Monetary values must be numbers (not strings). "
    "Use '.' as decimal separator. "
    f"Always include 'invoice_currency' (default to '{CURRENCY_DEFAULT}' if unknown). "
    "Do NOT perform any arithmetic. Do NOT add, subtract or recompute any amounts. "
    "Copy each numeric amount exactly from the invoice line or header where it appears. "
    "If a field is not present on the invoice (for example product, supplier_legal_entity, vendor_id, "
    "supplier_tax_id, retailer_name or period), leave it empty or 0.0. Do NOT guess or invent values. "
    "Do NOT aggregate or summarise multiple rows into one line. "
    "For every detailed invoice line table (tables that list individual services/shipments/items "
    "with quantities and line amounts), produce exactly one 'lines' item for each row in that table. "
    "Do NOT create 'lines' items from pure headers or summary/total overview tables such as "
    "'Invoice at a glance', 'Summary', 'Totals' or similar. Those tables may only be used to "
    "populate header fields like 'invoice_amount_net' or 'vat_on_invoice'. "
    "For each 'lines' item you MUST fill 'source_refs.page/table_index/row_index'. "

    "Put any extra useful row columns (SKU, cost center, etc.) into 'lines[i].extra'. "
    "The field 'charge_type' MUST be exactly one of: "
    + ', '.join(ALLOWED_CHARGE_TYPES)
    + ". If you are unsure, use 'Mixed'."
)

SYSTEM_INSTRUCTIONS = (
    "You are a financial data normalizer. Convert the given LEAN JSON "
    "into COMPACT JSON with a strict schema.\n\n"
    "SCHEMA (exact):\n"
    + COMPACT_SCHEMA_EXAMPLE
    + "\n\n"
    "RULES:\n"
    + COMPACT_SCHEMA_RULES
)

# ========= Helpers =========
def _log(stage: str, **kv):
    """
    Consistent JSON logger (one line per event), compatible with CloudWatch Logs metric filters.
    """
    # Core fields that appear in every log line
    kv.setdefault("app", os.getenv("APP_NAME", "lean_to_compact"))
    kv.setdefault("model_id", MODEL_ID)
    kv.setdefault("model_region", MODEL_REGION)
    # Useful for comparing before/after once chunking is enabled
    kv.setdefault("chunking", os.getenv("CHUNKING", "off"))
    # Add UTC timestamp (makes searching in Logs easier)
    kv.setdefault("ts_iso", datetime.utcnow().isoformat(timespec="milliseconds") + "Z")

    # Handle non-JSON-serializable types (Decimal, datetime, etc.)
    def _json_default(o):
        if isinstance(o, Decimal):
            return float(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)

    # Print as clean JSON (this is what allows CloudWatch metric filters to work)
    try:
        print(json.dumps({"stage": stage, **kv}, ensure_ascii=False, separators=(",", ":"), default=_json_default))
    except TypeError:
        # Fallback: convert any unsupported objects to strings
        safe = {k: (float(v) if isinstance(v, Decimal) else (v.isoformat() if isinstance(v, datetime) else v)) for k, v in kv.items()}
        print(json.dumps({"stage": stage, **safe}, ensure_ascii=False, separators=(",", ":")))

def _to_float(x):
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, Decimal):
        return float(x)
    s = str(x).replace(",", ".").strip()
    try:
        return float(s)
    except Exception:
        return 0.0

def _iso_date(s):
    if not s:
        return ""
    s = str(s).strip()
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%Y/%m/%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except Exception:
            pass
    return ""

def _normalize_and_fix(compact: dict, src_key: str) -> dict:
    # ----- Header -----
    hdr = compact.get("header", {}) or {}
    hdr.setdefault("invoice_currency", CURRENCY_DEFAULT)

    hdr["invoice_date"] = _iso_date(hdr.get("invoice_date"))
    # Only normalise numeric format, no calculations
    if "invoice_amount_net" in hdr:
        hdr["invoice_amount_net"] = _to_float(hdr.get("invoice_amount_net"))
    else:
        hdr["invoice_amount_net"] = 0.0

    if "vat_on_invoice" in hdr:
        hdr["vat_on_invoice"] = _to_float(hdr.get("vat_on_invoice"))
    else:
        hdr["vat_on_invoice"] = 0.0

    compact["header"] = hdr

    # ----- Lines -----
    lines = compact.get("lines", []) or []
    norm_lines = []
    for ln in lines:
        n = {
            "invoice_number": ln.get("invoice_number", hdr.get("invoice_number", "")) or "",
            "invoice_amount_net": _to_float(
                ln.get("invoice_amount_net", hdr.get("invoice_amount_net", 0.0))
            ),
            "invoice_currency": ln.get("invoice_currency", hdr.get("invoice_currency", CURRENCY_DEFAULT)) or CURRENCY_DEFAULT,
            "our_account_number": ln.get("our_account_number", "") or "",
            "shipment_id": ln.get("shipment_id", "") or "",
            "tracking_id": ln.get("tracking_id", "") or "",
            "reference_1": ln.get("reference_1", "") or "",
            "reference_2": ln.get("reference_2", "") or "",
            "shipment_date": _iso_date(ln.get("shipment_date", "")),
            "billed_weight": _to_float(ln.get("billed_weight")),
            "net_charges": _to_float(ln.get("net_charges")),
            "country": ln.get("country", "") or "",
            "product": ln.get("product", "") or "",
            "retailer_name": ln.get("retailer_name", "") or "",
            "period": ln.get("period", "") or "",
            "source_refs": ln.get("source_refs", {}) or {},
            "extra": ln.get("extra", {}) or {}
        }

        # Normalise charge_type to allowed list, default to Mixed
        raw_ct = (ln.get("charge_type") or "").strip()
        if raw_ct:
            match = None
            for ct in ALLOWED_CHARGE_TYPES:
                if raw_ct.lower() == ct.lower():
                    match = ct
                    break
            n["charge_type"] = match or "Mixed"
        else:
            n["charge_type"] = "Mixed"

        norm_lines.append(n)

    compact["lines"] = norm_lines

    # ----- Meta -----
    meta = compact.get("meta", {}) or {}
    meta["source_object_key"] = src_key
    meta.setdefault("normalization_notes", [])
    compact["meta"] = meta

    return compact

class ModelJsonParseError(Exception):
    def __init__(self, raw_text: str, preview: str = ""):
        super().__init__("Model returned non-JSON")
        self.raw_text = raw_text
        self.preview = preview or raw_text[:400]

# --- schema import (fail-soft) ---
try:
    from jsonschema import validate as _js_validate, ValidationError as _JsErr
except Exception:
    _js_validate = None
    class _JsErr(Exception): ...
    
def _validate_against_schema(obj: dict) -> Optional[str]:
    """ Returs None if valid; else a short error message. Respects STRICT_SCHEMA and only runs if jsonschema-lib is present. """
    if not STRICT_SCHEMA or _js_validate is None:
        return None
    try:
        _js_validate(instance=obj, schema=COMPACT_JSON_SCHEMA)
        return None
    except _JsErr as e:
        return str(e)

# ========= Bedrock: slice-variant =========
def _invoke_bedrock_for_slice(lean_json_slice: dict, src_key: str, chunk_meta: dict | None = None) -> dict:
    """
    Run Bedrock on a single LEAN chunk to produce COMPACT JSON.

    Notes:
    - No input truncation: the entire slice is sent (chunking keeps it small).
    - Adds CHUNK CONTEXT (docId/chunkId/pageRange) so the model only processes this slice.
    - Same Anthropic/Bedrock request shape as single-shot (system + one user message).
    - Robust parsing: strips code fences, fixes trailing commas, best-effort JSON extraction; raises ModelJsonParseError on parse failure.
    - Logs timing and input sizes for observability.

    Returns:
    - COMPACT JSON (dict) for this chunk only.
    """
    # --- 1) Build input text (NO truncation here) ---
    lean_text_full = json.dumps(lean_json_slice, ensure_ascii=False)
    lean_text = lean_text_full  # no slice truncation
    _log("lean_prompt_sizes",
         full_chars=len(lean_text_full),
         used_chars=len(lean_text),
         snippet_limit="(no-truncation-in-slice)",
         src_key=src_key,
         chunk_meta=(chunk_meta or {}))

    # --- 2) User prompt (mapping + the LEAN SLICE + optional CHUNK CONTEXT) ---
    chunk_ctx = ""
    if chunk_meta:
        # Keep it brief—just enough for the model to understand scope
        chunk_ctx = (
            "\n\nCHUNK CONTEXT:\n" +
            json.dumps({
                "docId": chunk_meta.get("docId"),
                "chunkId": chunk_meta.get("chunkId"),
                "pageRange": chunk_meta.get("pageRange"),
                "instructions": [
                    "Process ONLY this chunk; do not infer outside this content.",
                    "Return valid COMPACT JSON for this chunk."
                ]
            }, ensure_ascii=False)
        )

    user_prompt = (
    "Task: Convert LEAN JSON to COMPACT JSON for an invoice header and its charge lines.\n\n"
    f"Default currency: {CURRENCY_DEFAULT}\n\n"
    "IMPORTANT: The LEAN JSON contains a 'tables' array with many tables across multiple pages.\n"
    "Each table has 'page', 'headers' and 'rows'. You MUST use these tables as the ONLY source for line items.\n"
    "For each table, first decide if it is a DETAILED LINE TABLE or a SUMMARY/OVERVIEW table:\n"
    "- DETAILED LINE TABLE: tables that list individual services/shipments/items with columns like "
    "Description, Quantity, Unit price, Amount, etc. For these tables you MUST create one 'lines' entry "
    "for every row in 'rows' and you must NOT skip any rows.\n"
    "- SUMMARY/OVERVIEW table: high-level summaries such as 'Invoice at a glance', 'Summary', 'Totals', "
    "or tables that only show aggregated amounts per month or per contract without individual line details. "
    "Do NOT create 'lines' entries from these tables. You may only use them to fill header fields like "
    "'invoice_amount_net' or 'vat_on_invoice'.\n"
    "Previous months that are shown with detailed rows (with quantities and amounts per line) should be "
    "treated as normal DETAILED LINE TABLES and converted to 'lines' as well.\n\n"

    "Header mapping (read from invoice when available):\n"
    "- Invoice number → header.invoice_number\n"
    "- Invoice date → header.invoice_date\n"
    "- Invoice currency → header.invoice_currency\n"
    "- Invoice amount (net) → header.invoice_amount_net\n"
    "- VAT on invoice → header.vat_on_invoice\n"
    "- Supplier name → header.supplier_name\n"
    "- Supplier legal entity → header.supplier_legal_entity (only if printed on invoice, otherwise leave empty)\n"
    "- Vendor ID → header.vendor_id (only if printed on invoice, otherwise leave empty)\n"
    "- Supplier tax ID / VAT ID → header.supplier_tax_id (from invoice if present, otherwise leave empty)\n\n"
    "Line mapping (for each row in every DETAILED LINE TABLE):\n"
    "- Invoice number → lines[i].invoice_number\n"
    "- Invoice amount (net) column → lines[i].invoice_amount_net\n"
    "- Invoice currency → lines[i].invoice_currency\n"
    "- Our account number → lines[i].our_account_number\n"
    "- Shipment ID → lines[i].shipment_id\n"
    "- Tracking ID → lines[i].tracking_id\n"
    "- Reference 1 → lines[i].reference_1\n"
    "- Reference 2 → lines[i].reference_2\n"
    "- Shipment date → lines[i].shipment_date\n"
    "- Billed weight → lines[i].billed_weight\n"
    "- Net charges for that line → lines[i].net_charges\n"
    "- Country → lines[i].country\n\n"
    "Charge type classification:\n"
    "For each line, set lines[i].charge_type to ONE of these values only:\n"
    "- Transportation\n"
    "- Print label\n"
    "- Discount\n"
    "- Fuel surcharge\n"
    "- Other surcharge\n"
    "- Duties & taxes\n"
    "- VAT\n"
    "- Mixed (use this if you are not sure)\n\n"
    "Lookup/backend fields:\n"
    "- lines[i].product, lines[i].retailer_name and lines[i].period are filled by backend systems. "
    "If they are not explicitly present on the invoice, leave them empty. Do NOT guess these values.\n\n"
    "Very important behavioural rules:\n"
    "- Use the 'tables' array from the LEAN JSON. Iterate over ALL DETAILED LINE TABLES in page order, and over ALL rows in each of those tables, "
    "and create one 'lines' entry per row.\n"
    "- Do NOT perform any arithmetic. Do NOT add or subtract amounts. Do NOT calculate totals. "
    "Just copy each numeric amount exactly as shown in the invoice tables or header.\n"
    "- Do NOT merge, group or summarise lines. Never drop rows in a DETAILED LINE TABLE, even for previous months or adjustments.\n"
    "- If a value is missing on the invoice, use an empty string for text fields or 0.0 for numeric fields.\n"
    "- Fill lines[i].source_refs.page/table_index/row_index using the metadata from the LEAN JSON so each line can be traced back.\n"
    "- Respond with strictly valid COMPACT JSON only, no explanations or comments.\n\n"
    "Input LEAN JSON (chunk). Respond with COMPACT JSON only:\n"
    f"{lean_text}"
    f"{chunk_ctx}"
)



    # --- 3) Anthropic request body (system + a single user message) ---
    body = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": MAX_TOKENS,
        "temperature": TEMPERATURE,
        "system": SYSTEM_INSTRUCTIONS,
        "messages": [
            {
                "role": "user",
                "content": [{"type": "text", "text": user_prompt}]
            }
        ]
    }

    # --- 4) Invoke Bedrock with timing + explicit errors ---
    t0 = time.time()
    _log("bedrock_invoke_start", model=MODEL_ID, region=MODEL_REGION, src_key=src_key)
    resp = bedrock.invoke_model(
        modelId=MODEL_ID,
        accept="application/json",
        contentType="application/json",
        body=json.dumps(body)
    )
    ms = int((time.time() - t0) * 1000)
    _log("bedrock_invoke_ok", elapsed_ms=ms, src_key=src_key)

    # --- 5) Parse response ---
    payload = resp["body"].read()
    try:
        data = json.loads(payload)
    except Exception:
        _log("bedrock_parse_failed_raw_payload", bytes=len(payload), src_key=src_key)
        raise

    text = ""
    content = data.get("content")
    if isinstance(content, list) and content:
        parts = []
        for p in content:
            if isinstance(p, dict) and p.get("type") == "text":
                parts.append(p.get("text", ""))
        text = "".join(parts).strip()
    else:
        text = (data.get("output_text") or "").strip()

    if not text:
        raise RuntimeError("Empty response text from Bedrock model")

    # --- 6) Strip fences + robust parse ---
    t = text.strip()
    if t.startswith("```"):
        t = t.strip().strip("`")
        if t.lower().startswith("json"):
            t = t[4:].strip()

    def _try_parse_json(s: str):
        try:
            return json.loads(s)
        except Exception:
            pass

        start, end = s.find("{"), s.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = s[start:end+1]
            try:
                return json.loads(candidate)
            except Exception:
                pass
        # Remove trailing commas
        import re
        fixed = re.sub(r",(\s*[}\]])", r"\1", s)
        try:
            return json.loads(fixed)
        except Exception:
            return None

    compact = _try_parse_json(t)
    if compact is None:
        preview = t[:400]
        _log("json_loads_failed", preview=preview, src_key=src_key)
        raise ModelJsonParseError(raw_text=t, preview=preview)

    _log("bedrock_done", has_data=bool(compact), src_key=src_key)
    return compact


# ========= Entry for Chunker =========
def run_compact_for_slice(lean_slice: Dict[str, Any],
                          src_key: str,
                          *,
                          chunk_meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Produce a normalized COMPACT JSON object for this chunk."""
    
    compact_raw = _invoke_bedrock_for_slice(lean_slice, src_key, chunk_meta)
    compact = _normalize_and_fix(compact_raw, src_key)

    err = _validate_against_schema(compact)
    if err:
        _log("schema_fail_in_core", err=err)

    return compact


