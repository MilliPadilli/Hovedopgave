import json
import os
import boto3
import logging

# --- Logging (controlled by LOG_LEVEL env var, default INFO) ---
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.getLogger().setLevel(LOG_LEVEL)
log = logging.getLogger(__name__)

# --- AWS clients ---
s3 = boto3.client("s3")

# --- Read env vars set in the Lambda Console ---
REQUIRED = ("ANALYSES_BUCKET", "PROCESSED_BUCKET")
missing = [k for k in REQUIRED if k not in os.environ]
if missing:
    raise RuntimeError(f"Missing required env var(s): {', '.join(missing)}")

ANALYSES_BUCKET  = os.environ["ANALYSES_BUCKET"]     # RAW Textract JSON bucket
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]    # Output bucket
RAW_PREFIX       = os.environ.get("RAW_PREFIX", "textract/")
LEAN_PREFIX      = os.environ.get("LEAN_PREFIX", "lean/")
INCLUDE_LINES    = os.environ.get("INCLUDE_LINES_TEXT", "true").lower() == "true"

# ---------- Helpers ----------
def _page_count(blocks, raw):
    # Prefer DocumentMetadata.Pages; fallback to max(Page) in blocks
    dm = raw.get("DocumentMetadata", {})
    if isinstance(dm.get("Pages"), int):
        return dm["Pages"]
    pages = [b.get("Page") for b in blocks if isinstance(b.get("Page"), int)]
    return max(pages) if pages else 1

def _build_block_map(blocks):
    # Map block Id -> block
    return {b.get("Id", f"row-{i}"): b for i, b in enumerate(blocks) if isinstance(b, dict)}

def _text_from_block(block, block_map):
    # Join WORD children; add ☑ for selected checkboxes
    if "Relationships" not in block:
        return ""
    out = []
    for rel in block.get("Relationships", []):
        if rel.get("Type") != "CHILD":
            continue
        for cid in rel.get("Ids", []):
            child = block_map.get(cid)
            if not child:
                continue
            bt = child.get("BlockType")
            if bt == "WORD":
                out.append(child.get("Text", ""))
            elif bt == "SELECTION_ELEMENT" and child.get("SelectionStatus") == "SELECTED":
                out.append("☑")
    return " ".join(out).strip()

def _extract_fields(blocks):
    # Extract KEY_VALUE_SET (FORMS)
    block_map = _build_block_map(blocks)
    keys, values = {}, {}
    for b in blocks:
        if b.get("BlockType") == "KEY_VALUE_SET":
            ents = b.get("EntityTypes", [])
            if "KEY" in ents:
                keys[b["Id"]] = b
            elif "VALUE" in ents:
                values[b["Id"]] = b

    result = []
    for k_id, k in keys.items():
        key_text = _text_from_block(k, block_map)
        val_text, conf = "", k.get("Confidence", 0.0)
        for rel in k.get("Relationships", []):
            if rel.get("Type") != "VALUE":
                continue
            for v_id in rel.get("Ids", []):
                v = values.get(v_id)
                if not v:
                    continue
                val_text = _text_from_block(v, block_map)
                conf = v.get("Confidence", conf)
        if key_text:
            # Textract confidence is 0..100; store as 0..1
            result.append({
                "key": key_text,
                "value": val_text,
                "confidence": round((conf or 0.0) / 100.0, 4)
            })
    return result

def _extract_tables(blocks):
    # Extract TABLES as headers + rows
    block_map = _build_block_map(blocks)
    tables = []
    for b in blocks:
        if b.get("BlockType") != "TABLE":
            continue
        page = b.get("Page", 1)
        cells = []
        for rel in b.get("Relationships", []):
            if rel.get("Type") != "CHILD":
                continue
            for cid in rel.get("Ids", []):
                cell = block_map.get(cid)
                if not cell or cell.get("BlockType") != "CELL":
                    continue
                text = _text_from_block(cell, block_map)
                cells.append({
                    "row": cell.get("RowIndex", 0),
                    "col": cell.get("ColumnIndex", 0),
                    "text": text
                })
        if not cells:
            continue
        max_r = max(c["row"] for c in cells)
        max_c = max(c["col"] for c in cells)
        matrix = [["" for _ in range(max_c)] for _ in range(max_r)]
        for c in cells:
            r, cidx = c["row"] - 1, c["col"] - 1
            if 0 <= r < max_r and 0 <= cidx < max_c:
                matrix[r][cidx] = c["text"]
        headers = matrix[0] if matrix else []
        rows = matrix[1:] if len(matrix) > 1 else []
        tables.append({"page": page, "headers": headers, "rows": rows})
    return tables

def _extract_lines_by_page(blocks):
    # Group Textract LINE text per page
    by_page = {}
    for b in blocks:
        if b.get("BlockType") == "LINE" and b.get("Text"):
            p = b.get("Page", 1)
            by_page.setdefault(p, []).append(b["Text"])

    pages = []
    for p in sorted(by_page):
        text = "\n".join(by_page[p]).strip()
        if text:
            pages.append({"page": p, "text": text})

    full_text = "\n\n".join([f"— Page {o['page']} —\n{o['text']}" for o in pages])
    return pages, full_text

def _job_id_from_key(key):
    # Use filename (without .json) as job_id
    name = key.split("/")[-1]
    return name[:-5] if name.endswith(".json") else name

def _normalize_raw(raw):
    """
    Accept:
    - dict: normal AnalyzeDocument response
    - list of dicts: merge all with "Blocks" (common when you saved multiple pages/responses)
    - list of blocks: wrap as {"Blocks": [...]}
    """
    # Single dict -> ok
    if isinstance(raw, dict):
        return raw

    # List-like -> try to merge/unwrap
    if isinstance(raw, list):
        # Empty list -> not supported
        if not raw:
            raise ValueError("Empty Textract list")

        # If it's [ {DocumentMetadata..., Blocks:[...]}, {...}, ... ] -> merge
        if all(isinstance(x, dict) for x in raw) and any("Blocks" in x for x in raw):
            merged_blocks = []
            pages_hint = None
            # take first DocumentMetadata if present
            dm = {}
            for part in raw:
                if "DocumentMetadata" in part and not dm:
                    dm = part.get("DocumentMetadata", {})
                if "Blocks" in part and isinstance(part["Blocks"], list):
                    merged_blocks.extend(part["Blocks"])
                # remember a Pages hint
                if "DocumentMetadata" in part and "Pages" in part["DocumentMetadata"]:
                    pages_hint = part["DocumentMetadata"]["Pages"]
            return {"DocumentMetadata": dm or ({"Pages": pages_hint} if pages_hint else {}),
                    "Blocks": merged_blocks}

        # If it's a list of blocks -> wrap
        if isinstance(raw[0], dict) and ("BlockType" in raw[0] or "Id" in raw[0]):
            return {"Blocks": raw}

        # If it's a single-object list -> unwrap
        if len(raw) == 1 and isinstance(raw[0], dict):
            return raw[0]

    # No known shape
    raise ValueError("Unsupported Textract JSON shape")

# ---------- Entry point ----------
def lambda_handler(event, context):
    # Get bucket/key from S3 event
    rec = event["Records"][0]
    bucket = rec["s3"]["bucket"]["name"]
    key = rec["s3"]["object"]["key"]

    # Guard: only process our RAW bucket/prefix
    if bucket != ANALYSES_BUCKET or not key.startswith(RAW_PREFIX):
        log.info("Skip: %s/%s", bucket, key)
        return {"skip": f"{bucket}/{key}"}

    # Only JSON inputs
    if not key.lower().endswith(".json"):
        log.info("Skip non-JSON: %s", key)
        return {"skip": key}

    log.info("Read RAW: s3://%s/%s", bucket, key)
    obj = s3.get_object(Bucket=bucket, Key=key)
    raw = json.loads(obj["Body"].read())
    raw = _normalize_raw(raw)  # ensure dict with "Blocks"

    blocks = raw.get("Blocks", [])
    pages = _page_count(blocks, raw)
    job_id = _job_id_from_key(key)

    # Build lean/compact output
    lean = {
        "doc_meta": {"job_id": job_id, "pages": pages},
        "fields": _extract_fields(blocks),
        "tables": _extract_tables(blocks)
    }

    if INCLUDE_LINES:
        # NEW: per-page + full text
        pages_objs, full_text = _extract_lines_by_page(blocks)

        # Keep full text for readability/debug
        lean["lines_text"] = full_text

        # Add per-page structures so the orchestrator can chunk properly
        # 1) objects: [{"text": "..."}] (preferred by orchestrator)
        lean["pages"] = [{"text": o["text"]} for o in pages_objs]

        # 2) plain list of strings (optional/backward-compatible)
        lean["page_texts"] = [o["text"] for o in pages_objs]

    # Safe log even if INCLUDE_LINES=false
    if "pages" in lean:
        log.info("LEAN pages count: %d", len(lean["pages"]))

    # Write to processed bucket
    out_key = f"{LEAN_PREFIX}{job_id}.json"
    log.info("Write LEAN: s3://%s/%s", PROCESSED_BUCKET, out_key)
    s3.put_object(
        Bucket=PROCESSED_BUCKET,
        Key=out_key,
        Body=json.dumps(lean, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json"
    )

    log.info("Done: fields=%d, tables=%d, include_lines=%s",
             len(lean["fields"]), len(lean["tables"]), INCLUDE_LINES)

    return {
        "wrote": f"s3://{PROCESSED_BUCKET}/{out_key}",
        "fields": len(lean["fields"]),
        "tables": len(lean["tables"]),
        "include_lines": INCLUDE_LINES
    }
