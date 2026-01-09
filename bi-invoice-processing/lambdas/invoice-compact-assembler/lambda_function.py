from __future__ import annotations
import os, json, hashlib
from datetime import datetime, timezone
from typing import Dict, Any, List, Tuple
import boto3
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

# --------- ENV ---------
RESULTS_BUCKET = os.environ["RESULTS_BUCKET"]
MANIFESTS_BUCKET = os.environ.get("MANIFESTS_BUCKET", RESULTS_BUCKET)

CHUNKS_PREFIX    = os.getenv("CHUNKS_PREFIX", "pipeline/compact-chunks/")
MANIFESTS_PREFIX = os.getenv("MANIFESTS_PREFIX", "pipeline/manifests/")
COMPACT_PREFIX   = os.getenv("COMPACT_PREFIX", "pipeline/compact/")

# ---- Helpers ----
def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def _read_json(bucket: str, key: str) -> Dict[str, Any]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    return json.loads(body)

def _put_json(bucket: str, key: str, data: Dict[str, Any]) -> None:
    body = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=body,
        ContentType="application/json"
    )

def _list_keys(bucket: str, prefix: str) -> List[str]:
    keys = []
    kwargs = dict(Bucket=bucket, Prefix=prefix)
    while True:
        resp = s3.list_objects_v2(**kwargs)
        for it in resp.get("Contents", []):
            keys.append(it["Key"])
        if resp.get("IsTruncated"):
            kwargs["ContinuationToken"] = resp["NextContinuationToken"]
        else:
            break
    return keys

def _manifest_candidates() -> List[str]:
    keys = _list_keys(MANIFESTS_BUCKET, MANIFESTS_PREFIX)
    return [k for k in keys if k.endswith(".json")]


def _safe_chunk_id(val: Any) -> Tuple[int, str]:
    s = str(val)
    try:
        return (int(s), s)
    except ValueError:
        return (10**9, s)  # unknown chunkIds

def _line_dedupe_key(line: Dict[str, Any]) -> str:
    src = line.get("source_refs") or {}
    payload = [
        (line.get("invoice_number") or "").strip(),
        line.get("net_charges"),
        src.get("page"),
        src.get("table_index"),
        src.get("row_index"),
    ]
    return hashlib.sha1(
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    ).hexdigest()


def _merge_chunks(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    # sort by chunkId
    def chunk_key(c):
        meta = c.get("meta", {})
        return _safe_chunk_id(meta.get("chunkId", meta.get("chunk_id", "")))
    chunks_sorted = sorted(chunks, key=chunk_key)

    merged_header: Dict[str, Any] = {}
    merged_lines: List[Dict[str, Any]] = []
    seen: set[str] = set()
    merged_meta: Dict[str, Any] = {"chunked": True, "chunks": len(chunks_sorted)}

    for c in chunks_sorted:
        # header:
        h = c.get("header") or {}
        if h:
            merged_header.update(h)

        # lines: append + dedup
        for ln in c.get("lines") or []:
            key = _line_dedupe_key(ln)
            if key in seen:
                continue
            seen.add(key)
            merged_lines.append(ln)

        
        # meta: preserve any info, but keep our flags at the end
        m = c.get("meta") or {}
        for k, v in m.items():
            if k not in ("chunked", "chunks"):
                merged_meta[k] = v

    merged_meta.update({"chunked": True, "chunks": len(chunks_sorted)})

    return {
        "header": merged_header,
        "lines": merged_lines,
        "meta": merged_meta
    }

def _all_chunk_files_present(doc_id: str, expected_count: int | None) -> Tuple[bool, List[str]]:
    prefix = f"{CHUNKS_PREFIX}{doc_id}/"
    keys = [k for k in _list_keys(RESULTS_BUCKET, prefix) if k.endswith(".json")]
    if expected_count is not None and len(keys) < expected_count:
        return False, keys
    return (len(keys) > 0, keys)

def _collect_chunks(keys: List[str]) -> List[Dict[str, Any]]:
    chunks = []
    for k in keys:
        try:
            chunks.append(_read_json(RESULTS_BUCKET, k))
        except Exception as e:
            print(f"[WARN] Failed to read chunk {k}: {e}")
    return chunks

def _should_assemble(manifest: Dict[str, Any]) -> Tuple[bool, str, int | None]:
    status = (manifest.get("status") or "").upper()
    if status not in ("IN_PROGRESS", "CHUNKING_COMPLETE"):
        return (False, "", None)

    doc_id = manifest.get("docId") or manifest.get("doc_id")
    if not doc_id:
        return (False, "", None)

    expected: int | None = None

    # Preferred: summary.chunks
    summary = manifest.get("summary")
    if isinstance(summary, dict) and isinstance(summary.get("chunks"), int):
        expected = summary["chunks"]
    # Backwards-compat: if someone stored chunks as dict
    elif isinstance(manifest.get("chunks"), dict):
        expected = len([c for c in manifest["chunks"].values() if isinstance(c, dict)])
    # Or explicit expected_chunks
    elif isinstance(manifest.get("expected_chunks"), int):
        expected = manifest["expected_chunks"]

    return (True, doc_id, expected)


def lambda_handler(event, context):
    started = _now_iso()
    print(f"[Assembler] Start {started}")

    manifest_keys = _manifest_candidates()
    assembled_docs = 0
    skipped = 0
    errors = 0

    for mk in manifest_keys:
        try:
            manifest = _read_json(MANIFESTS_BUCKET, mk)
        except ClientError as e:
            print(f"[WARN] Cannot read manifest {mk}: {e}")
            skipped += 1
            continue

        should, doc_id, expected = _should_assemble(manifest)
        if not should:
            skipped += 1
            continue

        final_key = f"{COMPACT_PREFIX}{doc_id}.json"
        # idempotency: if output already exists, set status and skip
        try:
            s3.head_object(Bucket=RESULTS_BUCKET, Key=final_key)
            if (manifest.get("status") or "").upper() != "COMPACT_COMPLETE":
                manifest["status"] = "COMPACT_COMPLETE"
                manifest["completedAt"] = _now_iso()
                _put_json(MANIFESTS_BUCKET, mk, manifest)
            skipped += 1
            continue
        except ClientError:
            pass  

        ok, chunk_keys = _all_chunk_files_present(doc_id, expected)
        if not ok:
            skipped += 1
            continue

        # read chunks
        chunks = _collect_chunks(chunk_keys)
        if not chunks:
            skipped += 1
            continue

        # merge
        compact = _merge_chunks(chunks)

        _put_json(RESULTS_BUCKET, final_key, compact)

        # update manifest
        manifest["status"] = "COMPACT_COMPLETE"
        manifest["completedAt"] = _now_iso()
        manifest.setdefault("docId", doc_id)
        manifest["compactKey"] = final_key
        _put_json(MANIFESTS_BUCKET, mk, manifest)

        assembled_docs += 1
        print(f"[Assembler] Wrote {final_key}")

    ended = _now_iso()
    print(json.dumps({
        "component": "assembler",
        "started": started,
        "ended": ended,
        "assembled": assembled_docs,
        "skipped": skipped,
        "errors": errors
    }))
    return {"assembled": assembled_docs, "skipped": skipped, "errors": errors}
