import os, json, boto3, botocore, traceback
from typing import Dict, Any, List

# --------- ENV ---------
MODEL_REGION = os.environ["MODEL_REGION"]
MODEL_ID     = os.environ["MODEL_ID"]
DEST_BUCKET  = os.environ["DEST_BUCKET"]
MANIFEST_BUCKET = os.environ["MANIFEST_BUCKET"]
CURRENCY_DEFAULT = os.getenv("CURRENCY_DEFAULT", "EUR")
MAX_TOKENS = int(os.getenv("MAX_TOKENS", "4000"))
STRICT_SCHEMA = os.getenv("STRICT_SCHEMA", "true").lower() == "true"

s3 = boto3.client("s3")

# --------- From layer (compact_core) ---------
# Expected signatures:
#   run_compact_for_slice(lean_slice, *, src_key, chunk_meta, model_id, model_region) -> dict
#   validate_compact_schema(payload) -> None or raise Exception
from compact_core import run_compact_for_slice, _validate_against_schema

# ---- Helpers ----
def _s3_get_json(bucket: str, key: str) -> Any:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)

def _s3_put_json(bucket: str, key: str, obj: Any) -> None:
    s3.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

def _s3_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NotFound", "NoSuchKey"):
            return False
        raise

def _extract_pages(data):
    if isinstance(data, dict) and isinstance(data.get("pages"), list):
        return data["pages"]
    if isinstance(data, dict) and isinstance(data.get("page_texts"), list):
        # normalize page_texts → array of page dicts
        return [{"text": t} for t in data["page_texts"]]
    # fallback: single page
    return [data]


def lambda_handler(event, context):
    import json
    print("EVENT DUMP:", json.dumps(event)[:1500])

    """
    SQS batch handler with partial batch failure support.
    Message body must be JSON: { "docId": "...", "chunkId": "0001" }
    """
    failures: List[str] = []  # list of messageIds to retry

    for rec in event.get("Records", []):
        msg_id = rec["messageId"]
        try:
            body = json.loads(rec["body"])
            doc_id   = body["docId"]
            chunk_id = body["chunkId"]

            # ---- Load manifest and locate chunk plan ----
            mf_bucket = MANIFEST_BUCKET
            mf_key = f"pipeline/manifests/{doc_id}.json"
            manifest  = _s3_get_json(mf_bucket, mf_key)


            source_bucket = manifest["source"]["bucket"]
            source_key    = manifest["source"]["key"]

            chunk_plan = next(c for c in manifest["chunks"] if c["chunkId"] == chunk_id)
            start_p, end_p = chunk_plan["startPage"], chunk_plan["endPage"]
            print({"stage":"start","docId":doc_id,"chunkId":chunk_id,"range":[start_p,end_p]})

            # ---- Idempotency: skip if result already exists ----
            ok_key = f"pipeline/compact-chunks/{doc_id}/{chunk_id}.json"
            exists = _s3_exists(DEST_BUCKET, ok_key)
            print({"stage": "exist_check", "bucket": DEST_BUCKET, "key": ok_key, "exists": exists})
            if exists:
                chunk_plan["status"] = "DONE"
                _s3_put_json(mf_bucket, mf_key, manifest)
                continue


           # ---- Build LEAN slice for the pages we need ----
            lean = _s3_get_json(source_bucket, source_key)
            pages = _extract_pages(lean)

            # slice by index (orchestrator uses 1-based page indices)
            slice_pages = pages[start_p - 1 : end_p]
            print({"stage": "slice_info", "total_pages": len(pages), "slice_len": len(slice_pages),
                "startPage": start_p, "endPage": end_p, "src_key": source_key})
            if not slice_pages:
                raise RuntimeError(f"No pages in slice: start={start_p}, end={end_p}, src={source_key}")

            lean_slice = {"docId": doc_id, "pages": slice_pages}

            # ---- Call Bedrock via the layer helper ----
            chunk_meta = {"docId": doc_id, "chunkId": chunk_id, "pageRange": [start_p, end_p]}
            compact_payload = run_compact_for_slice(
                lean_slice,
                src_key=source_key,
                chunk_meta=chunk_meta,
            )

            # ---- Schema validation gate ----
            _validate_against_schema(compact_payload)

            # ---- Write OK result & update manifest ----
            _s3_put_json(DEST_BUCKET, ok_key, compact_payload)
            chunk_plan["status"] = "DONE"
            _s3_put_json(mf_bucket, mf_key, manifest)

        except Exception as e:
            print(json.dumps({"stage": "error", "err": str(e)}))
            # Write quarantine; mark FAILED in manifest if possible; request retry (partial failure)
            try:
                q_doc   = locals().get("doc_id") or "unknown"
                q_chunk = locals().get("chunk_id") or "unknown"
                q_key = f"pipeline/quarantine/{q_doc}/{q_chunk}.json"
                q_payload = {
                    "error": str(e),
                    "trace": traceback.format_exc(),
                    "message": rec.get("body"),
                }
                _s3_put_json(DEST_BUCKET, q_key, q_payload)

                if "manifest" in locals() and "chunk_plan" in locals():
                    chunk_plan["status"] = "FAILED"
                    _s3_put_json(mf_bucket, mf_key, manifest)
            finally:
                failures.append(msg_id)

    # Partial batch response: only failed messages are retried
    return {"batchItemFailures": [{"itemIdentifier": mid} for mid in failures]}
