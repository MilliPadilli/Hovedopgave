import os, json, time
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError

# --- AWS clients
s3 = boto3.client("s3")
sqs = boto3.client("sqs")

# --- Env vars
QUEUE_URL = os.environ["QUEUE_URL"]
TOKEN_BUDGET = int(os.environ.get("TOKEN_BUDGET", "8000"))
OVERLAP_LINES = int(os.environ.get("OVERLAP_LINES", "8"))
MAX_PAGES_PER_CHUNK = int(os.environ.get("MAX_PAGES_PER_CHUNK", "3"))

SAFETY_HEADROOM = 1000  # room for prompt/systemtext

# --- Helpers
def _now_iso():
    return datetime.now(timezone.utc).isoformat()

def estimate_tokens_for_page(page_obj) -> int:
    """Cheap estimate: ~1 token ≈ 4 characters.."""
    try:
        txt = json.dumps(page_obj, ensure_ascii=False)
    except Exception:
        txt = str(page_obj)
    return max(1, len(txt) // 4)

def _extract_pages(data):
    if isinstance(data, dict) and isinstance(data.get("pages"), list):
        return data["pages"]
    if isinstance(data, dict) and isinstance(data.get("page_texts"), list):
        return [{"text": t} for t in data["page_texts"]]
    return [data]

def plan_chunks(pages):
    """
    Builds a list of chunks with 1-based page ranges. Stops each chunk before we hit the token budget or the max pages.
    """
    chunks = []
    n = len(pages)
    i = 0
    while i < n:
        start = i
        est_sum = 0
        count = 0
        while i < n:
            page_tokens = estimate_tokens_for_page(pages[i])
            if (est_sum + page_tokens) > (TOKEN_BUDGET - SAFETY_HEADROOM) or count >= MAX_PAGES_PER_CHUNK:
                break
            est_sum += page_tokens
            i += 1
            count += 1
        if count == 0:
            # security: at least 1 page per chunk
            est_sum = estimate_tokens_for_page(pages[i])
            i += 1
            count = 1
        end = start + count - 1
        chunks.append({
            "startPage": start + 1, 
            "endPage": end + 1, 
            "estTokens": est_sum})
    return chunks

def write_manifest(bucket, key_manifest, manifest_obj):
    body = json.dumps(manifest_obj, ensure_ascii=False, indent=2).encode("utf-8")
    s3.put_object(Bucket=bucket, Key=key_manifest, Body=body, ContentType="application/json")

def send_chunk_message(chunk, doc_id, bucket, key_manifest, total_pages):
    idx = chunk.get("index", 0)
    msg = {
        "docId": doc_id,
        "chunkId": f"{idx+1:04d}",
        "startPage": chunk["startPage"],
        "endPage": chunk["endPage"],
        "overlapLines": OVERLAP_LINES,
        "tokenBudget": TOKEN_BUDGET,
        "maxPagesPerChunk": MAX_PAGES_PER_CHUNK,
        "manifest": {"bucket": bucket, "key": key_manifest},
        "totalPages": total_pages,
        "ts": int(time.time())
    }
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(msg))

def lambda_handler(event, context):
    # S3-Event can contain several records 
    for rec in event.get("Records", []):
        s3info = rec.get("s3", {})
        bucket = s3info.get("bucket", {}).get("name")
        key = s3info.get("object", {}).get("key")

        if not bucket or not key:
            continue
        if not key.startswith("pipeline/lean/") or not key.endswith(".json"):
            continue

        print(json.dumps({"stage": "start", "bucket": bucket, "key": key}))

        # docId = filename without .json 
        basename = key.rsplit("/", 1)[-1]
        doc_id = basename[:-5]

        # Get LEAN JSON 
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            data = json.loads(obj["Body"].read().decode("utf-8"))
            etag = obj.get("ETag", "").strip('"')
            size = obj.get("ContentLength", 0)
        except ClientError as e:
            print(json.dumps({"stage": "error_get_object", "error": str(e)}))
            continue

        pages = _extract_pages(data)

        total_pages = len(pages)
        chunks = plan_chunks(pages)
        for idx, c in enumerate(chunks):
            c["index"] = idx  # til chunkId

        # Write manifest
        key_manifest = f"pipeline/manifests/{doc_id}.json"
        manifest = {
            "docId": doc_id,
            "status": "IN_PROGRESS",
            "createdAt": _now_iso(),
            "source": {"bucket": bucket, "key": key, "etag": etag, "size": size},
            "settings": {
                "tokenBudget": TOKEN_BUDGET,
                "overlapLines": OVERLAP_LINES,
                "maxPagesPerChunk": MAX_PAGES_PER_CHUNK,
                "safetyHeadroom": SAFETY_HEADROOM
            },
            "summary": {"pages": total_pages, "chunks": len(chunks)},
            "chunks": [
                {
                    "chunkId": f"{c['index']+1:04d}",
                    "startPage": c["startPage"],
                    "endPage": c["endPage"],
                    "estTokens": c["estTokens"],
                    "status": "PENDING"
                }
                for c in chunks
            ]
        }
        write_manifest(bucket, key_manifest, manifest)
        
        print(json.dumps({"stage": "manifest_written", "manifest_key": key_manifest}))

        # Enqueue 1 message per chunk
        for c in chunks:
            send_chunk_message(c, doc_id, bucket, key_manifest, total_pages)
        print(json.dumps({"stage": "sqs_enqueued", "chunks": len(chunks)}))

    return {"ok": True}
