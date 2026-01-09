import json
import os
import boto3
from datetime import datetime

TEXTRACT = boto3.client("textract")
S3 = boto3.client("s3")

ANALYSES_BUCKET = os.environ["ANALYSES_BUCKET"]
RESULT_BUCKET   = os.environ["RESULT_BUCKET"]
TEXT_PREFIX     = os.environ.get("TEXT_PREFIX", "text/")
ANALYSIS_PREFIX = os.environ.get("ANALYSIS_PREFIX", "analysis/")

def _get_textract_analysis(job_id: str):
    """Fetch all pages from GetDocumentAnalysis (handles pagination)."""
    pages = []
    next_token = None
    while True:
        kwargs = {"JobId": job_id}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = TEXTRACT.get_document_analysis(**kwargs)
        pages.append(resp)
        next_token = resp.get("NextToken")
        if not next_token:
            break
    return pages

def _get_textract_text(job_id: str):
    """Fetch all pages from GetDocumentTextDetection (handles pagination)."""
    pages = []
    next_token = None
    while True:
        kwargs = {"JobId": job_id}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = TEXTRACT.get_document_text_detection(**kwargs)

        # Collect LINE blocks per page
        page_map = {}
        for b in resp.get("Blocks", []):
            if b.get("BlockType") == "LINE":
                p = b.get("Page", 1)
                page_map.setdefault(p, []).append(b.get("Text", ""))

        for p in sorted(page_map.keys()):
            text = "\n".join(page_map[p]).strip()
            if text:
                pages.append({"page": p, "text": text})

        next_token = resp.get("NextToken")
        if not next_token:
            break

    full_text = "\n\n".join([f"— Page {o['page']} —\n{o['text']}" for o in pages])
    return pages, full_text

def _put_s3(bucket, key, body, content_type="application/json"):
    S3.put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)

def lambda_handler(event, context):
    # SQS -> SNS envelope -> Textract message
    for r in event.get("Records", []):
        try:
            body = json.loads(r["body"])
            message = json.loads(body["Message"])
            job_id = message["JobId"]
            status = message["Status"]
            api = message.get("API") or message.get("Api")
            job_tag = message.get("JobTag", job_id)

            print(f"Textract job {job_id} status={status}, api={api}, tag={job_tag}")

            if status != "SUCCEEDED":
                print(f"Job {job_id} not SUCCEEDED -> {status}")
                continue

            # Case 1: DocumentAnalysis
            if api == "StartDocumentAnalysis":
                pages = _get_textract_analysis(job_id)
                raw_key = f"pipeline/textract/raw/{job_id}.json"
                _put_s3(ANALYSES_BUCKET, raw_key, json.dumps(pages).encode("utf-8"))

                total_blocks = sum(len(p.get("Blocks", [])) for p in pages)
                summary = {
                    "job_id": job_id,
                    "job_tag": job_tag,
                    "pages": len(pages),
                    "total_blocks": total_blocks,
                    "saved_raw_to": f"s3://{ANALYSES_BUCKET}/{raw_key}",
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
                summary_key = f"{ANALYSIS_PREFIX.rstrip('/')}/{job_tag}.summary.json"
                _put_s3(RESULT_BUCKET, summary_key, json.dumps(summary, indent=2).encode("utf-8"))
                print(f"Saved analysis summary -> s3://{RESULT_BUCKET}/{summary_key}")

            # Case 2: DocumentTextDetection (all text)
            elif api == "StartDocumentTextDetection":
                pages, full_text = _get_textract_text(job_id)

                summary = {
                    "job_id": job_id,
                    "job_tag": job_tag,
                    "page_count": len(pages),
                    "text": {"pages": pages, "full": full_text},
                    "timestamp": datetime.utcnow().isoformat() + "Z"
                }
                summary_key = f"{TEXT_PREFIX.rstrip('/')}/{job_tag}.summary.json"
                _put_s3(RESULT_BUCKET, summary_key, json.dumps(summary, indent=2, ensure_ascii=False).encode("utf-8"))
                print(f"Saved text summary -> s3://{RESULT_BUCKET}/{summary_key}")

        except Exception as e:
            print("ERROR processing record:", e)
            continue

    return {"status": "ok"}
