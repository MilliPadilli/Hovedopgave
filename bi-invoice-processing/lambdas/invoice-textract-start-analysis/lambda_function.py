import os
import re
import hashlib
import boto3
import urllib.parse
import os.path
import json

REGION = os.environ.get("AWS_REGION", "eu-west-1")
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
TEXTRACT_ROLE_ARN = os.environ["TEXTRACT_ROLE_ARN"]
TEXTRACT = boto3.client("textract", region_name=REGION)


def safe_job_tag_from_key(key: str) -> str:
    """
    Make the S3 key safe for Textract JobTag:
    - use only the filename (no folders)
    - replace spaces and special characters
    - max 64 characters
    - if too long: keep start+end and insert 8-char hash for uniqueness
    """
    base = os.path.basename(key)            # e.g. "P10078... (1).pdf"
    base = base.replace(" ", "_")
    base = re.sub(r"[^A-Za-z0-9_.-]", "_", base)  # replace "(" ")" "&" etc.
    
    if len(base) <= 64:
        return base
    
    h = hashlib.sha1(base.encode("utf-8")).hexdigest()[:8]
    # keep the first 40 chars + hash + last 10 chars → trim to 64 if needed
    tag = f"{base[:40]}_{h}_{base[-10:]}"
    
    return tag[:64]


def lambda_handler(event, context):
    for r in event.get("Records", []):
        bucket = r["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(r["s3"]["object"]["key"])

        # Async Textract (PDF/TIFF). 
        if not key.lower().endswith((".pdf", ".tiff", ".tif")):
            print(f"[Skip] Unsupported file type for async Textract: s3://{bucket}/{key}")
            continue
        base_tag = safe_job_tag_from_key(key)
        print(f"[JobTagBase] {base_tag!r} (len={len(base_tag)}) for key={key!r}")

        # 1) StartDocumentAnalysis 
        try:
            resp_analysis = TEXTRACT.start_document_analysis(
                DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
                FeatureTypes=["FORMS", "TABLES"],
                NotificationChannel={"SNSTopicArn": SNS_TOPIC_ARN, "RoleArn": TEXTRACT_ROLE_ARN},
                JobTag=f"ANALYSIS_{base_tag}"[:64]
            )
            print(f"[Started] StartDocumentAnalysis job_id={resp_analysis['JobId']} for s3://{bucket}/{key}")

        except TEXTRACT.exceptions.InvalidParameterException as e:
            print("[ERROR] StartDocumentAnalysis InvalidParameter full response:",
                  json.dumps(e.response, default=str))
            raise

        # 2) StartDocumentTextDetection
        try:
            resp_text = TEXTRACT.start_document_text_detection(
                DocumentLocation={"S3Object": {"Bucket": bucket, "Name": key}},
                NotificationChannel={"SNSTopicArn": SNS_TOPIC_ARN, "RoleArn": TEXTRACT_ROLE_ARN},
                JobTag=f"TEXT_{base_tag}"[:64]
            )

            print(f"[Started] StartDocumentTextDetection job_id={resp_text['JobId']} for s3://{bucket}/{key}")

        except TEXTRACT.exceptions.InvalidParameterException as e:

            print("[ERROR] StartDocumentTextDetection InvalidParameter full response:",
                  json.dumps(e.response, default=str))
            raise
    return {"status": "ok"}
