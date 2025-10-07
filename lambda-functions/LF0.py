import json
import os
import uuid
import datetime
import boto3
from botocore.exceptions import BotoCoreError, ClientError

REGION = os.getenv("LEX_REGION", os.environ.get("AWS_REGION", "us-east-1"))
LEX_BOT_ID       = os.environ.get("LEX_BOT_ID")
LEX_BOT_ALIAS_ID = os.environ.get("LEX_BOT_ALIAS_ID")
LEX_LOCALE_ID    = os.environ.get("LEX_LOCALE_ID", "en_US")

lex = boto3.client("lexv2-runtime",region_name=REGION)

def _bad_request(msg, code=403):
    return {
        "statusCode": code,
        "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,POST",
                },
        "body": json.dumps(msg),
    }


def lambda_handler(event, context):

    path = event.get("path")
    method = event.get("httpMethod")

    if path != "/chatbot" or method != "POST":
        return _bad_request("Invalid path or method")

    if not event.get("body"):
        return _bad_request("No body found")

    try:
        body = json.loads(event["body"])
    except json.JSONDecodeError:
        return _bad_request("Body is not valid JSON")

    messages = body.get("messages", [])
    if not messages:
        return _bad_request("No messages found")

    unstructured = messages[0].get("unstructured", {})
    text = unstructured.get("text", "")
    if not text:
        return _bad_request("No text found")

    session_id = body.get("sessionId") or str(uuid.uuid4())

    try:
        lex_resp = lex.recognize_text(
            botId=LEX_BOT_ID,
            botAliasId=LEX_BOT_ALIAS_ID,
            localeId=LEX_LOCALE_ID,
            sessionId=session_id,
            text=text,
        )

        print(f"Lex response: {lex_resp}")

        out_text_parts = []
        for m in lex_resp.get("messages", []):
            content = m.get("content")
            if content:
                out_text_parts.append(content)

        out_text = " ".join(out_text_parts).strip() or "Sorry, I didn't catch that."

        resp = [
            {
                "type": "unstructured",
                "unstructured": {
                    "id": "1",
                    "text": out_text,
                    "timestamp": datetime.datetime.now().isoformat(),
                },
            }
        ]

        return {
            "statusCode": 200,
            "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type",
                    "Access-Control-Allow-Methods": "OPTIONS,POST",
                },
            "body": json.dumps({"messages": resp, "sessionId": session_id}),
        }

    except (BotoCoreError, ClientError) as e:
        print(f"Lex call failed: {e}")
        return _bad_request("Failed to contact Lex", code=500)
