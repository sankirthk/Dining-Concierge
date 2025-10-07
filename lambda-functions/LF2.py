import json
import os
import html
import boto3
from typing import List
from boto3.dynamodb.conditions import Key
from decimal import Decimal
from botocore.exceptions import ClientError
from requests_aws4auth import AWS4Auth
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

# aoss config
region = 'us-east-1'
service = 'aoss'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)
client = OpenSearch(
    hosts=[{"host": os.getenv("AOSS_HOST"), "port": 443}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=True,
    connection_class=RequestsHttpConnection,
)
AOSS_INDEX = os.getenv("AOSS_INDEX", "restaurant_index")



# DynamoDB config
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.getenv("TABLE_NAME", "yelp-restaurants")
DEFAULT_LIMIT = int(os.getenv("DEFAULT_LIMIT", "10"))
SENDER = os.getenv("EMAIL_SENDER", "concierge.service.cc@gmail.com")
table = dynamodb.Table(TABLE_NAME)
CUISINE_INDEX = os.getenv("CUISINE_INDEX", "rating-index")

def lambda_handler(event, context):
    body = event[0].get("body")
    msg_to_be_processed = json.loads(body)
    # {
    #   "Location":"New York",
    #   "Cuisine":"italian",
    #   "DiningTime":"19:00",
    #   "NumPeople":"5",
    #   "Email":"abc@abc.com"
    # }

    q_location = msg_to_be_processed.get("Location")
    cuisine = msg_to_be_processed.get("Cuisine")
    dining_time = msg_to_be_processed.get("DiningTime")
    num_people = msg_to_be_processed.get("NumPeople")
    email = msg_to_be_processed.get("Email")

    restaurant_ids = aoss_query(cuisine, DEFAULT_LIMIT)
    restaurants = query_top_by_cuisine(cuisine, restaurant_ids, DEFAULT_LIMIT, 0.0)

    if dining_time:
        restaurants = filter_by_dining_time(restaurants, dining_time)


    print(f"Found {len(restaurants)} restaurants for cuisine={cuisine} at time={dining_time}")

    msg_id = send_restaurant_recommendations_email(
    sender=SENDER,
    recipients=[email],
    subject="Your dining suggestions",
    intro_text="Here are the best-rated options that match your request:",
    restaurants=restaurants,  # list from your DynamoDB + time filter
    ses_region="us-east-1",)
    print("SES MessageId:", msg_id)


    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "count": len(restaurants),
                "sample": restaurants[:3],
            },
            default=str,
        ),
    }

# def query_top_by_cuisine(cuisine: str, restaurant_ids: List[str], limit: int, min_rating: float | None):
#     cuisine = html.escape(cuisine.lower())
#     key_cond = Key("cuisine").eq(cuisine) & Key("business_id").is_in(restaurant_ids)
#     if min_rating is not None:
#         key_cond = key_cond & Key("rating").gte(to_decimal(min_rating))

#     kwargs = {
#         "IndexName": CUISINE_INDEX,
#         "KeyConditionExpression": key_cond,
#         "ProjectionExpression": "#id, #n, address, rating, cuisine, business_hours, #l, zip_code, price, review_count, coordinates",
#         "ExpressionAttributeNames": {
#             "#id": "business_id",
#             "#n": "name",
#             "#l": "location",
#         },
#         "ScanIndexForward": False,
#         "Limit": limit,
#     }

#     resp = table.query(**kwargs)
#     return resp.get("Items", [])

def query_top_by_cuisine(cuisine: str, restaurant_ids: List[str], limit: int, min_rating: float | None):
    if not restaurant_ids:
        return []

    cuisine = html.escape(cuisine.lower())
    keys = [{"cuisine": {"S": cuisine}, "business_id": {"S": rid}} for rid in restaurant_ids[:10]]
    resp = client.batch_get_item(RequestItems={TABLE_NAME: {"Keys": keys}})

    items = resp.get("Responses", {}).get(TABLE_NAME, [])
    restaurants = [{k: val_of(v) for k, v in item.items()} for item in items]

    if min_rating is not None:
        restaurants = [r for r in restaurants if float(r.get("rating", 0)) >= min_rating]

    restaurants.sort(key=lambda r: float(r.get("rating", 0)), reverse=True)

    return restaurants[:limit]
def aoss_query(cuisine: str, limit: int):
    cuisine = html.escape(cuisine.lower())
    query = {
        "size": limit,
        "query": {
            "query_string": {
                "default_field": "cuisine",
                "query": cuisine,
            }
        },
    }
    resp = client.search(index=AOSS_INDEX, body=query)
    return [hit["_source"] for hit in resp["hits"]["hits"]]

    
def filter_by_dining_time(items: list[dict], dining_time: str) -> list[dict]:
    target = _hhmm_to_minutes(dining_time)

    def open_now(hours_obj) -> bool:
        if not hours_obj:
            return False

        windows = []
        if isinstance(hours_obj, dict) and "start" in hours_obj and "end" in hours_obj:
            windows = [hours_obj]
        elif isinstance(hours_obj, (list, tuple)):
            windows = hours_obj
        else:
            return False

        for w in windows:
            start_raw = extract_hhmm(w.get("start"))
            end_raw = extract_hhmm(w.get("end"))
            if not start_raw or not end_raw:
                continue
            start = _hhmm_to_minutes(start_raw)
            end = _hhmm_to_minutes(end_raw)

            if start == end:
                return True

            if start < end:
                if start <= target < end:
                    return True
            else:
                if target >= start or target < end:
                    return True
        return False

    return [it for it in items if open_now(it.get("business_hours"))]


def extract_hhmm(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, dict) and "S" in val:
        return val["S"]
    return str(val)


def _hhmm_to_minutes(hhmm: str) -> int:
    s = hhmm.strip()
    if ":" in s:
        h, m = s.split(":")
    else:
        h, m = s[:2], s[2:]
    return int(h) * 60 + int(m)


def to_decimal(x):
    if x is None:
        return None
    if isinstance(x, Decimal):
        return x
    return Decimal(str(x))


def send_restaurant_recommendations_email(
    *,
    sender: str,
    recipients: list[str],
    subject: str,
    intro_text: str,
    restaurants: list[dict],
    ses_region: str | None = None,
    reply_to: list[str] | None = None,
) -> str:
    """
    Sends a formatted email via Amazon SES:
      - intro_text line
      - table of Name | Address | Rating | Business Hours
    Returns SES MessageId.
    """
    ses = boto3.client("ses", region_name=ses_region or os.getenv("SES_REGION") or "us-east-1")

    html_body = build_html_email(intro_text, restaurants)
    text_body = build_text_email(intro_text, restaurants)

    try:
        resp = ses.send_email(
            Source=sender,
            Destination={"ToAddresses": recipients},
            Message={
                "Subject": {"Data": subject, "Charset": "UTF-8"},
                "Body": {
                    "Text": {"Data": text_body, "Charset": "UTF-8"},
                    "Html": {"Data": html_body, "Charset": "UTF-8"},
                },
            },
            ReplyToAddresses=reply_to or [],
        )
        return resp["MessageId"]
    except ClientError as e:
        raise RuntimeError(f"SES send_email failed: {e.response['Error']['Message']}")

# ========== Email body builders =============================================

def build_html_email(intro_text: str, restaurants: list[dict]) -> str:
    table_html = build_restaurants_html(restaurants)
    intro_html = f"<p style='margin:0 0 12px 0;font:14px Arial,Helvetica,sans-serif'>{html.escape(intro_text)}</p>"
    return f"""\
<!doctype html>
<html>
  <body style="margin:0;padding:16px;background:#ffffff">
    <div style="max-width:720px;margin:auto">
      {intro_html}
      {table_html}
      <p style="color:#6b7280;font:12px Arial,Helvetica,sans-serif;margin-top:12px">Sent via Amazon SES</p>
    </div>
  </body>
</html>""".strip()

def build_text_email(intro_text: str, restaurants: list[dict]) -> str:
    return f"""{intro_text}

{build_restaurants_text(restaurants)}

Sent via Amazon SES""".strip()

# ========== Table/row builders ==============================================

def build_restaurants_html(restaurants: list[dict]) -> str:
    rows = []
    for r in restaurants:
        name = html.escape(str(val_of(r.get("name")) or "N/A"))
        address = html.escape(build_address(r))
        rating = html.escape(format_rating(r.get("rating")))
        _, hours_html = format_business_hours(r.get("business_hours"))

        rows.append(f"""\
<tr>
  <td style="padding:8px;border:1px solid #e5e7eb">{name}</td>
  <td style="padding:8px;border:1px solid #e5e7eb;white-space:pre-line">{address}</td>
  <td style="padding:8px;border:1px solid #e5e7eb;text-align:center">{rating}</td>
  <td style="padding:8px;border:1px solid #e5e7eb">{hours_html}</td>
</tr>""")

    return f"""\
<table cellpadding="0" cellspacing="0" width="100%" style="border-collapse:collapse;font:14px Arial,Helvetica,sans-serif">
  <thead>
    <tr style="background:#f9fafb">
      <th align="left"   style="padding:10px;border:1px solid #e5e7eb">Name</th>
      <th align="left"   style="padding:10px;border:1px solid #e5e7eb">Address</th>
      <th align="center" style="padding:10px;border:1px solid #e5e7eb">Rating</th>
      <th align="left"   style="padding:10px;border:1px solid #e5e7eb">Business Hours</th>
    </tr>
  </thead>
  <tbody>
    {''.join(rows)}
  </tbody>
</table>""".strip()

def build_restaurants_text(restaurants: list[dict]) -> str:
    lines = []
    for r in restaurants:
        name = str(val_of(r.get("name")) or "N/A")
        address = build_address(r)
        rating = format_rating(r.get("rating"))
        hours_text, _ = format_business_hours(r.get("business_hours"))
        lines.append(
            f"Name: {name}\n"
            f"Address: {address}\n"
            f"Rating: {rating}\n"
            f"Hours: {hours_text}\n"
            + ("-" * 40)
        )
    return "\n".join(lines)

def val_of(x):
    """Return plain Python value from DynamoDB attribute or raw value."""
    if isinstance(x, dict) and len(x) == 1 and next(iter(x)) in {"S", "N", "BOOL"}:
        t, v = next(iter(x.items()))
        if t == "N":
            try:
                return Decimal(v)
            except Exception:
                return v
        return v
    return x

def get_attr(d: dict, *path, default=None):
    """Safe nested getter supporting DynamoDB-export shapes."""
    cur = d
    for p in path:
        if cur is None:
            return default
        cur = cur.get(p)
    return val_of(cur) if cur is not None else default

def build_address(r: dict) -> str:
    # Use 'address' if already flattened; else compose from location + zip
    addr = val_of(r.get("address"))
    if addr:
        return str(addr)

    a1 = get_attr(r, "location", "address1") or ""
    a2 = get_attr(r, "location", "address2") or ""
    city = get_attr(r, "location", "city") or ""
    state = get_attr(r, "location", "state") or ""
    zipc = val_of(r.get("zip_code")) or get_attr(r, "location", "zip_code") or ""

    parts = [p for p in [a1, a2] if p]
    citystate = ", ".join(p for p in [city, state] if p)
    if citystate:
        parts.append(citystate)
    if zipc:
        parts.append(str(zipc))
    return ", ".join(parts) or "N/A"

def format_rating(r) -> str:
    r = val_of(r)
    if isinstance(r, Decimal):
        return f"{float(r):.1f}"
    try:
        return f"{float(r):.1f}"
    except Exception:
        return str(r) if r is not None else "N/A"

def extract_hhmm(val) -> str | None:
    if val is None:
        return None
    if isinstance(val, dict) and "S" in val:  # export-style {"S":"0700"}
        val = val["S"]
    s = str(val).strip()
    return s if s else None

def format_hhmm(s: str) -> str:
    s = str(s).strip()
    if ":" in s:
        hh, mm = s.split(":")
    else:
        hh, mm = s[:2], s[2:4] if len(s) >= 4 else "00"
    return f"{int(hh):02d}:{int(mm):02d}"

def format_business_hours(hours):
    """
    Accepts:
      • dict: {"start": "0700", "end": "2300"}   (or {"start":{"S":"0700"}})
      • list of such dicts (multiple windows)
    Returns (text_version, html_version) like '07:00–23:00'. start==end → 'Open 24 hours'.
    """
    if not hours:
        return ("Not provided", "Not provided")

    def window_to_str(win: dict) -> str | None:
        s_raw = extract_hhmm(win.get("start"))
        e_raw = extract_hhmm(win.get("end"))
        if not s_raw or not e_raw:
            return None
        if s_raw == e_raw:
            return "Open 24 hours"
        return f"{format_hhmm(s_raw)}–{format_hhmm(e_raw)}"

    windows = []
    if isinstance(hours, dict) and ("start" in hours or "end" in hours):
        windows = [hours]
    elif isinstance(hours, (list, tuple)):
        windows = [w for w in hours if isinstance(w, dict)]

    out = [w for w in (window_to_str(w) for w in windows) if w]
    if not out:
        return ("Not provided", "Not provided")

    text = "\n".join(out)
    html_lines = "<br>".join(html.escape(line) for line in out)
    return (text, html_lines)