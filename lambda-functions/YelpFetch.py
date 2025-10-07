import json
import requests
import os
import boto3
from models import RestaurantList
from pydantic import ValidationError
from decimal import Decimal
from collections import Counter

YELP_API_KEY = os.environ.get("YELP_API_KEY", "")
if not YELP_API_KEY:
    raise ValueError("YELP_API_KEY environment variable not set")
    exit(1)

TABLE_NAME = os.environ.get("TABLE_NAME", "yelp-restaurants")
cuisine_list = {"chinese", "japanese", "italian", "mexican", "american"}

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(TABLE_NAME)

def write_to_dynamo_db(data):
    """Write validated items to DynamoDB with error handling."""
    try:
        with table.batch_writer() as batch:
            for i, item in enumerate(data):
                try:
                    batch.put_item(Item=item)
                    print(f"[INFO] item number: {i}, Inserted item: {item['business_id']} ({item['name']})")
                except Exception as inner_e:
                    print(f"[ERROR] Failed to insert item {item.get('business_id')}: {inner_e}")
        return True
    except Exception as e:
        print(f"[ERROR] Batch write failed: {e}")
        return False


def validate_and_parse_fetched_data(data):
    """Validate Yelp response JSON and return DynamoDB-ready items."""
    try:
        restaurants = RestaurantList.model_validate_json(data)
        print(f"[INFO] Parsed {len(restaurants.businesses)} businesses")

        items = []
        for biz in restaurants.businesses:
            try:
                item = {
                    "cuisine": next(
                        (c.alias for c in biz.categories if c.alias in cuisine_list),
                        getattr(biz, "queried_cuisine", "other")
                    ),
                    "business_id": biz.id,
                    "name": biz.name,
                    "review_count": biz.review_count or 0,
                    "rating": Decimal(str(biz.rating)) if biz.rating is not None else Decimal("0"),
                    "coordinates": {
                        "latitude": Decimal(str(biz.coordinates.latitude)),
                        "longitude": Decimal(str(biz.coordinates.longitude)),
                    },
                    "price": biz.price or "N/A",
                    "location": biz.location.model_dump(),
                    "zip_code": biz.location.zip_code or "00000",
                    "business_hours": (
                        {
                            "start": biz.business_hours[0].open[0].start,
                            "end": biz.business_hours[0].open[0].end,
                        }
                        if biz.business_hours else {}
                    ),
                }
                print(f"[INFO] Parsed item {item}")
                items.append(item)
            except Exception as biz_e:
                print(f"[ERROR] Failed to parse business {biz.id}: {biz_e}")

        return items

    except ValidationError as e:
        print("[ERROR] Validation failed")
        for err in e.errors():
            print(f"  - {err}")
        raise

def fetch_restaurants(location, term, categories):
    url = "https://api.yelp.com/v3/businesses/search"
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {YELP_API_KEY}",
    }

    all_businesses = []
    MAX_LIMIT_PER_CATEGORY = 200
    for category in categories:
        offset = 0
        while offset < MAX_LIMIT_PER_CATEGORY:
        
            params = {
                "location": location,
                "term": term,
                "categories": category,
                "sort_by": "best_match",
                "limit": 50,
                "offset": offset
            }

            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            businesses = data.get("businesses", [])

            if not businesses:
                break

            for biz in businesses:
                biz["queried_cuisine"] = category  

            all_businesses.extend(businesses)
            offset += len(businesses)
            if offset >= MAX_LIMIT_PER_CATEGORY:
                break

    counts = Counter([biz["queried_cuisine"] for biz in all_businesses])
    print("[INFO] Counts per cuisine:", counts)
    print("[INFO] Total businesses:", sum(counts.values()))
    return json.dumps({"businesses": all_businesses})

def lambda_handler(event, context):
    print("[INFO] Lambda triggered")

    location = "Brooklyn"
    term = "restaurants"
    categories = list(cuisine_list)

    raw_json = fetch_restaurants(location, term, categories)
    safe_data = validate_and_parse_fetched_data(raw_json)

    if not safe_data:
        print("[WARN] No valid items parsed")
        return {"statusCode": 200, "body": "No items written"}

    success = write_to_dynamo_db(safe_data)
    return {
        "statusCode": 200 if success else 500,
        "body": json.dumps({"inserted": len(safe_data), "success": success}),
    }
