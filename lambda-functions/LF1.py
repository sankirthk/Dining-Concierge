import json, os, re, boto3
sqs = boto3.client('sqs')
QUEUE_URL = os.environ['QUEUE_URL']

CUISINES = {"chinese","japanese","italian","mexican","american"}

def close(intent, message):
    return {
        "sessionState": {
            "dialogAction": {"type": "Close"},
            "intent": {**intent, "state": "Fulfilled"}
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }

def elicit(slot, intent, message):
    return {
        "sessionState": {
            "dialogAction": {"type": "ElicitSlot", "slotToElicit": slot},
            "intent": intent
        },
        "messages": [{"contentType": "PlainText", "content": message}]
    }

def handle_dining(intent):
    slots = intent.get("slots") or {}
    get = lambda k: (slots.get(k) or {}).get("value", {}).get("interpretedValue")

    loc = get("Location")
    cui = (get("Cuisine") or "").lower()
    time = get("DiningTime")
    num = get("NumPeople")
    email = get("Email")

    if not loc: return elicit("Location", intent, "What city or area are you looking to dine in?")
    if cui not in CUISINES: return elicit("Cuisine", intent, f"Which cuisine? Try {', '.join(CUISINES)}")
    if not num or not num.isdigit(): return elicit("NumPeople", intent, "How many people are in your party?")
    if not time or not re.fullmatch(r"\d{2}:\d{2}", time): return elicit("DiningTime", intent, "What time? (HH:MM)")
    if not email or "@" not in email: return elicit("Email", intent, "What email should I send results to?")

    payload = {"Location": loc, "Cuisine": cui, "DiningTime": time, "NumPeople": num, "Email": email}
    sqs.send_message(QueueUrl=QUEUE_URL, MessageBody=json.dumps(payload))

    return close(intent, f"Got it! I’ll email {email} some {cui.title()} options in {loc} for {num} people at {time}.")

def lambda_handler(event, context):
    intent = event["sessionState"]["intent"]
    name = intent["name"]

    if name == "GreetingIntent":
        return close(intent, "Hi there, how can I help?")
    elif name == "ThankYouIntent":
        return close(intent, "You’re welcome!")
    elif name == "DiningSuggestionsIntent":
        return handle_dining(intent)
    else:
        return close(intent, "Sorry, I didn’t get that.")


