import json
import uuid
import random
import time
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer

fake = Faker()

# ── Confluent Cloud config ─────────────────────────────────────
BOOTSTRAP_SERVER = "your-confluent-bootstrap-server:9092"   
API_KEY          = "your-cluster-scoped-api-key"
API_SECRET       = "your-cluster-api-secret"
TOPIC            = "bank_transactions"

conf = {
    "bootstrap.servers": BOOTSTRAP_SERVER,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms":   "PLAIN",
    "sasl.username":     API_KEY,
    "sasl.password":     API_SECRET,
}

producer = Producer(conf)

# ── Static reference data ──────────────────────────────────────
COUNTRIES  = ["US", "GB", "DE", "FR", "IN", "CA", "AU", "JP", "BR", "MX"]
STATUSES   = ["success", "failed", "pending", "flagged"]
MERCHANTS  = ["Amazon", "Walmart", "Target", "Best Buy", "Starbucks",
              "McDonald's", "Apple Store", "Netflix", "Uber", "Airbnb"]
ACCOUNT_IDS = [f"ACC{str(i).zfill(6)}" for i in range(1, 201)]  # 200 fake accounts

# ── Event generators ───────────────────────────────────────────
def purchase_event():
    return {
        "event_id":   str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNT_IDS),
        "event_type": "purchase_event",
        "amount":     round(random.uniform(5.0, 4500.0), 2),
        "merchant":   random.choice(MERCHANTS),
        "country":    random.choice(COUNTRIES),
        "city":       fake.city(),
        "device_id":  fake.md5()[:16],
        "ip_address": fake.ipv4(),
        "status":     random.choices(STATUSES, weights=[70, 15, 10, 5])[0],
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

def atm_withdrawal():
    return {
        "event_id":   str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNT_IDS),
        "event_type": "atm_withdrawal",
        "amount":     round(random.uniform(20.0, 1000.0), 2),
        "merchant":   f"ATM-{fake.bothify('??###')}",
        "country":    random.choice(COUNTRIES),
        "city":       fake.city(),
        "device_id":  fake.md5()[:16],
        "ip_address": fake.ipv4(),
        "status":     random.choices(STATUSES, weights=[75, 15, 5, 5])[0],
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

def online_transfer():
    amount = round(random.uniform(100.0, 15000.0), 2)
    return {
        "event_id":   str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNT_IDS),
        "event_type": "online_transfer",
        "amount":     amount,
        "merchant":   f"TRANSFER-TO-{fake.bothify('ACC######')}",
        "country":    random.choice(COUNTRIES),
        "city":       fake.city(),
        "device_id":  fake.md5()[:16],
        "ip_address": fake.ipv4(),
        "status":     random.choices(STATUSES, weights=[65, 10, 15, 10])[0],
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

def login_attempt():
    return {
        "event_id":   str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNT_IDS),
        "event_type": "login_attempt",
        "amount":     0.0,
        "merchant":   "AUTH_SERVICE",
        "country":    random.choice(COUNTRIES),
        "city":       fake.city(),
        "device_id":  fake.md5()[:16],
        "ip_address": fake.ipv4(),
        "status":     random.choices(STATUSES, weights=[60, 35, 0, 5])[0],
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

def account_update():
    return {
        "event_id":   str(uuid.uuid4()),
        "account_id": random.choice(ACCOUNT_IDS),
        "event_type": "account_update",
        "amount":     0.0,
        "merchant":   "ACCOUNT_SERVICE",
        "country":    random.choice(COUNTRIES),
        "city":       fake.city(),
        "device_id":  fake.md5()[:16],
        "ip_address": fake.ipv4(),
        "status":     random.choices(STATUSES, weights=[80, 10, 5, 5])[0],
        "event_time": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
    }

EVENT_GENERATORS = [
    purchase_event,
    atm_withdrawal,
    online_transfer,
    login_attempt,
    account_update,
]

# ── Delivery callback ──────────────────────────────────────────
def delivery_report(err, msg):
    if err:
        print(f"  ✗ Delivery failed: {err}")

# ── Main loop ──────────────────────────────────────────────────
def main():
    print("=" * 55)
    print("  Fraud Detection — Kafka Producer")
    print("  Topic : bank_transactions")
    print("  Events: purchase, atm, transfer, login, update")
    print("=" * 55)
    print("  Starting stream... Press Ctrl+C to stop.\n")

    count = 0
    start = time.time()

    try:
        while True:
            # Pick a random event generator and produce
            event = random.choice(EVENT_GENERATORS)()
            producer.produce(
                topic=TOPIC,
                key=event["account_id"],
                value=json.dumps(event),
                callback=delivery_report,
            )
            producer.poll(0)
            count += 1

            # Print progress every 500 events
            if count % 500 == 0:
                elapsed = time.time() - start
                rate    = count / elapsed
                print(f"  [{datetime.now().strftime('%H:%M:%S')}]  "
                      f"Events sent: {count:,}  |  Rate: {rate:.0f}/sec")

            # Throttle to ~500 events/sec
            time.sleep(0.002)

    except KeyboardInterrupt:
        print(f"\n  Stopped. Total events sent: {count:,}")
        producer.flush()

if __name__ == "__main__":
    main()
