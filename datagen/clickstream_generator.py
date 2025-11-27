import time
import uuid
import json
import random
import os
from datetime import datetime
from kafka import KafkaProducer

# -----------------------------
# Environment Configuration
# -----------------------------
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "broker:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "clickstream")
CLIENT_ID = os.getenv("CLIENT_ID", "clickstream-generator-1")

NORMAL_MIN_DELAY = float(os.getenv("NORMAL_MIN_DELAY", 1))
NORMAL_MAX_DELAY = float(os.getenv("NORMAL_MAX_DELAY", 3))

FRAUD_CHANCE = float(os.getenv("FRAUD_CHANCE", 0.30))
USER_POOL_SIZE = int(os.getenv("USER_POOL_SIZE", 50))
PRODUCT_POOL_SIZE = int(os.getenv("PRODUCT_POOL_SIZE", 500))

USER_POOL = [f"user_{i}" for i in range(USER_POOL_SIZE)]
FRAUD_USERS = set(random.sample(USER_POOL, max(1, USER_POOL_SIZE // 7)))
PRODUCT_POOL = [f"prod_{i}" for i in range(PRODUCT_POOL_SIZE)]

# -----------------------------
# Kafka Producer
# -----------------------------
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    client_id=CLIENT_ID,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=5,
)

# -----------------------------
# Utility
# -----------------------------
def now_ts():
    return datetime.utcnow().isoformat() + "Z"

def emit(event):
    print(event)
    producer.send(KAFKA_TOPIC, event)

# -----------------------------
# Event Generators
# -----------------------------
def generate_normal_event(user_id):
    """Single random non-fraud event: only view or scroll."""
    event = {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "product_id": random.choice(PRODUCT_POOL),
        "action": random.choice(["view", "scroll"]),
        "timestamp": now_ts()
    }
    emit(event)


def generate_normal_flow(user_id):
    """A legitimate full purchase flow with natural delays."""
    product = random.choice(PRODUCT_POOL)
    steps = [
        ("view", 1, 2),
        ("scroll", 2, 5),
        ("add_to_cart", 0, 2),
        ("purchase", 1, 5)
    ]
    for action, dmin, dmax in steps:
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "product_id": product,
            "action": action,
            "timestamp": now_ts()
        }
        emit(event)
        if dmax > 0:
            time.sleep(random.uniform(dmin, dmax))


def generate_fraud_flow(user_id):
    """Suspiciously fast flow: entire sequence in < 1 second."""
    product = random.choice(PRODUCT_POOL)
    actions = ["view", "scroll", "add_to_cart", "purchase"]

    for action in actions:
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": user_id,
            "product_id": product,
            "action": action,
            "timestamp": now_ts()
        }
        emit(event)
        time.sleep(random.uniform(0.05, 0.20))  # very fast


# -----------------------------
# Main loop
# -----------------------------
def main():
    print("Starting Clickstream Generator")
    print("Kafka broker:", KAFKA_BROKER)
    print("Kafka topic:", KAFKA_TOPIC)
    print("Client ID:", CLIENT_ID)
    print("Fraud Users:", FRAUD_USERS)

    while True:
        user_id = random.choice(USER_POOL)

        # Fraud sequence for fraud users
        if user_id in FRAUD_USERS and random.random() < FRAUD_CHANCE:
            generate_fraud_flow(user_id)

        # Legit full sequence (30% chance)
        elif random.random() < 0.3:
            generate_normal_flow(user_id)

        # Otherwise just single lightweight events
        else:
            generate_normal_event(user_id)

        time.sleep(random.uniform(NORMAL_MIN_DELAY, NORMAL_MAX_DELAY))


if __name__ == "__main__":
    main()
