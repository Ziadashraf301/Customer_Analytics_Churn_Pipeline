import os
import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import logging
import numpy as np

# --------------------
# Logging setup
# --------------------
LOGS_DIR = os.path.join('.', 'src' ,'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
log_file = os.path.join(LOGS_DIR, 'purchase_events.log')

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

fake = Faker()

# --------------------
# Configuration
# --------------------
NUM_EVENTS_TO_GENERATE = None  # None for continuous
EVENT_GENERATION_DELAY = 0.0001
CHUNK_SIZE = 1_000_000
NUM_EVENTS_PER_CHUNK = 10000
PAYMENT_METHODS = ['credit_card', 'paypal', 'bank_transfer', 'apple_pay']
ORDER_STATUSES = ['completed', 'processing', 'shipped', 'cancelled', 'returned']

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'purchase_events'
MASTER_CUSTOMER_IDS_FILE = os.path.join('.', 'src' ,'raw_data', 'customer_ids.ndjson')  # updated path

# Pre-generate product IDs
NUM_FAKE_PRODUCTS = 200
fake_product_ids = [fake.uuid4() for _ in range(NUM_FAKE_PRODUCTS)]

producer = None

# --------------------
# Stream IDs in chunks
# --------------------
def stream_customer_id_chunks(file_name: str, chunk_size: int):
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"File not found: {file_name}")

    chunk = []
    with open(file_name, "r") as f:
        for i, line in enumerate(f, start=1):
            try:
                record = json.loads(line)
                chunk.append(record["customer_id"])
            except Exception:
                continue

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []

            if i % 1_000_000 == 0:
                log.info(f"🔄 Processed {i:,} IDs...")

        if chunk:
            yield chunk

# --------------------
# Event generation
# --------------------
def generate_purchase_event(user_id):
    order_id = fake.uuid4()
    purchase_time = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
    total_amount = round(random.uniform(5.0, 1500.0), 2)
    num_items = random.randint(1, 5)
    products_in_order = random.sample(fake_product_ids, k=num_items)

    return {
        'order_id': order_id,
        'user_id': user_id,
        'purchase_time': purchase_time,
        'total_amount': total_amount,
        'product_ids': products_in_order,
        'payment_method': random.choice(PAYMENT_METHODS),
        'shipping_address': fake.address(),
        'order_status': random.choice(ORDER_STATUSES),
        'currency': 'USD'
    }

# --------------------
# Main generator
# --------------------
def start_event_generation():
    global producer
    event_count = 0

    # Kafka producer connection
    retries = 5
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                api_version=(0, 10, 1)
            )
            log.info(f"Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}")
            break
        except Exception as e:
            log.warning(f"Kafka connection failed, retry {attempt+1}/{retries}: {e}")
            time.sleep(2 ** attempt)
    else:
        log.error("Failed to connect to Kafka after multiple retries. Exiting.")
        return

    log.info(f"🚀 Starting purchase event generation to Kafka topic: {KAFKA_TOPIC}")

    try:
        for chunk in stream_customer_id_chunks(MASTER_CUSTOMER_IDS_FILE, CHUNK_SIZE):
            for _ in range(NUM_EVENTS_PER_CHUNK):
                user_id = random.choice(chunk)
                event = generate_purchase_event(user_id)
                producer.send(KAFKA_TOPIC, event)
                event_count += 1
                log.info(f"Sent purchase event: Order ID={event['order_id']}, User ID={event['user_id']}, Amount=${event['total_amount']:.2f}")

                if NUM_EVENTS_TO_GENERATE is not None and event_count >= NUM_EVENTS_TO_GENERATE:
                    return

                time.sleep(EVENT_GENERATION_DELAY)

    except KeyboardInterrupt:
        log.info("\nPurchase event generation stopped by user.")
    except Exception as e:
        log.error(f"Error during event production: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            log.info("Kafka producer closed.")

if __name__ == "__main__":
    start_event_generation()
