import json
import time
import random
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer
import logging
import os

# --------------------
# Logging setup
# --------------------
LOGS_DIR = os.path.join('.', 'src' ,'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
log_file = os.path.join(LOGS_DIR, 'web_event_stream.log')

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
CHUNK_SIZE = 1_000_000  # number of IDs per chunk
NUM_EVENTS_PER_CHUNK = 10000  # events per chunk

EVENT_PROBABILITIES = {
    'page_view': 0.6,
    'product_view': 0.2,
    'add_to_cart': 0.1,
    'checkout_success': 0.05,
    'login': 0.02,
    'logout': 0.01,
    'search': 0.02,
}
DEVICE_TYPES = ['mobile', 'desktop', 'tablet']

# Kafka config
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9093'
KAFKA_TOPIC = 'website_events'
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
def generate_event(user_id):
    event_timestamp = datetime.utcnow().isoformat(timespec='milliseconds') + 'Z'
    session_id = fake.uuid4()
    device_type = random.choice(DEVICE_TYPES)
    event_type = random.choices(
        list(EVENT_PROBABILITIES.keys()),
        weights=list(EVENT_PROBABILITIES.values()),
        k=1
    )[0]

    page_url = None
    product_id = None
    search_query = None

    if event_type == 'page_view':
        page_url = fake.url()
    elif event_type == 'product_view':
        product_id = random.choice(fake_product_ids)
        page_url = fake.url()
    elif event_type == 'add_to_cart':
        product_id = random.choice(fake_product_ids)
    elif event_type == 'search':
        search_query = fake.word() + " " + fake.word()

    return {
        'user_id': user_id,
        'event_timestamp': event_timestamp,
        'event_type': event_type,
        'page_url': page_url,
        'product_id': product_id,
        'session_id': session_id,
        'device_type': device_type,
        'search_query': search_query
    }

# --------------------
# Main generator
# --------------------
def start_event_generation():
    global producer
    event_count = 0

    # Kafka producer connection
    for attempt in range(5):
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
            log.warning(f"Kafka connection failed, retry {attempt+1}/5: {e}")
            time.sleep(2 ** attempt)
    else:
        log.error("Failed to connect to Kafka after multiple retries. Exiting.")
        return

    log.info(f"🚀 Starting website/app event generation to Kafka topic: {KAFKA_TOPIC}")

    try:
        for chunk in stream_customer_id_chunks(MASTER_CUSTOMER_IDS_FILE, CHUNK_SIZE):
            for _ in range(NUM_EVENTS_PER_CHUNK):
                user_id = random.choice(chunk)
                event = generate_event(user_id)
                producer.send(KAFKA_TOPIC, event)
                event_count += 1
                log.info(f"Sent event: User ID={event['user_id']}, Type={event['event_type']}")

                if NUM_EVENTS_TO_GENERATE is not None and event_count >= NUM_EVENTS_TO_GENERATE:
                    return

                time.sleep(EVENT_GENERATION_DELAY)

    except KeyboardInterrupt:
        log.info("\nEvent generation stopped by user.")
    except Exception as e:
        log.error(f"Error during event production: {e}")
    finally:
        if producer:
            producer.flush()
            producer.close()
            log.info("Kafka producer closed.")

if __name__ == "__main__":
    start_event_generation()
