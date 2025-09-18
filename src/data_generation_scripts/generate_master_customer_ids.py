import uuid
import os
import logging

# --- Logging setup ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

log.info("Starting master customer ID generation script.")

# Configuration
NUM_MASTER_CUSTOMERS = 100_000_000  # 100M IDs
OUTPUT_FILE = os.path.join('.', 'src' , 'raw_data', 'customer_ids2.ndjson')

def generate_and_save_master_ids(num_ids, output_file, chunk_size=1_000_000):
    """
    Generates unique customer IDs and saves them to a file in NDJSON format.
    Each line is a JSON object with a single "customer_id".
    """
    log.info(f"Generating {num_ids:,} unique customer IDs...")

    with open(output_file, 'w') as f:
        for i in range(num_ids):
            f.write(f'{{"customer_id": "{uuid.uuid4()}"}}\n')

            if (i + 1) % chunk_size == 0:
                log.info(f"Generated {i + 1:,} IDs...")

    log.info(f"Successfully saved {num_ids:,} IDs to {output_file}.")

if __name__ == "__main__":
    # Ensure output directory exists
    output_dir = os.path.dirname(OUTPUT_FILE)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        log.info(f"Created output directory: {output_dir}")

    generate_and_save_master_ids(NUM_MASTER_CUSTOMERS, OUTPUT_FILE)
    log.info("Master customer ID generation complete.")
