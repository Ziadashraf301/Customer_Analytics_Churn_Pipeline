import os
import ujson as json
import logging
from typing import Iterable
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

# --------------------
# Logging
# --------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
log.info("🚀 Starting ultra-fast batch customer data generation.")

# --------------------
# Config
# --------------------
# Updated paths
MASTER_CUSTOMER_IDS_FILE = os.path.join('.', 'src' ,'raw_data', 'customer_ids.ndjson')
OUTPUT_PATH = os.path.join('.', 'src' , 'raw_data', 'customer_profiles.parquet')

NUM_CUSTOMERS_TO_PROFILE = 100_000_000
CHUNK_SIZE = 1_000_000
RNG_SEED = 42

SUBSCRIPTION_TIERS = np.array(["free", "basic", "premium"])
ACQUISITION_CHANNELS = np.array(["social", "search", "referral", "email", "ads"])
COUNTRY_CODES = np.array([
    "US","GB","DE","FR","ES","IT","NL","SE","NO","DK","CH","PL","RO","PT","GR","IE","BE","AT",
    "CA","BR","MX","AR","CL","CO","PE","AU","NZ","JP","KR","CN","IN","SG","MY","TH","PH","VN",
    "ZA","EG","MA","NG","KE","AE","SA","TR","IL","RU","UA"
])

rng = np.random.default_rng(RNG_SEED)

# --------------------
# Stream IDs
# --------------------
def stream_customer_ids(file_name: str) -> Iterable[str]:
    """Stream customer IDs from NDJSON file using ujson (fast)."""
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"❌ Missing file: {file_name}")

    with open(file_name, "r") as f:
        for i, line in enumerate(f, start=1):
            try:
                record = json.loads(line)
                yield record["customer_id"]
            except Exception:
                continue
            if i % 1_000_000 == 0:
                log.info(f"🔄 Processed {i:,} IDs...")

# --------------------
# Column generators
# --------------------
def generate_dates(size: int, days_back: int = 730) -> np.ndarray:
    offsets = rng.integers(0, days_back + 1, size=size, dtype=np.int32)
    base = np.datetime64("today") - np.timedelta64(days_back, "D")
    return base + offsets.astype("timedelta64[D]")

def to_emails_from_ids(user_ids: np.ndarray) -> np.ndarray:
    return np.char.add(user_ids.astype(str), "@example.com")

# --------------------
# Main generator
# --------------------
def generate_customer_profiles_streaming(
    input_file: str,
    output_file: str,
    num_profiles: int,
    chunk_size: int = 1_000_000,
    compression: str = "snappy"
):
    log.info(f"🛠 Generating {num_profiles:,} profiles in chunks of {chunk_size:,} -> {output_file}")

    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        log.info(f"Created output directory: {output_dir}")

    writer = None
    rows_written = 0
    id_iter = stream_customer_ids(input_file)

    while rows_written < num_profiles:
        ids_chunk = [next(id_iter, None) for _ in range(chunk_size)]
        ids_chunk = [i for i in ids_chunk if i is not None]
        if not ids_chunk:
            break

        size = len(ids_chunk)
        user_ids = np.array(ids_chunk, dtype="U36")

        reg_dates = generate_dates(size)
        sub_tier = rng.choice(SUBSCRIPTION_TIERS, size=size)
        acq_chan = rng.choice(ACQUISITION_CHANNELS, size=size)
        countries = rng.choice(COUNTRY_CODES, size=size)
        emails = to_emails_from_ids(user_ids)

        table = pa.table({
            "user_id": user_ids,
            "registration_date": reg_dates,
            "subscription_tier": sub_tier,
            "acquisition_channel": acq_chan,
            "email": emails,
            "country": countries
        })

        if writer is None:
            writer = pq.ParquetWriter(output_file, table.schema, compression=compression)
        writer.write_table(table)

        rows_written += size
        if rows_written % (5 * chunk_size) == 0 or rows_written >= num_profiles:
            log.info(f"📊 Progress: {rows_written:,}/{num_profiles:,} rows written")

    if writer:
        writer.close()

    log.info(f"✅ Done. Wrote {rows_written:,} rows -> {output_file} (compression={compression})")

# --------------------
# Entry
# --------------------
if __name__ == "__main__":
    try:
        generate_customer_profiles_streaming(
            input_file=MASTER_CUSTOMER_IDS_FILE,
            output_file=OUTPUT_PATH,
            num_profiles=NUM_CUSTOMERS_TO_PROFILE,
            chunk_size=CHUNK_SIZE,
            compression="snappy"
        )
        log.info("🎉 Generation completed successfully.")
    except Exception as e:
        log.exception(f"❌ Generation failed: {e}")
