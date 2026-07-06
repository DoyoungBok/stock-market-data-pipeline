import os
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.getenv("BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("TOPIC", "stock_ticks")
CSV_PATH = os.getenv("CSV_PATH", "data/sample_stock_prices.csv")
PRODUCER_INTERVAL_SECONDS = float(os.getenv("PRODUCER_INTERVAL_SECONDS", "1"))

AWS_REGION = os.getenv("AWS_REGION", "ca-central-1")
S3_BUCKET = os.getenv("S3_BUCKET", "")
S3_PREFIX = os.getenv("S3_PREFIX", "stock_ticks/raw")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
BATCH_SECONDS = int(os.getenv("BATCH_SECONDS", "10"))
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "stock-s3-consumer") 

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")
API_SYMBOLS = [
    symbol.strip()
    for symbol in os.getenv("API_SYMBOLS", "AAPL").split(",")
    if symbol.strip()
]
API_POLL_SECONDS = float(os.getenv("API_POLL_SECONDS", "90"))
