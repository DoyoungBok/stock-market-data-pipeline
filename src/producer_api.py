import json
import time
from datetime import datetime, timezone

import requests
from confluent_kafka import Producer

from config import (
    BOOTSTRAP_SERVERS,
    TOPIC,
    ALPHA_VANTAGE_API_KEY,
    API_SYMBOLS,
    API_POLL_SECONDS,
)


ALPHA_VANTAGE_URL = "https://www.alphavantage.co/query"


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Produced API event to topic={msg.topic()}, "
            f"partition={msg.partition()}, offset={msg.offset()}"
        )


def fetch_global_quote(symbol):
    if not ALPHA_VANTAGE_API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY is empty. Check your .env file.")

    params = {
        "function": "GLOBAL_QUOTE",
        "symbol": symbol,
        "apikey": ALPHA_VANTAGE_API_KEY,
    }

    response = requests.get(ALPHA_VANTAGE_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()

    if "Note" in data:
        raise RuntimeError(f"Alpha Vantage rate limit message: {data['Note']}")

    if "Information" in data:
        raise RuntimeError(f"Alpha Vantage information message: {data['Information']}")

    if "Error Message" in data:
        raise RuntimeError(f"Alpha Vantage error: {data['Error Message']}")

    quote = data.get("Global Quote")

    if not quote:
        raise RuntimeError(f"Unexpected API response: {data}")

    return {
        "event_time": datetime.now(timezone.utc).isoformat(),
        "symbol": quote["01. symbol"],
        "trade_date": quote["07. latest trading day"],
        "open": float(quote["02. open"]),
        "high": float(quote["03. high"]),
        "low": float(quote["04. low"]),
        "close": float(quote["05. price"]),
        "volume": int(quote["06. volume"]),
        "source": "alpha_vantage_global_quote",
    }


def main():
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "client.id": "stock-api-producer",
        "acks": "all",
    })

    print(f"Producing API stock events to Kafka topic: {TOPIC}")
    print(f"Symbols: {API_SYMBOLS}")
    print(f"Polling every {API_POLL_SECONDS} seconds")
    print("Press Ctrl+C to stop.")

    try:
        while True:
            for symbol in API_SYMBOLS:
                try:
                    event = fetch_global_quote(symbol)

                    producer.produce(
                        TOPIC,
                        key=event["symbol"].encode("utf-8"),
                        value=json.dumps(event).encode("utf-8"),
                        callback=delivery_report,
                    )

                    producer.poll(0)

                    print(
                        f"Fetched {event['symbol']} "
                        f"close={event['close']} "
                        f"trade_date={event['trade_date']}"
                    )

                except Exception as exc:
                    print(f"Failed to fetch/produce event for {symbol}: {exc}")

            time.sleep(API_POLL_SECONDS)

    except KeyboardInterrupt:
        print("Stopping API producer...")

    finally:
        producer.flush()


if __name__ == "__main__":
    main()
    