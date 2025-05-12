import json
import logging
import os
import time
import threading
from dotenv import load_dotenv
from websocket import WebSocketApp, enableTrace
from kafka import KafkaProducer

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
WS_URL = "wss://ws.kraken.com/v2"
INSTRUMENTS = ["BTC/USD", "XRP/USD", "SUI/USD"]
DEPTH = 10

class KrakenOrderBookWebSocket:
    def __init__(self, kafka_bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            max_block_ms=10000
        )
        self.ws = None
        self.keep_running = True
        self.is_connected = False

    def get_topic_name(self, instrument):
        """Generate Kafka topic name for an instrument"""
        return f"order_book_{instrument.replace('/', '_')}"

    def on_message(self, ws, message):
        """Print and publish incoming WebSocket messages"""
        try:
            data = json.loads(message)
            if data.get("method") in ["heartbeat", "pong"]:
                return
            # logger.info(f"Received message: {json.dumps(data, indent=2)}")

            # Publish to Kafka if it's a book message
            if data.get("channel") == "book":
                book_data = data["data"][0]
                instrument = book_data["symbol"]
                if instrument in INSTRUMENTS:
                    topic = self.get_topic_name(instrument)
                    try:
                        self.producer.send(topic, value=data)
                        logger.info(f"Published message to {topic}")
                    except Exception as e:
                        logger.error(f"Failed to publish to {topic}: {e}")
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")
        self.is_connected = False

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket closure"""
        logger.info(f"WebSocket closed: {close_status_code}, {close_msg}")
        self.is_connected = False
        self.keep_running = False
        logger.info("Attempting to reconnect...")

    def on_open(self, ws):
        """Subscribe to order book channels on WebSocket open"""
        try:
            self.is_connected = True
            subscription = {
                "method": "subscribe",
                "params": {
                    "channel": "book",
                    "symbol": INSTRUMENTS,
                    "depth": DEPTH
                }
            }
            ws.send(json.dumps(subscription))
            logger.info(f"Subscribed to order book for {INSTRUMENTS}")
        except Exception as e:
            logger.error(f"Error subscribing: {e}")
            self.is_connected = False

    def send_heartbeat(self, ws):
        """Send periodic heartbeat to maintain connection"""
        while self.keep_running:
            try:
                if not self.is_connected:
                    time.sleep(1)
                    continue
                ws.send(json.dumps({"method": "ping"}))
                logger.debug("Sent heartbeat")
                time.sleep(30)
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                time.sleep(1)

    def start(self):
        """Start WebSocket connection with exponential backoff"""
        retry_delay = 5
        max_delay = 60
        while True:
            try:
                logger.info(f"Connecting to WebSocket: {WS_URL}")
                enableTrace(False)
                self.ws = WebSocketApp(
                    WS_URL,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open
                )
                self.keep_running = True
                self.is_connected = False
                threading.Thread(target=self.send_heartbeat, args=(self.ws,), daemon=True).start()
                self.ws.run_forever(ping_interval=10, ping_timeout=8)
            except Exception as e:
                logger.error(f"WebSocket startup error: {e}")
            logger.info(f"Reconnecting in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)
            self.producer.flush()

    def close(self):
        """Clean up resources"""
        self.keep_running = False
        self.is_connected = False
        if self.ws:
            self.ws.close()
        self.producer.flush()
        self.producer.close()
        logger.info("WebSocket and Kafka producer closed")

def main():
    load_dotenv()
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    ws_client = KrakenOrderBookWebSocket(KAFKA_BOOTSTRAP_SERVERS)
    try:
        ws_client.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        ws_client.close()

if __name__ == "__main__":
    main()