import json
import logging
import os
import time
import threading
from dotenv import load_dotenv
from websocket import WebSocketApp, enableTrace
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
WS_URL = "wss://ws.kraken.com/v2"
INSTRUMENTS = ["BTC/USD", "XRP/USD", "SUI/USD"]

class KrakenWebSocket:
    def __init__(self, kafka_bootstrap_servers):
        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,
            max_block_ms=10000
        )
        self.ws = None
        self.keep_running = True

    def get_topic_name(self, instrument):
        """Generate Kafka topic name for an instrument"""
        return f"ohlc_data_{instrument.replace('/', '_')}"

    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            data = json.loads(message)
            # Skip heartbeat and subscription status messages
            if data.get("method") in ["heartbeat", "pong"] or data.get("result") == "subscribed":
                return

            # Process OHLC data
            if data.get("channel") == "ohlc":
                ohlc_data = data["data"][0]
                instrument = ohlc_data["symbol"]
                normalized_data = {
                    "instrument": instrument,
                    "timestamp": ohlc_data["timestamp"],
                    "open": float(ohlc_data["open"]),
                    "high": float(ohlc_data["high"]),
                    "low": float(ohlc_data["low"]),
                    "close": float(ohlc_data["close"]),
                    "volume": float(ohlc_data["volume"]),
                    "interval": ohlc_data["interval"]
                }
                topic = self.get_topic_name(instrument)
                self.producer.send(topic, value=normalized_data)
                logger.info(f"Published OHLC data to {topic}: {normalized_data}")
            else:
                logger.debug(f"Non-OHLC message: {data}")
        except Exception as e:
            logger.error(f"Error processing message: {e}, Message: {message}")

    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"WebSocket error: {error}")

    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket closure"""
        logger.info(f"WebSocket closed: {close_status_code}, {close_msg}")
        self.keep_running = False
        logger.info("Attempting to reconnect...")

    def on_open(self, ws):
        """Subscribe to OHLC channels on WebSocket open"""
        try:
            subscription = {
                "method": "subscribe",
                "params": {
                    "channel": "ohlc",
                    "symbol": INSTRUMENTS,
                    "interval": 1
                }
            }
            ws.send(json.dumps(subscription))
            logger.info(f"Subscribed to OHLC for {INSTRUMENTS}")
        except Exception as e:
            logger.error(f"Error subscribing to OHLC: {e}")

    def send_heartbeat(self, ws):
        """Send periodic heartbeat to maintain connection"""
        while self.keep_running:
            try:
                ws.send(json.dumps({"method": "ping"}))
                logger.debug("Sent heartbeat")
                time.sleep(30)  # Kraken expects heartbeats every 30 seconds
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                break

    def start(self):
        """Start WebSocket connection with exponential backoff"""
        retry_delay = 5  # Initial delay in seconds
        max_delay = 60  # Maximum delay in seconds
        while True:
            try:
                logger.info(f"Connecting to WebSocket: {WS_URL}")
                enableTrace(False)  # Disable verbose tracing
                self.ws = WebSocketApp(
                    WS_URL,
                    on_message=self.on_message,
                    on_error=self.on_error,
                    on_close=self.on_close,
                    on_open=self.on_open
                )
                self.keep_running = True
                # Start heartbeat in a separate thread
                threading.Thread(target=self.send_heartbeat, args=(self.ws,), daemon=True).start()
                self.ws.run_forever(ping_interval=10, ping_timeout=8)
            except Exception as e:
                logger.error(f"WebSocket startup error: {e}")
            logger.info(f"Reconnecting in {retry_delay} seconds...")
            time.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_delay)  # Exponential backoff
            self.producer.flush()  # Ensure messages are sent before retry

    def close(self):
        """Clean up resources"""
        self.keep_running = False
        if self.ws:
            self.ws.close()
        self.producer.flush()
        self.producer.close()
        logger.info("Kafka producer and WebSocket closed")

def main():
    # Load environment variables
    load_dotenv()
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")

    # Initialize and start WebSocket
    ws_client = KrakenWebSocket(KAFKA_BOOTSTRAP_SERVERS)
    try:
        ws_client.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        ws_client.close()

if __name__ == "__main__":
    main()