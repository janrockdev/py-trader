import json
import logging
import os
import time
from collections import deque
from dotenv import load_dotenv
from kafka import KafkaConsumer
from rich.console import Console
from rich.panel import Panel
from rich.layout import Layout
from rich.text import Text

# Configure basic logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Constants
INSTRUMENTS = ["BTC/USD", "XRP/USD", "SUI/USD"]
OHLC_TOPICS = [f"ohlc_data_{inst.replace('/', '_')}" for inst in INSTRUMENTS]
ORDER_BOOK_TOPICS = [f"order_book_{inst.replace('/', '_')}" for inst in INSTRUMENTS]
KAFKA_TOPICS = OHLC_TOPICS + ORDER_BOOK_TOPICS
UPDATE_INTERVAL = 5  # Seconds for dashboard update
CHART_HEIGHT = 10  # Number of rows in the price chart
CHART_VALUES = 30  # Number of close prices to display
INDICATORS_WIDTH = 40  # Width for indicators sub-row
PRICE_LABEL_WIDTH = 10  # Approximate width for price labels
CACHE_SIZE = 50    # Number of OHLC bars to store in cache
SMA_SHORT = 10     # Short SMA period
SMA_LONG = 20      # Long SMA period
EMA_PERIOD = 20    # EMA period
RSI_PERIOD = 14    # RSI period
MACD_FAST = 12     # MACD fast EMA period
MACD_SLOW = 26     # MACD slow EMA period
MACD_SIGNAL = 9    # MACD signal line period
BB_PERIOD = 20     # Bollinger Bands period
BB_STD = 2         # Bollinger Bands standard deviations

# Instrument-specific price interval scaling factors
PRICE_SCALE_FACTORS = {
    "BTC/USD": 0.005,  # ~$500 for ~$100,000 price
    "XRP/USD": 0.025,  # ~$0.05 for ~$2 price
    "SUI/USD": 0.03    # ~$0.10 for ~$3.40 price
}

class OHLCOrderBookConsumer:
    def __init__(self, kafka_bootstrap_servers):
        self.consumer = KafkaConsumer(
            *KAFKA_TOPICS,
            bootstrap_servers=kafka_bootstrap_servers,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="ohlc_orderbook_dashboard"
        )
        self.keep_running = True
        self.ohlc_cache = {inst: deque(maxlen=CACHE_SIZE) for inst in INSTRUMENTS}
        self.order_book_cache = {inst: {"bids": [], "asks": []} for inst in INSTRUMENTS}
        self.console = Console()
        # Initialize layout once
        self.layout = Layout()
        self.layout.split_row(
            Layout(name="chart"),
            Layout(name="indicators", size=INDICATORS_WIDTH + 10)
        )

    def update_ohlc_cache(self, instrument, message):
        """Update in-memory cache with OHLC data"""
        try:
            if message.get("instrument") != instrument:
                return
            if all(k in message for k in ["timestamp", "close"]):
                self.ohlc_cache[instrument].append({
                    "timestamp": message["timestamp"],
                    "close": float(message["close"])
                })
        except Exception as e:
            logger.error(f"Error updating OHLC cache for {instrument}: {e}")

    def update_order_book_cache(self, instrument, message):
        """Update in-memory cache with order book data"""
        try:
            if message.get("channel") != "book":
                return
            book_data = message["data"][0]
            if book_data["symbol"] != instrument:
                return
            cache = self.order_book_cache[instrument]
            if message.get("type") == "snapshot":
                cache["bids"] = [(float(b["price"]), float(b["qty"])) for b in book_data.get("bids", [])]
                cache["asks"] = [(float(a["price"]), float(a["qty"])) for a in book_data.get("asks", [])]
            else:  # Update
                for bid in book_data.get("bids", []):
                    price = float(bid["price"])
                    qty = float(bid["qty"])
                    cache["bids"] = [(p, q) for p, q in cache["bids"] if p != price]
                    if qty > 0:
                        cache["bids"].append((price, qty))
                for ask in book_data.get("asks", []):
                    price = float(ask["price"])
                    qty = float(ask["qty"])
                    cache["asks"] = [(p, q) for p, q in cache["asks"] if p != price]
                    if qty > 0:
                        cache["asks"].append((price, qty))
                cache["bids"] = sorted(cache["bids"], reverse=True)[:10]
                cache["asks"] = sorted(cache["asks"])[:10]
        except Exception as e:
            logger.error(f"Error updating order book cache for {instrument}: {e}")

    def calculate_sma(self, prices, period):
        """Calculate Simple Moving Average"""
        if len(prices) < period:
            return None
        return sum(prices[-period:]) / period

    def calculate_ema(self, prices, period):
        """Calculate Exponential Moving Average"""
        if len(prices) < period:
            return None
        k = 2 / (period + 1)
        ema = prices[-period]
        for price in prices[-period + 1:]:
            ema = price * k + ema * (1 - k)
        return ema

    def calculate_rsi(self, prices, period):
        """Calculate Relative Strength Index"""
        if len(prices) < period + 1:
            return None
        gains = []
        losses = []
        for i in range(1, len(prices)):
            diff = prices[i] - prices[i-1]
            gains.append(diff if diff > 0 else 0)
            losses.append(-diff if diff < 0 else 0)
        if len(gains) < period:
            return None
        avg_gain = sum(gains[-period:]) / period
        avg_loss = sum(losses[-period:]) / period
        rs = avg_gain / avg_loss if avg_loss != 0 else 0
        return 100 - (100 / (1 + rs))

    def calculate_macd(self, prices):
        """Calculate MACD (12, 26, 9)"""
        if len(prices) < MACD_SLOW + MACD_SIGNAL:
            return None, None
        ema_fast = self.calculate_ema(prices[-MACD_SLOW-MACD_SIGNAL:], MACD_FAST)
        ema_slow = self.calculate_ema(prices[-MACD_SLOW-MACD_SIGNAL:], MACD_SLOW)
        if ema_fast is None or ema_slow is None:
            return None, None
        macd_line = ema_fast - ema_slow
        # Calculate signal line (9-period EMA of MACD line)
        macd_prices = []
        for i in range(len(prices) - MACD_SLOW, len(prices)):
            ema_f = self.calculate_ema(prices[:i+1], MACD_FAST)
            ema_s = self.calculate_ema(prices[:i+1], MACD_SLOW)
            if ema_f is not None and ema_s is not None:
                macd_prices.append(ema_f - ema_s)
        signal_line = self.calculate_ema(macd_prices, MACD_SIGNAL) if len(macd_prices) >= MACD_SIGNAL else None
        return macd_line, signal_line

    def calculate_bollinger_bands(self, prices, period, std_dev):
        """Calculate Bollinger Bands (20-period, 2 std)"""
        if len(prices) < period:
            return None, None, None
        sma = self.calculate_sma(prices, period)
        if sma is None:
            return None, None, None
        # Calculate standard deviation
        squared_diff = [(price - sma) ** 2 for price in prices[-period:]]
        variance = sum(squared_diff) / period
        std = variance ** 0.5
        upper_band = sma + std_dev * std
        lower_band = sma - std_dev * std
        return sma, upper_band, lower_band

    def generate_signals(self, prices, bid_ask_ratio, imbalance):
        """Generate Buy/Sell/Hold signals based on multiple indicators"""
        if len(prices) < max(SMA_LONG, MACD_SLOW + MACD_SIGNAL, BB_PERIOD):
            return "None"
        short_sma = self.calculate_sma(prices, SMA_SHORT)
        long_sma = self.calculate_sma(prices, SMA_LONG)
        rsi = self.calculate_rsi(prices, RSI_PERIOD)
        macd_line, signal_line = self.calculate_macd(prices)
        _, bb_upper, bb_lower = self.calculate_bollinger_bands(prices, BB_PERIOD, BB_STD)
        current_price = prices[-1] if prices else 0
        if (short_sma is None or long_sma is None or rsi is None or
            macd_line is None or signal_line is None or bb_upper is None):
            return "None"
        # Buy conditions
        buy_condition = (
            short_sma > long_sma and
            rsi < 70 and
            macd_line > signal_line and
            current_price <= bb_lower * 1.01  # Near or below lower band
        )
        # Sell conditions
        sell_condition = (
            short_sma < long_sma and
            rsi > 30 and
            macd_line < signal_line and
            current_price >= bb_upper * 0.99  # Near or above upper band
        )
        # Adjust with order book metrics
        if buy_condition and (bid_ask_ratio > 1.1 or imbalance > 0.1):
            return "Buy"
        elif sell_condition and (bid_ask_ratio < 0.9 or imbalance < -0.1):
            return "Sell"
        return "Hold"

    def format_price(self, price, instrument):
        """Format price with more decimals for small prices"""
        if price < 10:  # XRP, SUI
            return f"{price:.4f}"
        return f"{price:.2f}"  # BTC

    def generate_price_chart(self, cache, instrument, chart_width):
        """Generate ASCII chart for 30 close prices with adaptive price intervals"""
        if not cache:
            return ["No data available"]

        # Extract close prices (limit to 30)
        close_prices = [bar["close"] for bar in list(cache)[-30:]]
        if not close_prices:
            return ["No price data"]

        # Determine price range and interval
        min_price = min(close_prices)
        max_price = max(close_prices)
        price_interval = self.calculate_price_interval(close_prices, instrument)
        chart_min = min_price - (min_price % price_interval)
        chart_max = max_price + (price_interval - (max_price % price_interval))
        price_range = chart_max - chart_min
        if price_range == 0:
            price_range = price_interval

        # Initialize chart
        chart = [[" " for _ in range(chart_width)] for _ in range(CHART_HEIGHT)]

        # Plot close prices (30 values, scaled to chart_width)
        step = max(1, CHART_VALUES / chart_width)  # Scale 30 values to chart_width
        for i in range(min(chart_width, CHART_VALUES)):
            price_idx = int(i * step)
            if price_idx < len(close_prices):
                price = close_prices[price_idx]
                normalized_height = int(((price - chart_min) / price_range) * (CHART_HEIGHT - 1))
                if 0 <= normalized_height < CHART_HEIGHT:
                    chart[CHART_HEIGHT - 1 - normalized_height][i] = "█"

        # Format chart with price labels
        chart_lines = []
        for i in range(CHART_HEIGHT):
            price_at_row = chart_max - (i * price_range / (CHART_HEIGHT - 1))
            chart_lines.append(f"{self.format_price(price_at_row, instrument)} | {' '.join(chart[i])}")
        # Pad to ensure fixed height
        while len(chart_lines) < CHART_HEIGHT:
            chart_lines.append(" " * (chart_width + PRICE_LABEL_WIDTH))

        return chart_lines

    def calculate_price_interval(self, close_prices, instrument):
        """Calculate dynamic price interval based on price scale"""
        if not close_prices:
            return 1.0
        avg_price = sum(close_prices) / len(close_prices)
        price_range = max(close_prices) - min(close_prices)
        scale_factor = PRICE_SCALE_FACTORS.get(instrument, 0.01)
        interval = max(avg_price * scale_factor, price_range / CHART_HEIGHT)
        if avg_price > 1000:  # BTC
            interval = round(interval / 100) * 100
        elif avg_price > 10:  # SUI
            interval = round(interval / 0.1) * 0.1
        else:  # XRP
            interval = round(interval / 0.05) * 0.05
        return max(interval, 0.01)

    def display_dashboard(self):
        """Display CLI dashboard with split frame-row for chart and indicators"""
        while self.keep_running:
            try:
                # Poll for new messages
                messages = self.consumer.poll(timeout_ms=1000)
                for topic_partition, partition_messages in messages.items():
                    for msg in partition_messages:
                        instrument = next((inst for inst in INSTRUMENTS if f"ohlc_data_{inst.replace('/', '_')}" == msg.topic or f"order_book_{inst.replace('/', '_')}" == msg.topic), None)
                        if instrument:
                            if f"ohlc_data_{instrument.replace('/', '_')}" == msg.topic:
                                self.update_ohlc_cache(instrument, msg.value)
                            else:
                                self.update_order_book_cache(instrument, msg.value)

                # Update dashboard
                self.console.clear()
                for instrument in INSTRUMENTS:
                    cache = self.ohlc_cache[instrument]
                    order_book = self.order_book_cache[instrument]
                    latest_bar = cache[-1] if cache else None
                    current_price = latest_bar["close"] if latest_bar else 0

                    # Calculate dynamic chart width
                    console_width = self.console.width
                    layout_margins = 10  # Approximate padding/borders
                    chart_width = max(CHART_VALUES, console_width - INDICATORS_WIDTH - PRICE_LABEL_WIDTH - layout_margins)

                    # Generate price chart
                    chart_lines = self.generate_price_chart(cache, instrument, chart_width)

                    # Calculate indicators and signals
                    close_prices = [bar["close"] for bar in cache]
                    sma_20 = self.calculate_sma(close_prices, SMA_LONG)
                    ema_20 = self.calculate_ema(close_prices, EMA_PERIOD)
                    rsi = self.calculate_rsi(close_prices, RSI_PERIOD)
                    macd_line, signal_line = self.calculate_macd(close_prices)
                    _, bb_upper, bb_lower = self.calculate_bollinger_bands(close_prices, BB_PERIOD, BB_STD)

                    # Calculate order book metrics
                    best_bid = order_book["bids"][0][0] if order_book["bids"] else 0
                    best_ask = order_book["asks"][0][0] if order_book["asks"] else 0
                    spread = best_ask - best_bid if best_bid and best_ask else 0
                    mid_price = (best_bid + best_ask) / 2 if best_bid and best_ask else 0
                    spread_percent = (spread / mid_price * 100) if mid_price > 0 else 0
                    spread_indicator = (
                        "Narrow" if spread_percent < 0.1 else
                        "Moderate" if spread_percent < 0.5 else
                        "Wide"
                    )
                    bid_volume = sum(qty for _, qty in order_book["bids"]) if order_book["bids"] else 0
                    ask_volume = sum(qty for _, qty in order_book["asks"]) if order_book["asks"] else 0
                    volume_ratio = bid_volume / ask_volume if ask_volume > 0 else 0
                    imbalance = (
                        (bid_volume - ask_volume) / (bid_volume + ask_volume)
                        if bid_volume + ask_volume > 0 else 0
                    )

                    # Generate signals
                    signal = self.generate_signals(close_prices, volume_ratio, imbalance)

                    # Build indicators content
                    indicators = []
                    indicators.append(f"Current Price: {self.format_price(current_price, instrument)}")
                    indicators.append(f"SMA (20): {self.format_price(sma_20, instrument)}" if sma_20 else "SMA (20): N/A")
                    indicators.append(f"EMA (20): {self.format_price(ema_20, instrument)}" if ema_20 else "EMA (20): N/A")
                    indicators.append(f"RSI (14): {rsi:.2f}" if rsi else "RSI (14): N/A")
                    indicators.append(f"MACD: {macd_line:.2f}/{signal_line:.2f}" if macd_line and signal_line else "MACD: N/A")
                    indicators.append(f"BB Upper: {self.format_price(bb_upper, instrument)}" if bb_upper else "BB Upper: N/A")
                    indicators.append(f"BB Lower: {self.format_price(bb_lower, instrument)}" if bb_lower else "BB Lower: N/A")
                    indicators.append(f"Strategy: {signal}")
                    indicators.append("--- Order Book ---")
                    indicators.append(Text(f"Spread: {self.format_price(spread, instrument)}", style="yellow"))
                    indicators.append(Text(f"Spread %: {spread_percent:.2f}% ({spread_indicator})", style="yellow"))
                    indicators.append(Text(f"Bid Vol: {self.format_price(bid_volume, instrument)}", style="green"))
                    indicators.append(Text(f"Ask Vol: {self.format_price(ask_volume, instrument)}", style="red"))
                    indicators.append(Text(f"Bid/Ask Ratio: {volume_ratio:.2f}", style="cyan"))
                    indicators.append(Text(f"Imbalance: {imbalance:.2f}", style="cyan"))
                    # Truncate to fit chart height
                    indicators = indicators[:CHART_HEIGHT]
                    while len(indicators) < CHART_HEIGHT:
                        indicators.append("")

                    # Update layout
                    self.layout["chart"].update(
                        Panel(
                            "\n".join(chart_lines),
                            title="Close Price Chart",
                            border_style="blue",
                            padding=(0, 1),
                            height=CHART_HEIGHT + 2
                        )
                    )
                    self.layout["indicators"].update(
                        Panel(
                            "\n".join(str(line) for line in indicators),
                            title="Indicators & Strategy",
                            border_style="blue",
                            padding=(0, 1),
                            height=CHART_HEIGHT + 2
                        )
                    )

                    # Display dashboard
                    self.console.print(Panel(
                        self.layout,
                        title=f"OHLC Dashboard - {instrument} (Cached {len(cache)} bars)",
                        border_style="blue",
                        padding=(0, 1),
                        height=CHART_HEIGHT + 4
                    ))

                time.sleep(UPDATE_INTERVAL)
            except Exception as e:
                logger.error(f"Error in dashboard loop: {e}")

    def start(self):
        """Start consumer and dashboard"""
        try:
            self.display_dashboard()
        finally:
            self.close()

    def close(self):
        """Clean up resources"""
        self.keep_running = False
        self.consumer.close()
        logger.info("Kafka consumer closed")

def main():
    load_dotenv()
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")
    consumer = OHLCOrderBookConsumer(KAFKA_BOOTSTRAP_SERVERS)
    try:
        consumer.start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        consumer.close()

if __name__ == "__main__":
    main()