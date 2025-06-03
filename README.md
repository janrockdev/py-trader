# PY-TRADER

## Description

## Install
```bash
python3 -m venv env
source env/bin/activate
```

### Run Confluent Kafka

```bash
docker-compose down
docker-compose up -d
docker ps
#open: http://localhost:9021
```

### Run Price Producer
```shell
python pricedata_producer.py
```
price:
```
2025-04-30 23:01:50,788 - INFO - Published OHLC data to ohlc_data_BTC_USD: {'instrument': 'BTC/USD', 'timestamp': '2025-04-30T22:05:00.000000Z', 'open': 94435.2, 'high': 94591.9, 'low': 94435.2, 'close': 94591.8, 'volume': 1.40669572, 'interval': 5}
```

### Run Orderbook Producer
```shell
python orderbook_producer.py
```


