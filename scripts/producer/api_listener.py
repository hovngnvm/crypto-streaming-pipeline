import websocket
import json
import time
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

# Connect to Kafka
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except Exception as e:
        time.sleep(1)
        print('reload')

def on_message(ws, message):
    if not message:
        return
    raw_payload = json.loads(message)
    data = raw_payload['data']
    producer.send("crypto_trades", value=data)
        
def on_error(ws, error):
    print(error)

def on_close(ws, close_status_code, close_msg):
    print("### Connection closed ###")

def on_open(ws):
    print("### Connection opened, listening for trades... ###")

if __name__ == "__main__":
    streams = "btcusdt@trade/ethusdt@trade"
    socket_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
    
    ws = websocket.WebSocketApp(socket_url,
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    
    ws.run_forever()