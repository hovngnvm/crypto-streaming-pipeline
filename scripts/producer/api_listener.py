import websocket
import json
from datetime import datetime, timezone, timedelta
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

def on_message(ws, message):
    if not message:
        return
    raw_payload = json.loads(message)
    data = raw_payload['data']
    
    symbol = data['s']
    price = float(data['p'])
    quantity = float(data['q'])
    is_buyer_maker = data['m'] 
    event_time = datetime.fromtimestamp(data['E'] / 1000, tz=timezone(timedelta(hours=7)))
    # True if the order is a sell order, False if the order is a buy order
    trade_type = "SELL" if is_buyer_maker else "BUY"
    total_value = price * quantity
    
    if total_value > 10000:
        producer.send("crypto_trades", value=data)
        print(f"| {symbol} | [{trade_type}] Price: {price:.2f} | Quantity: {quantity:.4f} | Total Value: ${total_value:,.2f}" f"| Event Time: {event_time.strftime('%Y-%m-%d %H:%M:%S UTC+7')}")

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