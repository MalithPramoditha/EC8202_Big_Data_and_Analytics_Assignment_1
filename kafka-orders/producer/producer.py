# producer/producer.py
import uuid
import time
import random
import argparse
from kafka import KafkaProducer
from avro_helper import load_schema, serialize_avro

DEFAULT_BROKER = "localhost:9092"
TOPIC = "orders"

PRODUCTS = ["Item1", "Item2", "Item3", "Gadget", "Widget"]

def make_order(schema):
    return {
        "orderId": str(uuid.uuid4()),
        "product": random.choice(PRODUCTS),
        "price": float(round(random.uniform(5.0, 500.0), 2))
    }

def main(broker=DEFAULT_BROKER, topic=TOPIC, schema_path="../schemas/order.avsc",
         interval=1.0, count=None):
    schema = load_schema(schema_path)
    producer = KafkaProducer(bootstrap_servers=[broker],
                             value_serializer=lambda v: v)  # we send bytes already

    sent = 0
    try:
        while True:
            order = make_order(schema)
            data = serialize_avro(schema, order)
            # send raw bytes as value
            producer.send(topic, value=data)
            producer.flush()
            sent += 1
            print(f"[Producer] Sent order #{sent}: {order}")
            if count and sent >= count:
                break
            time.sleep(interval)
    except KeyboardInterrupt:
        print("Producer stopped by user")
    finally:
        producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=DEFAULT_BROKER)
    parser.add_argument("--topic", default=TOPIC)
    parser.add_argument("--schema", default="../schemas/order.avsc")
    parser.add_argument("--interval", type=float, default=1.0,
                        help="seconds between messages")
    parser.add_argument("--count", type=int, default=None,
                        help="number of messages to send (default: infinite)")
    args = parser.parse_args()
    main(args.broker, args.topic, args.schema, args.interval, args.count)
