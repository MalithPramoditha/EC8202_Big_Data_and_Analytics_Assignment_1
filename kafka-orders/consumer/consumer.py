# consumer/consumer.py
import time
import random
import argparse
from kafka import KafkaConsumer, KafkaProducer
from avro_helper import load_schema, deserialize_avro

DEFAULT_BROKER = "localhost:9092"
INPUT_TOPIC = "orders"
DLQ_TOPIC = "orders-dlq"
GROUP_ID = "orders-consumer-group"

class TemporaryProcessingError(Exception):
    pass

class PermanentProcessingError(Exception):
    pass

def process_order(record):
    """
    Process the order dict.
    This is where you'd include business logic.
    For demo: randomly raise errors to show retry/DLQ behavior.
    """
    # Simulate a 10% permanent failure, 20% temporary failure
    r = random.random()
    if r < 0.10:
        raise PermanentProcessingError("Permanent failure (bad data)")
    elif r < 0.30:
        raise TemporaryProcessingError("Temporary failure (downstream fail)")
    # else success
    # (real logic would go here)
    return True

def handle_message(producer_for_dlq, schema, raw_value, max_retries=3):
    """
    Deserialize and process with retry + DLQ.
    raw_value: bytes
    """
    order = deserialize_avro(schema, raw_value)
    attempt = 0
    backoff = 1.0
    while attempt <= max_retries:
        try:
            attempt += 1
            # process
            process_order(order)
            # success: return True and the order
            return True, order
        except TemporaryProcessingError as e:
            print(f"[Consumer] Temporary error on attempt {attempt}: {e}")
            if attempt > max_retries:
                print("[Consumer] Retries exhausted — sending to DLQ")
                producer_for_dlq.send(DLQ_TOPIC, value=raw_value)
                producer_for_dlq.flush()
                return False, order
            else:
                time.sleep(backoff)
                backoff *= 2
                continue
        except PermanentProcessingError as e:
            print(f"[Consumer] Permanent error: {e}. Sending to DLQ.")
            producer_for_dlq.send(DLQ_TOPIC, value=raw_value)
            producer_for_dlq.flush()
            return False, order
        except Exception as e:
            # Unknown errors treated as temporary
            print(f"[Consumer] Unexpected error: {e}. Treating as temporary.")
            if attempt > max_retries:
                producer_for_dlq.send(DLQ_TOPIC, value=raw_value)
                producer_for_dlq.flush()
                return False, order
            time.sleep(backoff)
            backoff *= 2
    # default: failed
    producer_for_dlq.send(DLQ_TOPIC, value=raw_value)
    producer_for_dlq.flush()
    return False, order

def consumer_loop(broker=DEFAULT_BROKER, input_topic=INPUT_TOPIC,
                  schema_path="../schemas/order.avsc", group_id=GROUP_ID,
                  max_retries=3):
    schema = load_schema(schema_path)
    consumer = KafkaConsumer(input_topic,
                             bootstrap_servers=[broker],
                             group_id=group_id,
                             enable_auto_commit=True,
                             auto_offset_reset='earliest',
                             value_deserializer=lambda v: v)

    dlq_producer = KafkaProducer(bootstrap_servers=[broker], value_serializer=lambda v: v)

    total = 0.0
    count = 0

    print("[Consumer] Starting consumption loop...")
    try:
        for msg in consumer:
            raw = msg.value  # bytes
            success, order = handle_message(dlq_producer, schema, raw, max_retries=max_retries)
            if success:
                price = float(order.get("price", 0.0))
                total += price
                count += 1
                running_avg = total / count if count else 0.0
                print(f"[Consumer] Processed orderId={order.get('orderId')} product={order.get('product')} price={price:.2f}")
                print(f"[Consumer] Running average price after {count} items: {running_avg:.2f}")
            else:
                print(f"[Consumer] Order {order.get('orderId')} moved to DLQ.")
    except KeyboardInterrupt:
        print("Consumer stopped by user")
    finally:
        consumer.close()
        dlq_producer.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--broker", default=DEFAULT_BROKER)
    parser.add_argument("--topic", default=INPUT_TOPIC)
    parser.add_argument("--schema", default="../schemas/order.avsc")
    parser.add_argument("--group", default=GROUP_ID)
    parser.add_argument("--max-retries", type=int, default=3)
    args = parser.parse_args()
    consumer_loop(args.broker, args.topic, args.schema, args.group, args.max_retries)
