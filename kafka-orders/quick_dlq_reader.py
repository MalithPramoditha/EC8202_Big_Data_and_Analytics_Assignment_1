# quick_dlq_reader.py
from kafka import KafkaConsumer
from avro_helper import load_schema, deserialize_avro

schema = load_schema("schemas/order.avsc")
c = KafkaConsumer("orders-dlq", bootstrap_servers=["localhost:9092"],
                  auto_offset_reset='earliest', value_deserializer=lambda v: v)
for msg in c:
    try:
        record = deserialize_avro(schema, msg.value)
        print("DLQ record:", record)
    except Exception as e:
        print("Failed to decode DLQ record:", e)
