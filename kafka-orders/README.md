# Kafka Orders Processing System (Python + Avro)

This project implements a Kafka-based order processing pipeline using **Python**, **Kafka**, and **Avro serialization**.  
It includes:

- 🔹 **Producer** – generates random order messages (orderId, product, price)
- 🔹 **Consumer** – processes messages, calculates running average of prices
- 🔹 **Retry Logic** – automatic retry for temporary failures
- 🔹 **Dead Letter Queue (DLQ)** – permanently failed messages are redirected
- 🔹 **Avro Serialization** – using fastavro
- 🔹 **Topics**: `orders` and `orders-dlq`

## 📌 1. Project Structure

```
kafka-orders/
├── schemas/
│   └── order.avsc
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
├── avro_helper.py
├── quick_dlq_reader.py
├── requirements.txt
└── README.md
```

## 📌 2. Install Dependencies

```
python -m pip install -r requirements.txt
```

Dependencies:

```
kafka-python
fastavro
```

## 📌 3. Start Kafka & Zookeeper (Windows)

### Start Zookeeper

```
cd C:\kafka
.\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
```

### Start Kafka Broker

```
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

Kafka now runs on:
```
localhost:9092
```

## 📌 4. Create Kafka Topics

### Create orders topic
```
cd C:\kafka
.\bin\windows\kafka-topics.bat --create --topic orders --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

### Create DLQ topic
```
cd C:\kafka
.\bin\windows\kafka-topics.bat --create --topic orders-dlq --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1
```

### List topics
```
cd C:\kafka
.\bin\windows\kafka-topics.bat --list --bootstrap-server localhost:9092
```

### Describe topics
```
cd C:\kafka
.\bin\windows\kafka-topics.bat --describe --topic orders --bootstrap-server localhost:9092
.\bin\windows\kafka-topics.bat --describe --topic orders-dlq --bootstrap-server localhost:9092
```

## 📌 5. Running the System

### ✔ Start the Consumer First

```
python -m consumer.consumer --broker localhost:9092 --topic orders --schema schemas/order.avsc --max-retries 3
```

### ✔ Run the Producer

```
python -m producer.producer --broker localhost:9092 --topic orders --schema schemas/order.avsc --interval 0.5 --count 50
```

### ✔ View DLQ Messages

```
python quick_dlq_reader.py
```

## 📌 6. Avro Schema (order.avsc)

```
{
  "namespace": "com.orders",
  "type": "record",
  "name": "Order",
  "fields": [
    { "name": "orderId", "type": "string" },
    { "name": "product", "type": "string" },
    { "name": "price", "type": "float" }
  ]
}
```

## 📌 7. Features Summary

| Feature | Status |
|--------|--------|
| Avro Serialization | ✔ Implemented |
| Kafka Producer | ✔ Implemented |
| Kafka Consumer | ✔ Implemented |
| Retry Logic | ✔ Implemented |
| Dead Letter Queue | ✔ Implemented |
| Running Average | ✔ Implemented |
| Windows Support | ✔ Yes |

## 📌 8. Retry & DLQ Logic Explanation

### 🔁 Retry Logic
- Retries temporary errors up to 3 times  
- Exponential backoff (1s → 2s → 4s)

### 🟥 Dead Letter Queue
Message is sent to DLQ when:
- Permanent failure occurs  
- OR all retries fail  

DLQ topic: `orders-dlq`

---

# End of README
