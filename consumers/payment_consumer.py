import os
import json
import time

from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = "payment-group"
TOPIC = "order-events"


def make_consumer():
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    return Consumer(conf)


def main():
    consumer = make_consumer()
    consumer.subscribe([TOPIC])
    print(f"[PAYMENT] Consumer started, group={GROUP_ID}, topic={TOPIC}", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[PAYMENT] Kafka error: {msg.error()}", flush=True)
                continue

            try:
                order = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[PAYMENT] Bad message: {e}", flush=True)
                continue

            print(
                f"[PAYMENT] Got order {order['order_id']} "
                f"(partition={msg.partition()}, offset={msg.offset()}): "
                f"customer={order['customer']}, amount={order['amount']}",
                flush=True,
            )

            # Имитация оплаты
            time.sleep(1)
            print(f"[PAYMENT] Payment done for order {order['order_id']}", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()