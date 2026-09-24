import os
import json
import time

from confluent_kafka import Consumer, KafkaError

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = "delivery-group"
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
    print(f"[DELIVERY] Consumer started, group={GROUP_ID}, topic={TOPIC}", flush=True)

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[DELIVERY] Kafka error: {msg.error()}", flush=True)
                continue

            try:
                order = json.loads(msg.value().decode("utf-8"))
            except Exception as e:
                print(f"[DELIVERY] Bad message: {e}", flush=True)
                continue

            print(
                f"[DELIVERY] Got order {order['order_id']} "
                f"(partition={msg.partition()}, offset={msg.offset()}): "
                f"prepare {order['product']} for {order['customer']}",
                flush=True,
            )

            # Имитация подготовки доставки
            time.sleep(1)
            print(f"[DELIVERY] Delivery prepared for order {order['order_id']}", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()