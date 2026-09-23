import os
import json
import time

from flask import Flask, request, jsonify
import mysql.connector
from confluent_kafka import Producer
import pika

app = Flask(__name__)

# --- Конфигурация из env (имена переменных фиксированы в docker-compose) ---
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "mysql"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "database": os.getenv("DB_NAME", "orders"),
    "user": os.getenv("DB_USER", "app"),
    "password": os.getenv("DB_PASSWORD", "apppass"),
}

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBIT_USER", "app")
RABBIT_PASS = os.getenv("RABBIT_PASS", "apppass")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "order-notifications")

# Kafka producer создаём один раз
producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def get_db():
    return mysql.connector.connect(**DB_CONFIG)


def wait_for_db(max_attempts=30, delay=2):
    """Ждём MySQL, пока не поднимется."""
    for attempt in range(1, max_attempts + 1):
        try:
            conn = get_db()
            conn.close()
            print(f"[FLASK] MySQL is ready (attempt {attempt})", flush=True)
            return
        except Exception as e:
            print(f"[FLASK] MySQL not ready ({attempt}/{max_attempts}): {e}", flush=True)
            time.sleep(delay)
    raise RuntimeError("MySQL is not available")


@app.route("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/orders", methods=["POST"])
def create_order():
    data = request.get_json(force=True, silent=True) or {}

    customer = data.get("customer")
    product = data.get("product")
    amount = data.get("amount")

    if not customer or not product or amount is None:
        return jsonify({"error": "customer, product, amount are required"}), 400

    try:
        amount = float(amount)
    except (TypeError, ValueError):
        return jsonify({"error": "amount must be a number"}), 400

    # 1. Сохраняем заказ в MySQL и получаем order_id
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO orders (customer, product, amount) VALUES (%s, %s, %s)",
            (customer, product, amount),
        )
        conn.commit()
        order_id = cur.lastrowid
        cur.close()
        conn.close()
    except Exception as e:
        print(f"[FLASK] MySQL error: {e}", flush=True)
        return jsonify({"error": "database error"}), 500

    print(f"[FLASK] Order {order_id} saved to MySQL", flush=True)

    # 2. Отправляем событие в Kafka. Ключ = order_id (чтобы один заказ всегда шёл в одну партицию)
    event = {
        "order_id": order_id,
        "customer": customer,
        "product": product,
        "amount": amount,
        "status": "created",
        "timestamp": time.time(),
    }

    try:
        producer.produce(
            topic="order-events",
            key=str(order_id).encode("utf-8"),
            value=json.dumps(event).encode("utf-8"),
        )
        producer.flush(timeout=5)
        print(f"[FLASK] Event for order {order_id} sent to Kafka", flush=True)
    except Exception as e:
        print(f"[FLASK] Kafka error: {e}", flush=True)

    # 3. Отправляем короткое уведомление в RabbitMQ
    try:
        credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        params = pika.ConnectionParameters(
            host=RABBIT_HOST,
            credentials=credentials,
            heartbeat=30,
            blocked_connection_timeout=10,
        )
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=RABBIT_QUEUE, durable=True)

        message = f"Order {order_id} created: {customer} bought {product} for {amount}"
        channel.basic_publish(
            exchange="",
            routing_key=RABBIT_QUEUE,
            body=message.encode("utf-8"),
            properties=pika.BasicProperties(delivery_mode=2),  # persistent
        )
        connection.close()
        print(f"[FLASK] Notification for order {order_id} sent to RabbitMQ", flush=True)
    except Exception as e:
        print(f"[FLASK] RabbitMQ error: {e}", flush=True)

    return jsonify({"order_id": order_id, "status": "created"}), 201


if __name__ == "__main__":
    wait_for_db()
    app.run(host="0.0.0.0", port=5000)