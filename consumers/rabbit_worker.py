import os
import time

import pika

RABBIT_HOST = os.getenv("RABBIT_HOST", "rabbitmq")
RABBIT_USER = os.getenv("RABBIT_USER", "app")
RABBIT_PASS = os.getenv("RABBIT_PASS", "apppass")
RABBIT_QUEUE = os.getenv("RABBIT_QUEUE", "order-notifications")


def connect_with_retry():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials,
        heartbeat=30,
        blocked_connection_timeout=10,
    )
    while True:
        try:
            return pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError as e:
            print(f"[RABBIT] Waiting for RabbitMQ: {e}", flush=True)
            time.sleep(2)


def main():
    connection = connect_with_retry()
    channel = connection.channel()
    channel.queue_declare(queue=RABBIT_QUEUE, durable=True)
    channel.basic_qos(prefetch_count=1)

    def callback(ch, method, properties, body):
        message = body.decode("utf-8", errors="replace")
        print(f"[RABBIT] Notification: {message}", flush=True)
        ch.basic_ack(delivery_tag=method.delivery_tag)

    channel.basic_consume(queue=RABBIT_QUEUE, on_message_callback=callback)
    print(f"[RABBIT] Worker started, queue={RABBIT_QUEUE}", flush=True)
    channel.start_consuming()


if __name__ == "__main__":
    main()