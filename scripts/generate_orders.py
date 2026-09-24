"""
Генератор тестовых заказов для демонстрации работы системы.
Шлёт POST на http://localhost/api/orders (через nginx → Flask).

Запуск:
    python scripts/generate_orders.py
    python scripts/generate_orders.py 200
"""

import json
import random
import sys
import time
import urllib.request
import urllib.error


URL = "http://localhost/api/orders"

CUSTOMERS = [
    "Александр", "Мария", "Иван", "Ольга", "Дмитрий",
    "Екатерина", "Сергей", "Анна", "Никита", "Саша",
    "Пётр", "Елена", "Максим", "Татьяна", "Артём",
]

PRODUCTS = [
    "Бензин", "Кофе", "Ноутбук", "Книга", "Телефон",
    "Наушники", "Клавиатура", "Монитор", "Мышь", "Рюкзак",
    "Планшет", "Часы", "Кресло", "Лампа", "Термос",
]


def send_order():
    data = {
        "customer": random.choice(CUSTOMERS),
        "product": random.choice(PRODUCTS),
        "amount": round(random.uniform(50, 50000), 2),
    }
    body = json.dumps(data, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        URL,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status, resp.read().decode("utf-8", errors="replace")


def main():
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 100

    print(f"Отправляю {count} заказов на {URL}")
    ok = 0
    fail = 0

    for i in range(1, count + 1):
        try:
            status, _ = send_order()
            if status in (200, 201):
                ok += 1
            else:
                fail += 1
                print(f"[{i}/{count}] HTTP {status}")
        except urllib.error.HTTPError as e:
            fail += 1
            print(f"[{i}/{count}] HTTPError {e.code}: {e.read().decode(errors='replace')}")
        except Exception as e:
            fail += 1
            print(f"[{i}/{count}] ERROR: {e}")

        if i % 10 == 0:
            print(f"  [{i}/{count}] ok={ok} fail={fail}")

        # небольшая пауза, чтобы Kafka успевала распределять по партициям
        time.sleep(0.05)

    print(f"\nГотово. Успешно: {ok}, ошибок: {fail}")


if __name__ == "__main__":
    main()