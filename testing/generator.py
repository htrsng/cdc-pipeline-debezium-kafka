"""
Sinh giao dịch liên tục vào Postgres nguồn (OLTP) để mô phỏng hệ thống
đang hoạt động thật, phục vụ đo latency CDC hàng loạt.

Chạy: python generator.py --count 50 --interval 1
"""

import argparse
import random
import time
import logging

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("generator")

SOURCE_CONN_PARAMS = dict(
    host="localhost",
    port=5432,
    dbname="oltp_db",
    user="postgres",
    password="postgres",
)

STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "CANCELLED"]


def get_connection():
    conn = psycopg2.connect(**SOURCE_CONN_PARAMS)
    conn.autocommit = True
    return conn


def get_random_customer_id(cur):
    cur.execute("SELECT customer_id FROM customers ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    return row[0] if row else None


def get_random_product(cur):
    cur.execute("SELECT product_id, unit_price FROM products ORDER BY random() LIMIT 1")
    return cur.fetchone()


def create_order_with_items(cur):
    customer_id = get_random_customer_id(cur)
    cur.execute(
        "INSERT INTO orders (customer_id, order_status) VALUES (%s, %s) RETURNING order_id",
        (customer_id, "PENDING"),
    )
    order_id = cur.fetchone()[0]

    num_items = random.randint(1, 3)
    for _ in range(num_items):
        product_id, unit_price = get_random_product(cur)
        quantity = random.randint(1, 5)
        cur.execute(
            "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s, %s, %s, %s)",
            (order_id, product_id, quantity, unit_price),
        )
    log.info(f"[CREATE] order_id={order_id}, customer_id={customer_id}, {num_items} item(s)")
    return order_id


def update_random_order_status(cur):
    cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return
    order_id = row[0]
    new_status = random.choice(STATUSES)
    cur.execute(
        "UPDATE orders SET order_status = %s, updated_at = now() WHERE order_id = %s",
        (new_status, order_id),
    )
    log.info(f"[UPDATE] order_id={order_id} -> order_status={new_status}")


def delete_random_order_item(cur):
    cur.execute("SELECT order_item_id FROM order_items ORDER BY random() LIMIT 1")
    row = cur.fetchone()
    if not row:
        return
    order_item_id = row[0]
    cur.execute("DELETE FROM order_items WHERE order_item_id = %s", (order_item_id,))
    log.info(f"[DELETE] order_item_id={order_item_id}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=50, help="Số giao dịch cần sinh")
    parser.add_argument("--interval", type=float, default=1.0, help="Khoảng cách giữa các giao dịch (giây)")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    log.info(f"Bắt đầu sinh {args.count} giao dịch, cách nhau {args.interval}s...")

    for i in range(args.count):
        action = random.choices(
            ["create", "update", "delete"],
            weights=[0.6, 0.3, 0.1],
            k=1,
        )[0]

        try:
            if action == "create":
                create_order_with_items(cur)
            elif action == "update":
                update_random_order_status(cur)
            else:
                delete_random_order_item(cur)
        except Exception as e:
            log.error(f"Lỗi khi sinh giao dịch: {e}")

        time.sleep(args.interval)

    log.info("Hoàn tất sinh dữ liệu.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()