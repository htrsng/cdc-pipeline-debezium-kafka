"""
Sinh giao dịch mô phỏng thực tế: phân phối lệch Zipf + lưu lượng
không đều theo Poisson process.
"""

import argparse
import random
import time
import logging

import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("generator")

SOURCE_CONN_PARAMS = dict(
    host="localhost", port=5432, dbname="oltp_db", user="postgres", password="postgres",
)

STATUSES = ["PENDING", "CONFIRMED", "SHIPPED", "CANCELLED"]
ORDER_SIZE_CHOICES = [1, 2, 3, 4, 5, 8, 10]
ORDER_SIZE_WEIGHTS = [35, 30, 15, 10, 5, 3, 2]


def get_connection():
    conn = psycopg2.connect(**SOURCE_CONN_PARAMS)
    conn.autocommit = True
    return conn


def build_zipf_weights(n, skew=1.2):
    return [1 / ((i + 1) ** skew) for i in range(n)]


def load_ids_with_weights(cur, table, id_column, skew=1.2):
    cur.execute(f"SELECT {id_column} FROM {table} ORDER BY {id_column}")
    ids = [r[0] for r in cur.fetchall()]
    random.shuffle(ids)
    weights = build_zipf_weights(len(ids), skew=skew)
    return ids, weights


def create_order_with_items(cur, customer_ids, customer_weights, product_ids, product_weights, product_price_map):
    customer_id = random.choices(customer_ids, weights=customer_weights, k=1)[0]
    cur.execute(
        "INSERT INTO orders (customer_id, order_status) VALUES (%s, %s) RETURNING order_id",
        (customer_id, "PENDING"),
    )
    order_id = cur.fetchone()[0]

    num_items = random.choices(ORDER_SIZE_CHOICES, weights=ORDER_SIZE_WEIGHTS, k=1)[0]
    chosen_products = random.choices(product_ids, weights=product_weights, k=num_items)

    for product_id in chosen_products:
        quantity = random.randint(1, 5)
        unit_price = product_price_map[product_id]
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
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--mean-interval", type=float, default=0.5)
    parser.add_argument("--skew", type=float, default=1.2)
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    customer_ids, customer_weights = load_ids_with_weights(cur, "customers", "customer_id", args.skew)
    product_ids, product_weights = load_ids_with_weights(cur, "products", "product_id", args.skew)

    cur.execute("SELECT product_id, unit_price FROM products")
    product_price_map = {r[0]: r[1] for r in cur.fetchall()}

    log.info(f"Đã nạp {len(customer_ids)} khách hàng, {len(product_ids)} sản phẩm (skew={args.skew})")
    log.info(f"Bắt đầu sinh giao dịch trong {args.duration}s, khoảng cách trung bình {args.mean_interval}s...")

    start_time = time.time()
    count = 0

    while time.time() - start_time < args.duration:
        action = random.choices(["create", "update", "delete"], weights=[0.6, 0.3, 0.1], k=1)[0]
        try:
            if action == "create":
                create_order_with_items(cur, customer_ids, customer_weights, product_ids, product_weights, product_price_map)
            elif action == "update":
                update_random_order_status(cur)
            else:
                delete_random_order_item(cur)
            count += 1
        except Exception as e:
            log.error(f"Lỗi khi sinh giao dịch: {e}")

        sleep_time = random.expovariate(1 / args.mean_interval)
        time.sleep(min(sleep_time, args.mean_interval * 5))

    log.info(f"Hoàn tất sinh dữ liệu. Tổng cộng {count} giao dịch trong {args.duration}s.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()