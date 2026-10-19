"""
CDC Consumer: đọc message Debezium từ Kafka, nạp vào Star Schema (DWH).
Chạy trên máy host (Windows), kết nối Kafka và Postgres DWH qua port publish ra ngoài.
"""

import json
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer
from decimal import Decimal
import psycopg2

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("cdc-consumer")

KAFKA_BOOTSTRAP_SERVERS = "127.0.0.1:29092"   # đổi từ "localhost:29092"
TOPICS = [
    "oltp.public.customers",
    "oltp.public.products",
    "oltp.public.orders",
    "oltp.public.order_items",
]
DWH_CONN_PARAMS = dict(
    host="localhost",
    port=5433,
    dbname="dwh_db",
    user="postgres",
    password="postgres",
)


def get_dwh_connection():
    conn = psycopg2.connect(**DWH_CONN_PARAMS)
    conn.autocommit = False
    return conn


def get_or_create_date_id(cur, dt: datetime) -> int:
    date_id = int(dt.strftime("%Y%m%d"))
    cur.execute(
        """
        INSERT INTO dim_date (date_id, full_date, day, month, quarter, year)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (date_id) DO NOTHING
        """,
        (date_id, dt.date(), dt.day, dt.month, (dt.month - 1) // 3 + 1, dt.year),
    )
    return date_id


def micros_to_datetime(value) -> datetime:
    return datetime.fromtimestamp(value / 1_000_000, tz=timezone.utc)


def handle_customer(cur, op, before, after):
    if op == "d":
        log.info(f"[customers] DELETE customer_id={before['customer_id']} -> giữ nguyên dim_customer")
        return
    cur.execute(
        """
        INSERT INTO dim_customer (customer_id, full_name, email, city)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (customer_id) DO UPDATE SET
            full_name = EXCLUDED.full_name, email = EXCLUDED.email, city = EXCLUDED.city
        """,
        (after["customer_id"], after["full_name"], after["email"], after.get("city")),
    )


def handle_product(cur, op, before, after):
    if op == "d":
        log.info(f"[products] DELETE product_id={before['product_id']} -> giữ nguyên dim_product")
        return
    cur.execute(
        """
        INSERT INTO dim_product (product_id, product_name, category, unit_price)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (product_id) DO UPDATE SET
            product_name = EXCLUDED.product_name, category = EXCLUDED.category, unit_price = EXCLUDED.unit_price
        """,
        (after["product_id"], after["product_name"], after.get("category"), after["unit_price"]),
    )


def handle_order(cur, op, before, after):
    if op == "d":
        cur.execute(
            "UPDATE stg_orders SET is_deleted = true, updated_at = now() WHERE order_id = %s",
            (before["order_id"],),
        )
        return
    order_date = micros_to_datetime(after["order_date"])
    cur.execute(
        """
        INSERT INTO stg_orders (order_id, customer_id, order_status, order_date, is_deleted)
        VALUES (%s, %s, %s, %s, false)
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id = EXCLUDED.customer_id, order_status = EXCLUDED.order_status,
            order_date = EXCLUDED.order_date, is_deleted = false, updated_at = now()
        """,
        (after["order_id"], after["customer_id"], after["order_status"], order_date),
    )
    cur.execute(
        "UPDATE fact_orders SET order_status = %s, customer_id = %s, date_id = %s WHERE order_id = %s",
        (after["order_status"], after["customer_id"], get_or_create_date_id(cur, order_date), after["order_id"]),
    )


def handle_order_item(cur, op, before, after, source_ts_ms, retry=True):
    if op == "d":
        cur.execute("DELETE FROM pending_order_items WHERE order_item_id = %s", (before["order_item_id"],))
        cur.execute(
            "UPDATE fact_orders SET is_deleted = true, loaded_at = now() WHERE order_item_id = %s",
            (before["order_item_id"],),
        )
        return

    order_item_id = after["order_item_id"]
    # Replace an older deferred update with the newest event for this key.
    if retry:
        cur.execute("DELETE FROM pending_order_items WHERE order_item_id = %s", (order_item_id,))
    order_id = after["order_id"]

    cur.execute(
        "SELECT customer_id, order_status, order_date FROM stg_orders WHERE order_id = %s",
        (order_id,),
    )
    row = cur.fetchone()

    if row is None:
        if retry:
            log.warning(f"[order_items] Chưa thấy order_id={order_id} -> đưa vào hàng đợi thử lại")
            cur.execute(
                "INSERT INTO pending_order_items (order_item_id, event) VALUES (%s, %s::jsonb) "
                "ON CONFLICT (order_item_id) DO UPDATE SET event = EXCLUDED.event",
                (after["order_item_id"], json.dumps(dict(op=op, before=before, after=after, source_ts_ms=source_ts_ms))),
            )
        return

    customer_id, order_status, order_date = row
    date_id = get_or_create_date_id(cur, order_date)
    total_amount = after["quantity"] * Decimal(str(after["unit_price"]))
    source_updated_at = micros_to_datetime(after["updated_at"])

    cur.execute(
        """
        INSERT INTO fact_orders (
            order_item_id, order_id, customer_id, product_id, date_id,
            quantity, unit_price, total_amount, order_status,
            source_updated_at, loaded_at, is_deleted
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), false)
        ON CONFLICT (order_item_id) DO UPDATE SET
            order_id = EXCLUDED.order_id,
            customer_id = EXCLUDED.customer_id, product_id = EXCLUDED.product_id,
            date_id = EXCLUDED.date_id, quantity = EXCLUDED.quantity,
            unit_price = EXCLUDED.unit_price, total_amount = EXCLUDED.total_amount,
            order_status = EXCLUDED.order_status, source_updated_at = EXCLUDED.source_updated_at,
            loaded_at = now(), is_deleted = false
        """,
        (order_item_id, order_id, customer_id, after["product_id"], date_id,
         after["quantity"], after["unit_price"], total_amount, order_status, source_updated_at),
    )


TABLE_HANDLERS = {
    "customers": handle_customer,
    "products": handle_product,
    "orders": handle_order,
    "order_items": handle_order_item,
}


def log_audit(cur, topic, offset, op, table_name, record_key, source_ts_ms):
    cur.execute(
        """
        INSERT INTO cdc_audit_log (kafka_topic, kafka_offset, operation, table_name, record_key, source_ts_ms)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (topic, offset, op, table_name, str(record_key), source_ts_ms),
    )


def process_message(cur, topic, key, value, offset):
    table_name = topic.split(".")[-1]

    if value is None:
        log.info(f"[{table_name}] Bỏ qua tombstone message, key={key}")
        return

    payload = value.get("payload", value)
    op = payload["op"]
    before = payload.get("before")
    after = payload.get("after")
    source_ts_ms = payload.get("source", {}).get("ts_ms")

    handler = TABLE_HANDLERS.get(table_name)
    if handler is None:
        log.warning(f"Không có handler cho bảng {table_name}, bỏ qua")
        return

    if table_name == "order_items":
        handler(cur, op, before, after, source_ts_ms)
    else:
        handler(cur, op, before, after)

    log_audit(cur, topic, offset, op, table_name, key, source_ts_ms)


def retry_pending_order_items(cur):
    cur.execute("SELECT order_item_id, event FROM pending_order_items ORDER BY order_item_id LIMIT 100")
    for item_id, event in cur.fetchall():
        handle_order_item(cur, **event, retry=False)
        cur.execute("SELECT 1 FROM stg_orders WHERE order_id = %s", (event["after"]["order_id"],))
        if cur.fetchone():
            cur.execute("DELETE FROM pending_order_items WHERE order_item_id = %s", (item_id,))


def main():
    log.info("Đang kết nối tới Kafka...")
    consumer = Consumer({
        "bootstrap.servers": "127.0.0.1:29092",
        "group.id": "dwh-loader-consumer-group-v6",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe(TOPICS)
    log.info(f"Đã subscribe {len(TOPICS)} topic. Đang chờ message...")

    conn = get_dwh_connection()
    cur = conn.cursor()

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                retry_pending_order_items(cur)
                conn.commit()
                continue

            if msg.error():
                log.error(f"Lỗi Kafka: {msg.error()}")
                continue

            key = json.loads(msg.key().decode("utf-8")) if msg.key() else None
            value = json.loads(msg.value().decode("utf-8")) if msg.value() else None

            try:
                process_message(cur, msg.topic(), key, value, msg.offset())
                retry_pending_order_items(cur)
                conn.commit()
                consumer.commit(message=msg, asynchronous=False)
            except Exception:
                conn.rollback()
                log.exception("Xử lý thất bại; dừng để không commit vượt qua event lỗi")
                raise

    except KeyboardInterrupt:
        log.info("Dừng consumer theo yêu cầu người dùng (Ctrl+C)")
    finally:
        cur.close()
        conn.close()
        consumer.close()
if __name__ == "__main__":
    main()
