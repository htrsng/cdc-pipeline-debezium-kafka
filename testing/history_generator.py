"""Append historical demo orders to OLTP; Debezium delivers them through CDC."""
import argparse
from datetime import date, datetime, time, timedelta
import random

import psycopg2


def make_orders(customers, products, days, daily_orders, end_date, seed):
    """Pure, reproducible generation; end_date is inclusive."""
    rng = random.Random(seed)
    customers = list(customers)
    products = list(products)
    rng.shuffle(customers)
    rng.shuffle(products)
    customer_weights = [1 / (i + 1) ** 1.1 for i in range(len(customers))]
    product_weights = [1 / (i + 1) ** 1.1 for i in range(len(products))]
    start = end_date - timedelta(days=days - 1)
    for day_index in range(days):
        day = start + timedelta(days=day_index)
        # Synthetic weekly pattern, gradual growth and monthly promotions.
        factor = (1.3 if day.weekday() >= 5 else 1.0)
        factor *= 0.8 + 0.4 * day_index / max(days - 1, 1)
        factor *= 1.6 if day.day in (10, 20) else 1.0
        count = max(1, round(daily_orders * factor * rng.uniform(0.8, 1.2)))
        for _ in range(count):
            ordered_at = datetime.combine(day, time(rng.randint(8, 22), rng.randrange(60), rng.randrange(60)))
            status = rng.choices(['SHIPPED', 'CONFIRMED', 'CANCELLED', 'PENDING'], [75, 15, 8, 2])[0]
            chosen = rng.choices(products, weights=product_weights, k=rng.randint(1, 5))
            # Merge repeated products into a single line in each order.
            items = {}
            for product_id, price in chosen:
                quantity = items.get(product_id, (0, price))[0] + rng.randint(1, 3)
                items[product_id] = (quantity, price)
            yield rng.choices(customers, weights=customer_weights, k=1)[0], ordered_at, status, items


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--days', type=int, default=180)
    parser.add_argument('--daily-orders', type=int, default=20)
    parser.add_argument('--end-date', type=date.fromisoformat, default=date.today() - timedelta(days=1))
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--apply', action='store_true', help='Write new orders; default only previews the count')
    parser.add_argument('--dsn', default='host=localhost port=5432 dbname=oltp_db user=postgres password=postgres connect_timeout=5')
    args = parser.parse_args()
    if args.days < 1 or args.daily_orders < 1:
        parser.error('--days and --daily-orders must be positive')
    if args.end_date >= date.today():
        parser.error('--end-date must be before today (complete historical days only)')
    with psycopg2.connect(args.dsn) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT customer_id FROM customers ORDER BY customer_id')
            customers = [row[0] for row in cur.fetchall()]
            cur.execute('SELECT product_id, unit_price FROM products ORDER BY product_id')
            products = cur.fetchall()
            if not customers or not products:
                parser.error('Seed customers and products first; do not use --wipe on existing data')
            orders = list(make_orders(customers, products, args.days, args.daily_orders, args.end_date, args.seed))
            print(f'{len(orders)} orders; {args.end_date - timedelta(days=args.days-1)} to {args.end_date}')
            if not args.apply:
                print('Preview only. Add --apply to append orders. Repeating --apply adds another batch.')
                return
            for customer, ordered_at, status, items in orders:
                # Business date is historical; write timestamp stays current so CDC latency remains meaningful.
                cur.execute('INSERT INTO orders (customer_id, order_date, order_status) VALUES (%s,%s,%s) RETURNING order_id', (customer, ordered_at, status))
                order_id = cur.fetchone()[0]
                cur.executemany('INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (%s,%s,%s,%s)', [(order_id, pid, qty, price) for pid, (qty, price) in items.items()])
    print(f'Committed {len(orders)} new orders to OLTP. Wait for the CDC consumer before refreshing Superset.')


if __name__ == '__main__':
    main()
