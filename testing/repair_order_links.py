"""Repair reused item IDs without deleting facts or changing CDC latency timestamps.

Default previews only. Stop the transaction generator while applying.
"""
import argparse
import psycopg2
from psycopg2.extras import execute_values


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    params = dict(host='localhost', user='postgres', password='postgres', connect_timeout=5)
    with psycopg2.connect(**params, port=5432, dbname='oltp_db') as source:
        with source.cursor() as cur:
            cur.execute('''SELECT i.order_item_id, i.order_id, i.product_id, i.quantity,
                i.unit_price, o.customer_id, o.order_status, o.order_date::date
                FROM order_items i JOIN orders o USING (order_id)''')
            rows = cur.fetchall()
    if not rows:
        raise SystemExit('Source empty: no repair performed.')
    with psycopg2.connect(**params, port=5433, dbname='dwh_db') as dwh:
        with dwh.cursor() as cur:
            cur.execute('''CREATE TEMP TABLE repair_source (
                item_id int, order_id int, product_id int, quantity int, price numeric,
                customer_id int, status text, order_date date) ON COMMIT DROP''')
            execute_values(cur, 'INSERT INTO repair_source VALUES %s', rows)
            # Only repair this known fault; skip rows whose other item values differ.
            condition = '''f.order_item_id=s.item_id AND NOT f.is_deleted
                AND f.order_id IS DISTINCT FROM s.order_id
                AND f.product_id=s.product_id AND f.quantity=s.quantity AND f.unit_price=s.price'''
            cur.execute('SELECT count(*) FROM fact_orders f JOIN repair_source s ON ' + condition)
            count = cur.fetchone()[0]
            print(f'Eligible order-link repairs: {count}')
            if not args.apply:
                return
            cur.execute('LOCK TABLE fact_orders IN SHARE ROW EXCLUSIVE MODE')
            cur.execute('''CREATE TABLE IF NOT EXISTS analytics.fact_order_link_repairs (
                repair_id bigserial PRIMARY KEY, repaired_at timestamp DEFAULT now(),
                old_row jsonb NOT NULL, new_order_id int NOT NULL)''')
            cur.execute('''INSERT INTO analytics.fact_order_link_repairs (old_row,new_order_id)
                SELECT to_jsonb(f), s.order_id FROM fact_orders f JOIN repair_source s ON ''' + condition)
            cur.execute('''INSERT INTO dim_date (date_id,full_date,day,month,quarter,year)
                SELECT DISTINCT to_char(order_date,'YYYYMMDD')::int,order_date,
                extract(day FROM order_date)::int, extract(month FROM order_date)::int,
                extract(quarter FROM order_date)::int, extract(year FROM order_date)::int
                FROM repair_source ON CONFLICT (date_id) DO NOTHING''')
            cur.execute('''UPDATE fact_orders f SET order_id=s.order_id,
                customer_id=s.customer_id, order_status=s.status,
                date_id=to_char(s.order_date,'YYYYMMDD')::int
                FROM repair_source s WHERE ''' + condition)
            print(f'Repaired {cur.rowcount}; previous rows saved in analytics.fact_order_link_repairs.')


if __name__ == '__main__':
    main()
