"""
Kiểm chứng tính đúng đắn dữ liệu (data reconciliation) giữa Postgres
nguồn (oltp_db) và Data Warehouse (dwh_db).

Chạy: python reconciliation.py
"""

import psycopg2

SOURCE_CONN_PARAMS = dict(
    host="localhost", port=5432, dbname="oltp_db", user="postgres", password="postgres",
)
DWH_CONN_PARAMS = dict(
    host="localhost", port=5433, dbname="dwh_db", user="postgres", password="postgres",
)


def get_source_conn():
    return psycopg2.connect(**SOURCE_CONN_PARAMS)


def get_dwh_conn():
    return psycopg2.connect(**DWH_CONN_PARAMS)


def check_aggregate(src_cur, dwh_cur):
    print("=" * 60)
    print("1. KIỂM TRA TỔNG HỢP (AGGREGATE CHECK)")
    print("=" * 60)

    src_cur.execute(
        "SELECT COUNT(*), COALESCE(SUM(quantity), 0), COALESCE(SUM(quantity * unit_price), 0) FROM order_items"
    )
    src_count, src_qty, src_total = src_cur.fetchone()

    dwh_cur.execute(
        "SELECT COUNT(*), COALESCE(SUM(quantity), 0), COALESCE(SUM(total_amount), 0) FROM fact_orders WHERE is_deleted = false"
    )
    dwh_count, dwh_qty, dwh_total = dwh_cur.fetchone()

    print(f"\n[order_items -> fact_orders]")
    print(f"  Số dòng      : nguồn={src_count:<6} | DWH (chưa xóa)={dwh_count:<6} | {'OK' if src_count == dwh_count else 'LECH'}")
    print(f"  Tổng quantity: nguồn={src_qty:<6} | DWH={dwh_qty:<6} | {'OK' if src_qty == dwh_qty else 'LECH'}")
    print(f"  Tổng tiền    : nguồn={float(src_total):<12.2f} | DWH={float(dwh_total):<12.2f} | {'OK' if float(src_total) == float(dwh_total) else 'LECH'}")

    dwh_cur.execute("SELECT order_item_id FROM fact_orders WHERE is_deleted = true")
    deleted_in_dwh = {r[0] for r in dwh_cur.fetchall()}
    src_cur.execute("SELECT order_item_id FROM order_items")
    existing_in_src = {r[0] for r in src_cur.fetchall()}
    wrongly_marked = deleted_in_dwh & existing_in_src

    print(f"\n[Kiểm tra cờ is_deleted]")
    print(f"  Số order_item DWH đánh dấu xóa  : {len(deleted_in_dwh)}")
    print(f"  Đánh dấu SAI (đã xóa nhưng nguồn còn): {len(wrongly_marked)} {'OK' if not wrongly_marked else 'LOI: ' + str(wrongly_marked)}")


def check_row_level(src_cur, dwh_cur):
    print("\n" + "=" * 60)
    print("2. KIỂM TRA CHI TIẾT TỪNG DÒNG (ROW-LEVEL CHECK)")
    print("=" * 60)

    src_cur.execute("SELECT order_item_id, order_id, product_id, quantity, unit_price FROM order_items")
    src_rows = {r[0]: r for r in src_cur.fetchall()}

    dwh_cur.execute(
        "SELECT order_item_id, order_id, product_id, quantity, unit_price FROM fact_orders WHERE is_deleted = false"
    )
    dwh_rows = {r[0]: r for r in dwh_cur.fetchall()}

    missing_in_dwh = set(src_rows.keys()) - set(dwh_rows.keys())
    extra_in_dwh = set(dwh_rows.keys()) - set(src_rows.keys())
    mismatched = []

    for oid in set(src_rows.keys()) & set(dwh_rows.keys()):
        src_row = src_rows[oid]
        dwh_row = dwh_rows[oid]
        if (src_row[1], src_row[2], src_row[3], float(src_row[4])) != (dwh_row[1], dwh_row[2], dwh_row[3], float(dwh_row[4])):
            mismatched.append((oid, src_row, dwh_row))

    print(f"\nTổng số order_item đang hoạt động ở nguồn : {len(src_rows)}")
    print(f"Thiếu ở DWH (có ở nguồn, không có ở DWH)  : {len(missing_in_dwh)} {'OK' if not missing_in_dwh else 'LOI: ' + str(missing_in_dwh)}")
    print(f"Thừa ở DWH (không còn ở nguồn nhưng chưa đánh dấu xóa): {len(extra_in_dwh)} {'OK' if not extra_in_dwh else 'LOI: ' + str(extra_in_dwh)}")
    print(f"Sai lệch giá trị: {len(mismatched)} {'OK' if not mismatched else 'LOI'}")

    if mismatched:
        print("\nChi tiết các dòng sai lệch:")
        for oid, src_row, dwh_row in mismatched:
            print(f"  order_item_id={oid}")
            print(f"    Nguồn : order_id={src_row[1]}, product_id={src_row[2]}, qty={src_row[3]}, price={src_row[4]}")
            print(f"    DWH   : order_id={dwh_row[1]}, product_id={dwh_row[2]}, qty={dwh_row[3]}, price={dwh_row[4]}")


def check_dimension_consistency(src_cur, dwh_cur):
    print("\n" + "=" * 60)
    print("3. KIỂM TRA DIMENSION (customers, products)")
    print("=" * 60)

    for table, dim_table, pk in [("customers", "dim_customer", "customer_id"), ("products", "dim_product", "product_id")]:
        src_cur.execute(f"SELECT {pk} FROM {table}")
        src_ids = {r[0] for r in src_cur.fetchall()}
        dwh_cur.execute(f"SELECT {pk} FROM {dim_table}")
        dwh_ids = {r[0] for r in dwh_cur.fetchall()}

        missing = src_ids - dwh_ids
        print(f"\n[{table} -> {dim_table}]")
        print(f"  Số dòng nguồn: {len(src_ids)} | Số dòng DWH: {len(dwh_ids)}")
        print(f"  Thiếu ở DWH (có ở nguồn, chưa có ở dim): {len(missing)} {'OK' if not missing else 'LOI: ' + str(missing)}")
        print("  (Lưu ý: dim table giữ cả bản ghi đã bị xóa ở nguồn - đây là chủ đích thiết kế, không phải lỗi)")


def main():
    src_conn = get_source_conn()
    dwh_conn = get_dwh_conn()
    src_cur = src_conn.cursor()
    dwh_cur = dwh_conn.cursor()

    check_aggregate(src_cur, dwh_cur)
    check_row_level(src_cur, dwh_cur)
    check_dimension_consistency(src_cur, dwh_cur)

    print("\n" + "=" * 60)
    print("HOAN TAT KIEM CHUNG")
    print("=" * 60)

    src_cur.close()
    dwh_cur.close()
    src_conn.close()
    dwh_conn.close()


if __name__ == "__main__":
    main()