"""
Đo lường latency end-to-end của pipeline CDC, dựa trên 2 cột đã có sẵn
trong fact_orders: source_updated_at (thời điểm ghi ở nguồn) và
loaded_at (thời điểm nạp xong vào DWH).

Chạy: python latency_report.py
Chạy với toàn bộ dữ liệu (kể cả cũ): python latency_report.py --all
Chạy với khoảng thời gian tùy chỉnh: python latency_report.py --minutes 30
"""

import argparse
import statistics
import psycopg2

DWH_CONN_PARAMS = dict(
    host="localhost",
    port=5433,
    dbname="dwh_db",
    user="postgres",
    password="postgres",
)


def percentile(data, pct):
    data_sorted = sorted(data)
    k = (len(data_sorted) - 1) * (pct / 100)
    f = int(k)
    c = min(f + 1, len(data_sorted) - 1)
    if f == c:
        return data_sorted[f]
    return data_sorted[f] + (data_sorted[c] - data_sorted[f]) * (k - f)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--minutes", type=int, default=10,
        help="Chỉ tính latency của các bản ghi được nạp trong N phút gần nhất "
             "(loại bỏ nhiễu từ dữ liệu snapshot cũ). Mặc định: 10 phút.",
    )
    parser.add_argument(
        "--all", action="store_true",
        help="Bỏ qua bộ lọc thời gian, tính toàn bộ dữ liệu (kể cả snapshot cũ).",
    )
    args = parser.parse_args()

    conn = psycopg2.connect(**DWH_CONN_PARAMS)
    cur = conn.cursor()

    if args.all:
        cur.execute(
            """
            SELECT EXTRACT(EPOCH FROM (loaded_at - source_updated_at)) * 1000 AS latency_ms
            FROM fact_orders
            WHERE source_updated_at IS NOT NULL AND loaded_at IS NOT NULL
            ORDER BY loaded_at
            """
        )
    else:
        cur.execute(
            """
            SELECT EXTRACT(EPOCH FROM (loaded_at - source_updated_at)) * 1000 AS latency_ms
            FROM fact_orders
            WHERE source_updated_at IS NOT NULL
              AND loaded_at IS NOT NULL
              AND source_updated_at >= now() - (%s || ' minutes')::interval
            ORDER BY loaded_at
            """,
            (args.minutes,),
        )
    rows = cur.fetchall()
    # Ép kiểu Decimal (PostgreSQL trả về) sang float để tính toán được
    latencies = [float(r[0]) for r in rows if r[0] is not None]

    if not latencies:
        print("Không có bản ghi nào trong khoảng thời gian này. Thử --all hoặc tăng --minutes.")
        return

    print("=" * 50)
    print(f"TỔNG SỐ BẢN GHI ĐO ĐƯỢC: {len(latencies)}")
    print("-" * 50)
    print(f"Latency trung bình (avg): {statistics.mean(latencies):.2f} ms")
    print(f"Latency nhỏ nhất  (min):  {min(latencies):.2f} ms")
    print(f"Latency lớn nhất  (max):  {max(latencies):.2f} ms")
    print(f"Latency trung vị  (p50):  {percentile(latencies, 50):.2f} ms")
    print(f"Latency p95:              {percentile(latencies, 95):.2f} ms")
    print(f"Latency p99:              {percentile(latencies, 99):.2f} ms")
    print("=" * 50)

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()