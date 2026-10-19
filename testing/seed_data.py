"""
Sinh lại dữ liệu nền (customers, products) phong phú và thực tế hơn,
dùng thư viện Faker thay vì vài dòng viết tay.

CẢNH BÁO: Script này XÓA SẠCH dữ liệu cũ trong 4 bảng (TRUNCATE CASCADE)
rồi sinh lại từ đầu. Chỉ chạy khi muốn làm mới hoàn toàn dữ liệu demo.

Chạy: python seed_data.py --customers 100 --products 50 --wipe
"""

import argparse
import random
import psycopg2
from faker import Faker

fake = Faker("vi_VN")

SOURCE_CONN_PARAMS = dict(
    host="localhost", port=5432, dbname="oltp_db", user="postgres", password="postgres",
)

CATEGORIES = {
    "Thời trang": (50_000, 800_000),
    "Điện tử": (200_000, 15_000_000),
    "Gia dụng": (100_000, 3_000_000),
    "Sách": (30_000, 250_000),
    "Mỹ phẩm": (80_000, 1_200_000),
    "Thể thao": (100_000, 2_500_000),
}

PRODUCT_NAME_TEMPLATES = {
    "Thời trang": ["Áo thun", "Quần jean", "Áo khoác", "Váy", "Áo sơ mi", "Chân váy", "Áo len"],
    "Điện tử": ["Tai nghe", "Bàn phím", "Chuột không dây", "Loa bluetooth", "Sạc dự phòng", "Ốp lưng điện thoại", "Webcam"],
    "Gia dụng": ["Nồi cơm điện", "Máy xay sinh tố", "Bình giữ nhiệt", "Đèn bàn", "Quạt điện", "Bàn ủi"],
    "Sách": ["Tiểu thuyết", "Sách kỹ năng", "Truyện tranh", "Sách giáo khoa", "Sổ tay"],
    "Mỹ phẩm": ["Kem dưỡng da", "Son môi", "Sữa rửa mặt", "Nước hoa", "Kem chống nắng"],
    "Thể thao": ["Giày chạy bộ", "Áo thể thao", "Bình nước thể thao", "Thảm tập yoga", "Găng tay tập gym"],
}


def get_connection():
    conn = psycopg2.connect(**SOURCE_CONN_PARAMS)
    conn.autocommit = True
    return conn


def wipe_existing_data(cur):
    print("Đang xóa dữ liệu cũ (TRUNCATE CASCADE)...")
    cur.execute("TRUNCATE order_items, orders, products, customers RESTART IDENTITY CASCADE;")


def seed_customers(cur, n):
    print(f"Đang sinh {n} khách hàng...")
    cities = ["Ha Noi", "Ho Chi Minh", "Da Nang", "Hai Phong", "Can Tho", "Hue", "Nha Trang"]
    for _ in range(n):
        full_name = fake.name()
        email = fake.unique.email()
        city = random.choice(cities)
        cur.execute(
            "INSERT INTO customers (full_name, email, city) VALUES (%s, %s, %s)",
            (full_name, email, city),
        )


def seed_products(cur, n):
    print(f"Đang sinh {n} sản phẩm...")
    for _ in range(n):
        category = random.choice(list(CATEGORIES.keys()))
        min_price, max_price = CATEGORIES[category]
        name = random.choice(PRODUCT_NAME_TEMPLATES[category])
        name = f"{name} {fake.color_name()}"
        price = round(random.uniform(min_price, max_price), -3)
        cur.execute(
            "INSERT INTO products (product_name, category, unit_price) VALUES (%s, %s, %s)",
            (name, category, price),
        )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--customers", type=int, default=100)
    parser.add_argument("--products", type=int, default=50)
    parser.add_argument("--wipe", action="store_true")
    args = parser.parse_args()

    conn = get_connection()
    cur = conn.cursor()

    if args.wipe:
        wipe_existing_data(cur)

    seed_customers(cur, args.customers)
    seed_products(cur, args.products)

    print(f"\nHoàn tất: {args.customers} khách hàng, {args.products} sản phẩm.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()