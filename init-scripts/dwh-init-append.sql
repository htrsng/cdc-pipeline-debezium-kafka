-- Bảng đệm (staging) lưu dữ liệu orders mới nhất, dùng để join khi
-- consumer xử lý message order_items (vì fact_orders cần customer_id
-- và order_status vốn nằm ở bảng orders, không có sẵn ở order_items)
CREATE TABLE stg_orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    order_status VARCHAR(20),
    order_date TIMESTAMP,
    is_deleted BOOLEAN DEFAULT false,
    updated_at TIMESTAMP DEFAULT now()
);