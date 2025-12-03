# Data Warehouse & ETL Project

## 📊 Tổng Quan Dự Án
Dự án xây dựng Data Warehouse và ETL pipeline để xử lý dữ liệu bán hàng sử dụng MySQL.

## 🏗️ Kiến Trúc
1. **Staging Area**: Lưu trữ dữ liệu thô
2. **Data Warehouse**: Mô hình hình sao (Star Schema)
   - Fact Table: sales_fact
   - Dimension Tables: dim_customer, dim_product, dim_date

## 🚀 Cài Đặt

### Yêu Cầu Hệ Thống
- Python 3.8+
- MySQL Server 8.0+
- Docker & Docker Compose (tùy chọn)

### Cài Đặt
```bash
# Clone repository
git clone <repo-url>
cd data-warehouse-project

# Cài đặt dependencies
pip install -r requirements.txt

# Khởi tạo database
bash setup_database.sh

# Chạy ETL pipeline
bash run_etl.sh