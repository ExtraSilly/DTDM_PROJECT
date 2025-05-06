# Dự án Xử lý và Upload Dữ liệu lên BigQuery

Dự án này bao gồm các script Python để xử lý và upload dữ liệu từ các file CSV lên Google BigQuery. Dự án được thiết kế để xử lý dữ liệu vé xem phim và các thông tin liên quan.

## Cấu trúc thư mục

```
.
├── data/                      # Thư mục chứa dữ liệu gốc
│   ├── Ticket.csv            # Dữ liệu vé gốc
│   ├── Customer.csv          # Dữ liệu khách hàng gốc
│   └── Film.csv              # Dữ liệu phim gốc
├── db_schema_ticket_data/     # Thư mục chứa các file CSV dữ liệu đã xử lý
│   ├── tickets.csv           # Dữ liệu vé
│   ├── sales.csv             # Dữ liệu bán hàng
│   ├── films.csv             # Dữ liệu phim
│   ├── rooms.csv             # Dữ liệu phòng chiếu
│   ├── film_room.csv         # Mối quan hệ phim-phòng
│   ├── ticket_types.csv      # Các loại vé
│   ├── seat_types.csv        # Các loại ghế
│   ├── hourly_analysis.csv   # Phân tích theo giờ
│   ├── daily_analysis.csv    # Phân tích theo ngày
│   ├── monthly_analysis.csv  # Phân tích theo tháng
│   ├── film_detailed_analysis.csv  # Phân tích chi tiết phim
│   └── schema_documentation.txt    # Tài liệu mô tả schema
├── processed_ticket_data/     # Thư mục chứa dữ liệu đã xử lý từ Kafka
│   ├── processed_tickets.csv  # Dữ liệu vé đã xử lý
│   ├── film_distribution.csv  # Phân bố phim
│   ├── ticket_type_distribution.csv  # Phân bố loại vé
│   ├── seat_type_analysis.csv # Phân tích loại ghế
│   └── ticket_summary.csv     # Tổng hợp dữ liệu vé
├── scripts/                   # Thư mục chứa các script xử lý
│   ├── upload.py             # Script upload lên BigQuery
│   ├── ticket_to_kafka_producer.py  # Script gửi dữ liệu vé lên Kafka
│   ├── customer_to_kafka_product.py # Script gửi dữ liệu khách hàng lên Kafka
│   ├── film_producer.py      # Script gửi dữ liệu phim lên Kafka
│   ├── ticket_kafka_consumer.py     # Script nhận dữ liệu từ Kafka
│   ├── ticket_schema_convert_db.py  # Script chuyển đổi schema
│   └── ticket_import_postgre.py     # Script import vào PostgreSQL
├── logs/                      # Thư mục chứa các file log
│   ├── kafka_producer.log    # Log của Kafka producer
│   ├── kafka_consumer.log    # Log của Kafka consumer
│   ├── schema_converter.log  # Log của schema converter
│   ├── postgres_import.log   # Log của PostgreSQL import
│   └── bigquery_upload.log   # Log của BigQuery upload
├── config/                    # Thư mục chứa các file cấu hình
│   ├── kafka_config.json     # Cấu hình Kafka
│   ├── postgres_config.json  # Cấu hình PostgreSQL
│   └── bigquery_config.json  # Cấu hình BigQuery
├── tests/                    # Thư mục chứa các test case
│   ├── test_kafka.py        # Test Kafka
│   ├── test_postgres.py     # Test PostgreSQL
│   └── test_bigquery.py     # Test BigQuery
├── docs/                     # Thư mục chứa tài liệu
│   ├── api_docs.md          # Tài liệu API
│   ├── data_flow.md         # Sơ đồ luồng dữ liệu
│   └── troubleshooting.md   # Hướng dẫn xử lý lỗi
├── docker-compose.yml        # Cấu hình Docker cho Kafka và Zookeeper
├── requirements.txt          # Danh sách các thư viện Python cần thiết
├── .env                      # File chứa các biến môi trường
├── .gitignore               # File cấu hình Git
└── README.md                # Tài liệu hướng dẫn này
```

## Yêu cầu hệ thống

- Python 3.7 trở lên
- Google Cloud SDK
- Docker và Docker Compose
- PostgreSQL
- Thư viện Python:
  - google-cloud-bigquery
  - pandas
  - kafka-python
  - psycopg2
  - python-dotenv
  - pathlib

## Cài đặt

1. Cài đặt Docker và Docker Compose:
```bash
# Windows: Tải và cài đặt Docker Desktop từ https://www.docker.com/products/docker-desktop
# Linux: 
sudo apt-get update
sudo apt-get install docker.io docker-compose
```

2. Tạo môi trường ảo Python:
```bash
python -m venv venv
```

3. Kích hoạt môi trường ảo:
- Windows:
```bash
venv\Scripts\activate
```
- Linux/Mac:
```bash
source venv/bin/activate
```

4. Cài đặt các thư viện cần thiết:
```bash
pip install -r requirements.txt
```

5. Build docker-compose
```bash
docker-compose up --build -d
```

6. Khởi động Kafka và Zookeeper:
```bash
docker-compose up -d
docker-compose up -d kafka
```

## Cách chạy

1. Đảm bảo bạn đã cài đặt và cấu hình đầy đủ như hướng dẫn ở trên.

2. Chạy các script theo thứ tự:
```bash
# Bước 1: Gửi dữ liệu lên Kafka
*** Tạo 3 topic để chứa dữ liệu trên kafka ***
docker exec kafka kafka-topics --create --topic customer  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic ticket  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka kafka-topics --create --topic film  --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 
python ticket_to_kafka_producer.py
python customer_to_kafka_product.py
python film_producer.py

# Bước 2: Xử lý dữ liệu từ Kafka
python ticket_kafka_consumer.py

# Bước 3: Chuyển đổi schema
python ticket_schema_convert_db.py

# Bước 4: Import vào PostgreSQL
Phải tạo bảng trước trong PostgreSql và cập nhật ở file .env và cả file py bên dưới
python ticket_import_postgre.py

# Bước 5: Upload lên BigQuery
Lưu ý để chạy được bước 5 cần phải có file .json từ google cloud
Các bước thực hiệndocker-compose up --build -d
```

6. Khởi động Kafka và Zookeeper:
```bash
docker-compose up -d
docker-compose up -d kafka
```

## Cách chạy

1. Đảm bảo bạn đã cài đặt và cấu hình đầy đủ như hướng dẫn ở trên.

2. Chạy các script theo thứ tự:
```bash
# Bước 1: Gửi dữ liệu lên Kafka
python ticket_to_kafka_producer.py
python customer_to_kafka_product.py
python film_producer.py

# Bước 2: Xử lý dữ liệu từ Kafka
python ticket_kafka_consumer.py

# Bước 3: Chuyển đổi schema
python ticket_schema_convert_db.py

# Bước 4: Import vào PostgreSQL
python ticket_import_postgre.py

# Bước 5: Upload lên BigQuery
Lưu ý để chạy được bước 5 cần phải có file .json từ google cloud
Các bước thực hiện
+ Tạo project
+ Enable Bigquery
+ Vào Credentials và create credentials
+ Trong IAM & Admin, chọn Service Accounts.
+ Tạo một Service Account mới và cấp quyền BigQuery Admin cho tài khoản này.
+ Tải về key JSON và lưu vào nơi bảo mật.
+ Chạy các lệnh sau:
+ export GOOGLE_APPLICATION_CREDENTIALS="path_to_your_service_account_key.json"
+ echo $env:GOOGLE_APPLICATION_CREDENTIALS
+ Sau đó chạy lệnh bên dưới:
python upload.py
```

## Mô tả các thành phần

### 1. Kafka Producer
- `ticket_to_kafka_producer.py`: Gửi dữ liệu vé lên Kafka
- `customer_to_kafka_product.py`: Gửi dữ liệu khách hàng lên Kafka
- `film_producer.py`: Gửi dữ liệu phim lên Kafka

### 2. Kafka Consumer
- `ticket_kafka_consumer.py`: Nhận và xử lý dữ liệu từ Kafka
- Tạo các báo cáo phân tích
- Lưu dữ liệu đã xử lý vào thư mục `processed_ticket_data`

### 3. Schema Converter
- `ticket_schema_convert_db.py`: Chuyển đổi dữ liệu sang schema phù hợp
- Tạo các bảng dữ liệu trong thư mục `db_schema_ticket_data`

### 4. PostgreSQL Importer
- `ticket_import_postgre.py`: Import dữ liệu vào PostgreSQL
- Tạo và cập nhật các bảng
- Kiểm tra tính toàn vẹn dữ liệu

### 5. BigQuery Uploader
- `upload.py`: Upload dữ liệu lên Google BigQuery
- Xử lý đặc biệt cho các bảng `sales` và `film_detailed_analysis`
- Kiểm tra và báo cáo kết quả upload

## Mô tả các bảng dữ liệu

### 1. tickets
- Chứa thông tin chi tiết về các vé đã bán
- Các trường chính: orderid, customerid, ticketcode, date, time, slot, room, film, slot_type, ticket_type, ticket_price, popcorn, processed_at

### 2. sales
- Ghi lại thông tin về các giao dịch bán hàng
- Các trường chính: orderid, cashier, saledate, total
- Định dạng datetime được xử lý tự động để phù hợp với BigQuery

### 3. films
- Danh sách các phim
- Các trường chính: film_id, film

### 4. rooms
- Thông tin về các phòng chiếu
- Các trường chính: room_id, room

### 5. film_room
- Mối quan hệ giữa phim và phòng chiếu
- Các trường chính: film_id, room_id

### 6. ticket_types
- Các loại vé khác nhau
- Các trường chính: type_id, ticket_type, count

### 7. seat_types
- Các loại ghế trong rạp
- Các trường chính: seat_type_id, slot_type

### 8. hourly_analysis
- Phân tích doanh thu theo giờ
- Các trường chính: hour, ticket_count, revenue

### 9. daily_analysis
- Phân tích doanh thu theo ngày
- Các trường chính: date, ticket_count, revenue

### 10. monthly_analysis
- Phân tích doanh thu theo tháng
- Các trường chính: month, ticket_count, revenue

### 11. film_detailed_analysis
- Phân tích chi tiết về từng phim
- Các trường chính: 
  - film: Tên phim
  - total_tickets: Tổng số vé
  - total_customers: Tổng số khách hàng
  - tickets_per_customer: Số vé trung bình mỗi khách
  - total_revenue: Tổng doanh thu
  - avg_ticket_price: Giá vé trung bình
  - revenue_per_ticket: Doanh thu trung bình mỗi vé
  - single_seats: Số ghế đơn
  - double_seats: Số ghế đôi
  - total_seats: Tổng số ghế
- Dữ liệu ghế được xử lý đặc biệt để phân biệt ghế đơn và ghế đôi

## Lưu ý

- Đảm bảo bạn có quyền truy cập vào dự án Google Cloud và dataset BigQuery
- Kiểm tra kết nối internet trước khi chạy script
- Script sẽ tự động xóa và tạo lại các bảng trong BigQuery
- Các file CSV phải được đặt đúng trong thư mục `db_schema_ticket_data`
- Đảm bảo định dạng dữ liệu trong các file CSV phù hợp với schema đã định nghĩa
- Dữ liệu datetime trong file sales.csv sẽ được tự động chuyển đổi sang định dạng phù hợp
- Dữ liệu ghế trong film_detailed_analysis sẽ được tự động phân tích để xác định ghế đơn và ghế đôi
- Đảm bảo Kafka và Zookeeper đang chạy trước khi thực hiện các bước xử lý dữ liệu
- Kiểm tra kết nối PostgreSQL trước khi import dữ liệu

## Xử lý lỗi

Nếu gặp lỗi khi chạy script:
1. Kiểm tra kết nối internet
2. Xác minh thông tin xác thực Google Cloud
3. Kiểm tra định dạng dữ liệu trong các file CSV
4. Xem thông báo lỗi chi tiết trong console
5. Đảm bảo các thư viện đã được cài đặt đầy đủ
6. Kiểm tra định dạng datetime trong file sales.csv
7. Xác minh cấu trúc dữ liệu ghế trong film_detailed_analysis.csv
8. Kiểm tra trạng thái của Kafka và Zookeeper
9. Xác minh kết nối và cấu hình PostgreSQL
10. Kiểm tra quyền truy cập vào các thư mục và file 
