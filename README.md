# Hệ Thống Quản Lý Tương Tác Facebook Page (Microservices + Kafka + AI)

Đây là đồ án tích hợp Facebook Graph API, sử dụng kiến trúc Microservices hướng sự kiện (Event-driven) với Apache Kafka và tích hợp Google Gemini AI để phân tích ngôn ngữ tự nhiên.

##  Kiến trúc hệ thống

Hệ thống bao gồm 4 Service độc lập:

1. **Webhook Service (`:3001`)**: Chịu trách nhiệm nhận Webhook từ Facebook, xác thực chữ ký (HMAC-SHA256) và đẩy dữ liệu thô vào Kafka.
2. **Core Service (`:3002`)**: Bộ não của hệ thống. Nhận dữ liệu từ Kafka, lọc Spam, phân tích cảm xúc (Sentiment) bằng AI và đưa ra quyết định tự động.
3. **Backend API (`:3000`)**: Đóng vai trò Proxy giao tiếp với Facebook API. Thực thi các lệnh (Reply/Hide) từ Core Service đưa sang, có tích hợp Circuit Breaker và Idempotency.
4. **Retry Service (`:3003`)**: Quản lý các lệnh bị lỗi. Thực hiện Exponential Backoff (chờ 1s, 2s, 4s) và đẩy lỗi không thể khôi phục vào Dead Letter Queue (DLQ).

##  Hướng dẫn khởi chạy

### 1. Khởi động hạ tầng (Kafka, PostgreSQL, Prometheus)
Yêu cầu: Đã cài đặt Docker và Docker Compose.
```bash
docker compose up -d
```

### 2. Cài đặt biến môi trường
Tạo file `.env` ở thư mục gốc dựa trên file mẫu `.env.example` và điền các thông số:
- `PAGE_ACCESS_TOKEN`: Token của Facebook Page
- `FB_APP_SECRET`: App Secret để xác thực Webhook
- `GEMINI_API_KEY`: API Key của Google Gemini

### 3. Chạy các Services
Mở 4 dấu cộng (+) để tạo 4 Terminal riêng biệt ở thư mục gốc và chạy lần lượt:

**Terminal 1:**
```bash
npm run dev:webhook
```

**Terminal 2:**
```bash
npm run dev:core
```

**Terminal 3:**
```bash
npm run dev:backend
```

**Terminal 4:**
```bash
npm run dev:retry
```

### 4. Kết nối Webhook ra Internet
Vì file thực thi ngrok khá nặng nên không được lưu trữ trên Git. Bạn cần làm theo 2 bước sau:
1. Tải ngrok tại [https://ngrok.com/download](https://ngrok.com/download) và giải nén file `ngrok.exe` vào thư mục gốc của project.
2. Chạy lệnh sau để mở port:
```bash
./ngrok http 3001
```
Lấy link Ngrok dán vào phần cấu hình Webhook của Facebook Developer.

##  Giám sát (Monitoring)
- **Kafka UI**: `http://localhost:8080` (Xem các topic, message, consumer)
- **Prometheus Alerts**: `http://localhost:9090/alerts` (Giám sát cảnh báo DLQ)
- **Swagger API Docs**: `http://localhost:3000/api-docs`
