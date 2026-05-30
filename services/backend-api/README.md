# Backend API

**Port mặc định:** `3000`

## Mô tả
Backend API là nơi duy nhất trong hệ thống có quyền kết nối trực tiếp (HTTP Request) tới Facebook Graph API. Nó hoạt động như một Proxy cho Frontend, đồng thời là một Kafka Consumer nhận lệnh thực thi từ Core Service.

## Chức năng cốt lõi
1. **API Proxy:**
   - Cung cấp các RESTful API chuẩn mực (`GET /api/page/:pageId/posts`) để Frontend hoặc Admin Dashboard gọi vào, giấu đi sự phức tạp của Facebook Graph API.
   - Chuẩn hóa lỗi: Bắt các lỗi từ Facebook và bọc thành JSON Response chuẩn (`400 Bad Request`, `401 Unauthorized`) tránh sập server.
2. **Idempotent Consumer:**
   - Mỗi lệnh thực thi (Reply/Hide) đều đi kèm một `command_id` duy nhất.
   - Kết nối với Database (PostgreSQL) để kiểm tra `idempotency_keys`. Nếu Kafka lỡ gửi trùng 2 lệnh, hệ thống sẽ phát hiện và bỏ qua lần thứ 2, tránh việc bình luận 2 lần trên Fanpage.
3. **Circuit Breaker (Cầu dao tự ngắt):**
   - Giám sát sức khỏe của Facebook API.
   - Nếu Facebook sập hoặc lỗi mạng liên tục 10 lần, Cầu dao sẽ ngắt mạch (`OPEN`) trong 30 giây. Các yêu cầu trong thời gian này bị từ chối ngay tại cửa, bảo vệ tài nguyên hệ thống. Sau 30s sẽ hé cửa (`HALF-OPEN`) để thử nghiệm lại.

## Swagger API Documentation
- Giao diện Swagger UI được tích hợp sẵn tại: `http://localhost:3000/api-docs`

## Cách chạy
```bash
npm run dev:backend
```
