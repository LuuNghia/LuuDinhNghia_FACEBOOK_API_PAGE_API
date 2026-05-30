# Webhook Service

**Port mặc định:** `3001`

## Mô tả
Đây là dịch vụ "Gác cổng" (Entry-point) của hệ thống. Webhook Service trực tiếp nhận các sự kiện (bình luận, tin nhắn mới) được đẩy về từ Facebook.

## Chức năng cốt lõi
1. **Xác thực bảo mật (Security):** 
   - Không nhận dữ liệu rác. Mọi request gửi đến đều phải có header `X-Hub-Signature-256`.
   - Sử dụng `crypto.createHmac('sha256')` và `FB_APP_SECRET` để băm lại Payload và đối chiếu. Chỉ xử lý khi mã băm khớp 100%.
2. **Chuẩn hóa dữ liệu (Normalization):**
   - Bóc tách cấu trúc JSON phức tạp của Facebook thành một Schema chuẩn nội bộ (chứa `event_id`, `user_id`, `content`).
3. **Kafka Producer:**
   - Đẩy sự kiện đã chuẩn hóa vào topic `raw_events` của Kafka để các service khác xử lý.
   - Trả về mã HTTP `200 OK` cho Facebook ngay lập tức để tránh tình trạng Timeout (Facebook sẽ khóa Webhook nếu Timeout quá lâu).

## API Endpoints
- `GET /webhook`: Endpoint để Facebook xác minh (Verify Token) khi thiết lập Webhook.
- `POST /webhook`: Endpoint nhận luồng sự kiện (Event Stream) từ Facebook.
- `GET /health`: Kiểm tra trạng thái sống của Service.

## Cách chạy
```bash
npm run dev:webhook
```