# Retry Service

**Port mặc định:** `3003`

## Mô tả
Retry Service là "Trạm xá" của hệ thống. Bất kỳ lệnh nào gửi qua Facebook bị lỗi (do rớt mạng, Facebook sập, sai Token...) đều được Backend API vứt vào topic `send_failed`. Retry Service sẽ tóm lấy chúng và tìm cách "cứu chữa".
## Chức năng cốt lõi
1. **Phân loại lỗi thông minh (Error Filtering):**
   - Đọc kỹ thông báo lỗi. Nếu là những lỗi **Không thể khôi phục** (như sai Access Token, hoặc Account bị khóa), nó biết ngay có cố gắng cũng vô ích. Nó sẽ từ chối đếm giờ và ném thẳng tin nhắn đó vào Thùng rác (Dead Letter Queue).
   - Nếu là lỗi **Tạm thời** (Network Timeout), nó sẽ tiến hành thử lại.
2. **Thử lại giãn cách theo cấp số nhân (Exponential Backoff):**
   - Không dội bom dồn dập vào Facebook khi mạng đang lỗi.
   - Lần 1: Chờ 1 giây.
   - Lần 2: Chờ 2 giây.
   - Lần 3: Chờ 4 giây.
   - Công thức: `Delay = 1000ms * (2 ^ retry_count)`.
   - Lợi ích: Tránh bão hòa hệ thống (Thundering Herd) và giúp Facebook có thời gian phục hồi.
3. **Dead Letter Queue (DLQ):**
   - Sự kiện nào thử quá 3 lần mà vẫn thất bại sẽ được ném vào topic `dead_letter`.
   - Metric của Topic này được hệ thống Prometheus (cổng 9090) giám sát liên tục 15s/lần. Nếu có bất kỳ tin nhắn nào lọt vào DLQ, Alertmanager sẽ kích hoạt báo động đỏ gửi cho đội ngũ vận hành.
   
## Cách chạy
```bash
npm run dev:retry
```
