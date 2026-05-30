# Core Service

**Port mặc định:** `3002`

## Mô tả
Core Service là "Bộ não" của toàn bộ hệ thống. Nhiệm vụ của nó là lắng nghe các luồng sự kiện thô từ Webhook, thực hiện kiểm duyệt, phân tích AI và ra quyết định tự động.

## Chức năng cốt lõi
1. **Lắng nghe Kafka Consumer:** 
   - Hút dữ liệu liên tục từ topic `raw_events`.
   - Lưu vết trạng thái vòng đời của sự kiện (`received` -> `processed` -> `replied`).
2. **Bảo vệ hệ thống (Anti-Spam & Rate Limit):**
   - **Rate Limiting:** Chặn các User có dấu hiệu gửi > 20 comment / phút.
   - **Lọc Spam:** Kiểm tra link độc hại, đếm số lần vi phạm.
   - **Blacklist nội bộ:** Tự động đưa User vi phạm 3 lần vào Blacklist và bỏ qua các sự kiện sau này của User đó.
3. **Phân tích Cảm xúc bằng AI (Sentiment Analysis):**
   - Tích hợp **Google Gemini 2.0 Flash** để đọc hiểu Tiếng Việt.
   - Trích xuất tự động Ý định (`intent`) và Cảm xúc (`sentiment`).
   - Có cơ chế **Rule-based NLP Fallback** (Dự phòng bằng từ khóa) trong trường hợp API Gemini bị lỗi hoặc hết Quota.
4. **Tự động hóa (Automation Rules):**
   - Dựa vào kết quả AI để ra quyết định: Khách khen -> Gửi lệnh Cảm ơn; Khách chê -> Gửi lệnh Xin lỗi; Hỏi giá -> Chuyển hướng.
   - Đẩy quyết định (Command) vào topic `reply_commands` để Backend thực thi.

## Cách chạy
```bash
npm run dev:core
```


