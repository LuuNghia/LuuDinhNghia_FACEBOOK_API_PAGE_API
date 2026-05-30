require('dotenv').config({ path: require('path').resolve(__dirname, '../../../.env') });
const express = require('express');
const { Kafka } = require('kafkajs');
const { checkSpam }       = require('./rules/checkSpam');
const { analyzeWithGemini } = require('./ai/gemini');
const { TOPICS, RETRY_MAX } = require('../../../shared/constants');
const logger = require('../../../shared/logger');

const SERVICE = 'core-service';
const app     = express();
const PORT    = process.env.CORE_PORT    || 3002;
const BROKER  = process.env.KAFKA_BROKER || 'localhost:9092';

const kafka    = new Kafka({ clientId: SERVICE, brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'core-processing-group' });
const producer = kafka.producer();

// Blacklist nội bộ (in-memory)
const blacklist = new Set();

// Rate Limit (in-memory): { count, startTime }
const rateLimitMap = new Map();

function checkRateLimit(userId) {
    if (userId === 'N/A') return false;
    const now = Date.now();
    let record = rateLimitMap.get(userId);
    if (!record || now - record.startTime > 60000) {
        record = { count: 1, startTime: now };
    } else {
        record.count++;
    }
    rateLimitMap.set(userId, record);
    return record.count >= 20; // Giới hạn 20 bình luận / 1 phút
}

// ── Health check ────────────────────────────────────────────────
app.get('/health', (_, res) => res.json({
    status: 'ok', service: SERVICE, port: PORT,
    blacklisted: blacklist.size,
    rateLimited: rateLimitMap.size,
    timestamp: new Date().toISOString(),
}));

// ── Gửi reply_command tới Backend API ───────────────────────────
async function dispatch(eventId, action, payload = {}) {
    if (!eventId) {
        logger.warn(SERVICE, `[dispatch] Thiếu event_id, bỏ qua lệnh [${action}].`);
        return;
    }
    const command = {
        command_id:  `${eventId}_${action}_${Date.now()}`,
        action,
        comment_id:  eventId,
        reason:      payload.reason || '',
        reply_text:  payload.reply_text || null,
        timestamp:   new Date().toISOString(),
    };
    await producer.send({ topic: TOPICS.REPLY_COMMANDS, messages: [{ value: JSON.stringify(command) }] });
    logger.success(SERVICE, `[reply_commands] Lệnh [${action}] | comment: ${eventId}`);
}

// ── Gửi vào manual_review queue ─────────────────────────────────
async function sendToReview(event, reason, aiResult = null) {
    await producer.send({
        topic: TOPICS.MANUAL_REVIEW,
        messages: [{ value: JSON.stringify({ event, reason, ai_result: aiResult, flagged_at: new Date().toISOString() }) }],
    });
    logger.warn(SERVICE, `[manual_review] Gửi sự kiện vào hàng chờ duyệt thủ công: ${reason}`);
}

// ── Pipeline xử lý chính ────────────────────────────────────────
async function processEvent(event) {
    const eventId = event.event_id || null;
    const userId  = event.user_id  || 'N/A';
    const content = event.content  || '';

    logger.info(SERVICE, `[Status: received] ID=${eventId} | User=${event.sender_name} | "${content}"`);

    // 0. Rate Limiting (20 comments / 1 phút)
    if (checkRateLimit(userId)) {
        logger.warn(SERVICE, `[Status: rate_limited] User ${userId} gửi quá 20 bình luận/phút. Chuyển sang pending_review.`);
        await sendToReview(event, 'rate_limit_exceeded');
        return 'pending_review';
    }

    // 1. Kiểm tra Blacklist
    if (blacklist.has(userId)) {
        logger.warn(SERVICE, `[Status: blocked] User ${userId} trong blacklist. Tự động ẩn comment.`);
        await dispatch(eventId, 'hide', { reason: 'user_in_blacklist' });
        return 'blocked';
    }

    // 2. Phát hiện Spam
    const spam = checkSpam(userId, content);
    if (spam.isSpam) {
        if (spam.type === 'malicious_link') {
            logger.warn(SERVICE, `[Status: spam] Link độc hại → ẩn ngay + manual_review`);
            await dispatch(eventId, 'hide', { reason: 'malicious_link' });
            await sendToReview(event, 'malicious_link');
            return 'spam_manual_review';
        }
        if (spam.type === 'light_spam') {
            logger.warn(SERVICE, `[Status: spam] Spam nhẹ → ẩn bình luận ngay`);
            await dispatch(eventId, 'hide', { reason: 'light_spam' });
            return 'spam_hidden';
        }
        if (spam.type === 'repeated_3_times') {
            logger.warn(SERVICE, `[Status: spam] Lặp lại 3 lần → BLACKLIST nội bộ và ẩn comment`);
            blacklist.add(userId);
            await dispatch(eventId, 'hide', { reason: 'repeated_3_times' });
            return 'blacklisted';
        }
    }

    // 3. Phân tích AI (intent + sentiment)
    const ai = await analyzeWithGemini(content);
    logger.info(SERVICE, `[Status: ai_analyzed] intent=${ai.intent} | sentiment=${ai.sentiment}`);

    // 4. Ra quyết định tự động
    if (ai.sentiment === 'tiêu cực' || ai.intent === 'khiếu nại') {
        logger.info(SERVICE, `[Status: needs_review] Tiêu cực/khiếu nại → gửi manual review`);
        await sendToReview(event, 'negative_sentiment', ai);
        // Gửi reply xin lỗi tự động
        await dispatch(eventId, 'reply', { reason: 'auto_reply_apology', reply_text: 'Rất xin lỗi vì trải nghiệm chưa tốt, chúng tôi sẽ kiểm tra ngay.' });
        return 'needs_review';
    }

    if (ai.intent === 'hỏi giá') {
        logger.info(SERVICE, `[Status: replied] Hỏi giá → gửi reply`);
        await dispatch(eventId, 'reply', { reason: 'auto_reply_price', reply_text: 'Bạn vui lòng nhắn tin cho shop để được tư vấn giá chính xác nhất!' });
        return 'replied';
    }

    // Tích cực → Cảm ơn người dùng (theo đúng yêu cầu 5.2)
    if (ai.sentiment === 'tích cực' || ai.intent === 'khen') {
        logger.info(SERVICE, `[Status: replied] Tích cực/Khen → gửi cảm ơn`);
        await dispatch(eventId, 'reply', { reason: 'auto_reply_thanks', reply_text: 'Cảm ơn bạn đã ủng hộ shop! Chúc bạn mua sắm vui vẻ.' });
        return 'replied';
    }

    logger.info(SERVICE, `[Status: processed] Xử lý hoàn tất.`);
    return 'processed';
}

// ── Kafka Consumer ───────────────────────────────────────────────
const startService = async () => {
    await consumer.connect();
    await producer.connect();
    // fromBeginning: false → chỉ nhận event MỚI, tránh xử lý lại event cũ
    await consumer.subscribe({ topic: TOPICS.RAW_EVENTS, fromBeginning: false });
    logger.success(SERVICE, `Đang lắng nghe topic [${TOPICS.RAW_EVENTS}]...`);

    await consumer.run({
        // Xử lý tuần tự → không bỏ sót event khi tải cao
        eachMessage: async ({ message }) => {
            let event;
            try { event = JSON.parse(message.value.toString()); }
            catch { logger.error(SERVICE, 'Message JSON không hợp lệ, bỏ qua.'); return; }

            let finalState;
            try {
                finalState = await processEvent(event);
            } catch (err) {
                logger.error(SERVICE, 'Lỗi xử lý event.', err);
                finalState = 'failed';
            }

            // Nếu failed → push send_failed để Retry Service xử lý
            if (finalState === 'failed') {
                await producer.send({
                    topic: TOPICS.SEND_FAILED,
                    messages: [{ value: JSON.stringify({
                        original_event: event,
                        reason: 'core_runtime_error',
                        failed_at: new Date().toISOString(),
                        retry_count: 0,
                    }) }],
                });
            }

            logger.info(SERVICE, `[Tracking] event_id=${event.event_id} → [${finalState}]`);
        },
    });
};

startService().catch(err => { logger.error(SERVICE, 'Khởi động thất bại.', err); process.exit(1); });
app.listen(PORT, () => logger.success(SERVICE, `Health check: http://localhost:${PORT}/health`));
