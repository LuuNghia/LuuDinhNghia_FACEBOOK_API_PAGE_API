require('dotenv').config({ path: require('path').resolve(__dirname, '../../../.env') });
const express = require('express');
const crypto  = require('crypto');
const { Kafka, Partitioners } = require('kafkajs');
const { TOPICS } = require('../../../shared/constants');
const logger    = require('../../../shared/logger');

const SERVICE = 'webhook-service';
const app     = express();

app.use(express.json({ verify: (req, res, buf) => { req.rawBody = buf; } }));
app.use((req, res, next) => { res.setHeader('ngrok-skip-browser-warning', 'true'); next(); });

const PORT         = process.env.WEBHOOK_PORT    || 3001;
const VERIFY_TOKEN = process.env.FB_VERIFY_TOKEN || 'hahahahahahahaha';
const APP_SECRET   = process.env.FB_APP_SECRET   || '';
const KAFKA_BROKER = process.env.KAFKA_BROKER    || 'localhost:9092';

// Khởi tạo Kafka Producer
const kafka    = new Kafka({ clientId: SERVICE, brokers: [KAFKA_BROKER] });
const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });

(async () => {
    await producer.connect();
    logger.success(SERVICE, `Kafka Producer kết nối thành công.`);
})().catch(err => logger.error(SERVICE, 'Kafka kết nối thất bại.', err));

// ── Xác thực chữ ký HMAC-SHA256 ──────────────────────────────
function verifySignature(req, res, next) {
    if (!APP_SECRET) return next(); // bỏ qua khi dev chưa có APP_SECRET
    const sig = req.headers['x-hub-signature-256'];
    if (!sig) return res.sendStatus(401);
    const hash = crypto.createHmac('sha256', APP_SECRET).update(req.rawBody).digest('hex');
    if (`sha256=${hash}` !== sig) {
        logger.error(SERVICE, 'Chữ ký HMAC-SHA256 không hợp lệ.');
        return res.sendStatus(401);
    }
    next();
}

// ── GET /webhook: Facebook xác minh callback URL ─────────────
app.get('/webhook', (req, res) => {
    const mode      = req.query['hub.mode'];
    const token     = req.query['hub.verify_token'];
    const challenge = req.query['hub.challenge'];
    if (mode === 'subscribe' && token === VERIFY_TOKEN) {
        logger.success(SERVICE, 'Xác minh Webhook với Facebook thành công!');
        return res.status(200).send(challenge);
    }
    return res.sendStatus(403);
});

// ── POST /webhook: Nhận event, normalize, publish raw_events ─
app.post('/webhook', verifySignature, async (req, res) => {
    // Trả 200 OK ngay lập tức để Facebook không retry
    res.status(200).send('EVENT_RECEIVED');

    const body = req.body;
    logger.info(SERVICE, `Nhận payload: ${JSON.stringify(body)}`);

    if (body.object !== 'page' && body.object !== 'test') return;

    for (const entry of body.entry || []) {
        // Feed events (bình luận bài viết)
        for (const change of entry.changes || []) {
            if (change.field === 'feed' && change.value?.verb === 'add') {
                const raw = change.value;
                await publish({
                    event_id:     raw.comment_id || raw.id || null,
                    event_source: 'facebook_feed',
                    event_type:   raw.item,
                    user_id:      raw.from?.id   || 'N/A',
                    sender_name:  raw.from?.name || 'Ẩn danh',
                    content:      raw.message    || '',
                    post_id:      raw.post_id    || null,
                    timestamp:    new Date().toISOString(),
                });
            }
        }

        // Messaging events (tin nhắn inbox)
        for (const msg of entry.messaging || []) {
            if (msg.message && !msg.message.is_echo) {
                await publish({
                    event_id:     msg.message.mid,
                    event_source: 'facebook_messenger',
                    event_type:   'message',
                    user_id:      msg.sender.id,
                    sender_name:  `User_${msg.sender.id}`,
                    content:      msg.message.text || '',
                    post_id:      null,
                    timestamp:    new Date().toISOString(),
                });
            }
        }
    }
});

async function publish(event) {
    try {
        await producer.send({ topic: TOPICS.RAW_EVENTS, messages: [{ value: JSON.stringify(event) }] });
        logger.success(SERVICE, `[raw_events] | ${event.sender_name} | "${event.content}" | ID: ${event.event_id}`);
    } catch (err) {
        logger.error(SERVICE, 'Lỗi publish Kafka.', err);
    }
}

app.listen(PORT, () => {
    logger.success(SERVICE, `Đang chạy tại http://localhost:${PORT}`);
    logger.info(SERVICE, `Webhook URL: http://localhost:${PORT}/webhook`);
});
