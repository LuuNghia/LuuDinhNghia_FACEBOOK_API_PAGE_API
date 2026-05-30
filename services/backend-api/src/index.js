require('dotenv').config({ path: require('path').resolve(__dirname, '../../../.env') });
const express    = require('express');
const cors       = require('cors');
const swaggerUi  = require('swagger-ui-express');
const { Kafka }  = require('kafkajs');
const { Pool }   = require('pg');
const fbApi      = require('./facebook/graphApi');
const pageRoutes = require('./routes/pageRoutes');
const { TOPICS } = require('../../../shared/constants');
const logger     = require('../../../shared/logger');

const SERVICE = 'backend-api';
const app     = express();
app.use(express.json());
app.use(cors());

const PORT   = process.env.BACKEND_PORT  || 3000;
const BROKER = process.env.KAFKA_BROKER  || 'localhost:9092';

// ── PostgreSQL: Idempotency Key ──────────────────────────────────
const db = new Pool({
    host:     process.env.DB_HOST     || 'localhost',
    port:     process.env.DB_PORT     || 5432,
    database: process.env.DB_NAME     || 'fb_api_db',
    user:     process.env.DB_USER     || 'fb_api_user',
    password: process.env.DB_PASSWORD || 'fb_api_password',
});

async function initDb() {
    await db.query(`
        CREATE TABLE IF NOT EXISTS idempotency_keys (
            command_id   VARCHAR(200) PRIMARY KEY,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status       VARCHAR(20) NOT NULL
        );
        CREATE TABLE IF NOT EXISTS comments (
            id         SERIAL PRIMARY KEY,
            comment_id VARCHAR(100) UNIQUE NOT NULL,
            post_id    VARCHAR(100),
            message    TEXT,
            intent     VARCHAR(50),
            sentiment  VARCHAR(20),
            status     VARCHAR(20) DEFAULT 'received',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    `);
    logger.success(SERVICE, 'PostgreSQL: Các bảng đã sẵn sàng.');
}

// Kiểm tra idempotency key
async function isDuplicate(commandId) {
    const r = await db.query('SELECT 1 FROM idempotency_keys WHERE command_id=$1', [commandId]);
    return r.rowCount > 0;
}
async function markProcessed(commandId, status) {
    await db.query(
        'INSERT INTO idempotency_keys(command_id,status) VALUES($1,$2) ON CONFLICT DO NOTHING',
        [commandId, status]
    );
}

// ── Circuit Breaker (đơn giản, in-memory) ───────────────────────
const breaker = { failures: 0, state: 'closed', openedAt: null };
const CB_THRESHOLD = 5;    // mở mạch sau 5 lỗi liên tiếp
const CB_TIMEOUT   = 30000; // thử lại sau 30 giây

function circuitAllow() {
    if (breaker.state === 'closed')   return true;
    if (breaker.state === 'open') {
        if (Date.now() - breaker.openedAt > CB_TIMEOUT) {
            breaker.state = 'half-open';
            logger.info(SERVICE, `[CircuitBreaker] Chuyển sang HALF-OPEN, thử lại...`);
            return true;
        }
        return false;
    }
    return true; // half-open: cho 1 request thử
}
function circuitSuccess() { breaker.failures = 0; breaker.state = 'closed'; }
function circuitFail() {
    breaker.failures++;
    if (breaker.failures >= CB_THRESHOLD) {
        breaker.state = 'open'; breaker.openedAt = Date.now();
        logger.error(SERVICE, `[CircuitBreaker] Mở mạch! ${CB_THRESHOLD} lỗi liên tiếp. Dừng gọi FB API trong ${CB_TIMEOUT/1000}s.`);
    }
}

// ── Kafka ────────────────────────────────────────────────────────
const kafka    = new Kafka({ clientId: SERVICE, brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'backend-action-group' });
const producer = kafka.producer();

async function executeCommand(command) {
    const { command_id, action, comment_id, reply_text } = command;

    // Idempotency check
    if (await isDuplicate(command_id)) {
        logger.warn(SERVICE, `[Idempotency] command_id=${command_id} đã xử lý, bỏ qua.`);
        return;
    }

    // Circuit Breaker check
    if (!circuitAllow()) {
        logger.error(SERVICE, `[CircuitBreaker] Mạch OPEN, không gọi FB API.`);
        throw new Error('circuit_open');
    }

    try {
        if (action === 'hide' && comment_id) {
            logger.info(SERVICE, `[FB API] Ẩn comment: ${comment_id}`);
            const res = await fbApi.hideComment(comment_id);
            if (res.data.success) logger.success(SERVICE, `[FB API] Ẩn thành công comment: ${comment_id}`);
        } else if (action === 'reply' && comment_id && reply_text) {
            logger.info(SERVICE, `[FB API] Reply comment: ${comment_id} | "${reply_text}"`);
            await fbApi.replyComment(comment_id, reply_text);
            logger.success(SERVICE, `[FB API] Reply thành công comment: ${comment_id}`);
        } else {
            logger.warn(SERVICE, `[Lệnh không rõ] action=${action}, comment_id=${comment_id}`);
        }
        circuitSuccess();
        await markProcessed(command_id, 'success');
    } catch (err) {
        circuitFail();
        const detail = err.response?.data?.error?.message || err.message;
        logger.error(SERVICE, `[FB API Error] ${detail}`);
        throw err; // để caller xử lý retry
    }
}

async function startKafkaConsumer() {
    await consumer.connect();
    await producer.connect();

    // Lắng nghe reply_commands (từ core-service) VÀ send_retry (từ retry-service)
    await consumer.subscribe({ topics: [TOPICS.REPLY_COMMANDS, TOPICS.SEND_RETRY], fromBeginning: false });
    logger.success(SERVICE, `Đang lắng nghe [${TOPICS.REPLY_COMMANDS}] và [${TOPICS.SEND_RETRY}]...`);

    await consumer.run({
        eachMessage: async ({ topic, message }) => {
            let command;
            try { command = JSON.parse(message.value.toString()); }
            catch { logger.error(SERVICE, 'Message JSON không hợp lệ, bỏ qua.'); return; }

            logger.info(SERVICE, `[Kafka:${topic}] Nhận lệnh: ${command.action} | comment_id: ${command.comment_id}`);

            try {
                await executeCommand(command);
            } catch (err) {
                logger.error(SERVICE, `Lỗi khi thực thi lệnh ${command.action}:`, err);
                // Gửi send_failed để Retry Service xử lý
                const retryCount = command.retry_count || 0;
                await producer.send({
                    topic: TOPICS.SEND_FAILED,
                    messages: [{ value: JSON.stringify({
                        ...command,
                        retry_count: retryCount,
                        error:       err.response?.data?.error?.message || err.message,
                        failed_at:   new Date().toISOString(),
                    }) }],
                });
                logger.warn(SERVICE, `[send_failed] Gửi lệnh thất bại sang Retry Service (retry=${retryCount}).`);
            }
        },
    });
}

// ── Swagger UI ───────────────────────────────────────────────────
const swaggerDoc = {
    openapi: '3.0.0',
    info: { title: 'Facebook Page API', version: '1.0.0', description: 'REST API cho dashboard quản trị Facebook Page' },
    tags: [{ name: 'Page API' }],
    paths: {
        '/api/page/{pageId}':                { get: { tags:['Page API'], parameters:[{in:'path',name:'pageId',required:true,schema:{type:'string'}}], responses:{'200':{description:'OK'}} } },
        '/api/page/{pageId}/posts':          { get: { tags:['Page API'], parameters:[{in:'path',name:'pageId',required:true,schema:{type:'string'}}], responses:{'200':{description:'OK'}} }, post: { tags:['Page API'], parameters:[{in:'path',name:'pageId',required:true,schema:{type:'string'}}], requestBody:{required:true,content:{'application/json':{schema:{type:'object',properties:{message:{type:'string'}}}}}}, responses:{'200':{description:'OK'}} } },
        '/api/page/post/{postId}':           { delete: { tags:['Page API'], parameters:[{in:'path',name:'postId',required:true,schema:{type:'string'}}], responses:{'200':{description:'OK'}} } },
        '/api/page/{pageId}/insights':       { get: { tags:['Page API'], parameters:[{in:'path',name:'pageId',required:true,schema:{type:'string'}}], responses:{'200':{description:'OK'}} } },
        '/api/page/post/{postId}/comments':  { get: { tags:['Page API'], parameters:[{in:'path',name:'postId',required:true,schema:{type:'string'}}], responses:{'200':{description:'OK'}} } },
        '/api/page/post/{postId}/likes':     { get: { tags:['Page API'], parameters:[{in:'path',name:'postId',required:true,schema:{type:'string'}}], responses:{'200':{description:'OK'}} } },
    },
};
app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDoc, { explorer: true }));
app.use('/api', pageRoutes);
app.get('/health', (_, res) => res.json({ status: 'ok', service: SERVICE, circuit: breaker.state }));

// ── Khởi động ────────────────────────────────────────────────────
(async () => {
    try {
        await initDb();
        await startKafkaConsumer();
        app.listen(PORT, () => {
            logger.success(SERVICE, `Đang chạy tại http://localhost:${PORT}`);
            logger.info(SERVICE, `Swagger UI: http://localhost:${PORT}/api-docs`);
        });
    } catch (err) {
        logger.error(SERVICE, 'Khởi động thất bại.', err);
        process.exit(1);
    }
})();
