require('dotenv').config({ path: require('path').resolve(__dirname, '../../../.env') });
const express = require('express');
const { Kafka } = require('kafkajs');
const { TOPICS, RETRY_MAX } = require('../../../shared/constants');
const logger = require('../../../shared/logger');

const SERVICE = 'retry-service';
const app     = express();
const PORT    = process.env.RETRY_PORT   || 3003;
const BROKER  = process.env.KAFKA_BROKER || 'localhost:9092';

const kafka    = new Kafka({ clientId: SERVICE, brokers: [BROKER] });
const consumer = kafka.consumer({ groupId: 'retry-processing-group' });
const producer = kafka.producer();

let retryMetric = 0;
let dlqMetric   = 0;

app.get('/health', (_, res) => res.json({ status: 'ok', service: SERVICE, port: PORT, timestamp: new Date().toISOString() }));
app.get('/metrics', (_, res) => res.json({ retries_attempted: retryMetric, dead_letter_sent: dlqMetric }));

/**
 * Tính thời gian chờ theo exponential backoff:  delay = 1000ms × 2^retry_count
 * retry_count=0 → 1s, retry_count=1 → 2s, retry_count=2 → 4s
 */
function backoffDelay(retryCount) {
    return 1000 * Math.pow(2, retryCount);
}

async function startService() {
    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({ topic: TOPICS.SEND_FAILED, fromBeginning: false });
    logger.success(SERVICE, `Đang lắng nghe topic [${TOPICS.SEND_FAILED}]...`);

    await consumer.run({
        eachMessage: async ({ message }) => {
            let failedCmd;
            try { failedCmd = JSON.parse(message.value.toString()); }
            catch { logger.error(SERVICE, 'Message JSON không hợp lệ, bỏ qua.'); return; }

            const retryCount = failedCmd.retry_count || 0;
            const delay      = backoffDelay(retryCount);
            const errorMsg   = failedCmd.error || '';

            logger.warn(SERVICE, `Nhận lệnh thất bại: ${failedCmd.action} | comment=${failedCmd.comment_id} | retry=${retryCount}/${RETRY_MAX}`);

            // Phân biệt lỗi tạm thời và lỗi không thể khôi phục (Yêu cầu 5.4)
            const isUnrecoverable = errorMsg.includes('Invalid OAuth access token') || 
                                    errorMsg.includes('Permissions error') || 
                                    errorMsg.includes('circuit_open');

            if (isUnrecoverable) {
                logger.error(SERVICE, `[Phân loại lỗi] Lỗi không thể khôi phục ("${errorMsg}"). Bỏ qua Retry, đẩy thẳng vào DLQ!`);
                await producer.send({
                    topic: TOPICS.DEAD_LETTER,
                    messages: [{ value: JSON.stringify({ ...failedCmd, dead_at: new Date().toISOString(), reason: 'unrecoverable_error' }) }],
                });
                dlqMetric++;
            } else if (retryCount < RETRY_MAX) {
                // Chưa hết số lần → chờ rồi publish lại vào send_retry
                logger.info(SERVICE, `Chờ ${delay}ms trước khi retry (lần ${retryCount + 1}/${RETRY_MAX})...`);
                setTimeout(async () => {
                    try {
                        const retryCmd = { ...failedCmd, retry_count: retryCount + 1 };
                        await producer.send({
                            topic: TOPICS.SEND_RETRY,
                            messages: [{ value: JSON.stringify(retryCmd) }],
                        });
                        retryMetric++;
                        logger.success(SERVICE, `[send_retry] Đã đẩy lại (lần ${retryCount + 1}) vào topic [send_retry].`);
                    } catch (err) {
                        logger.error(SERVICE, 'Lỗi khi gửi send_retry.', err);
                    }
                }, delay);
            } else {
                // Hết số lần → đẩy vào dead_letter
                await producer.send({
                    topic: TOPICS.DEAD_LETTER,
                    messages: [{ value: JSON.stringify({ ...failedCmd, dead_at: new Date().toISOString(), reason: 'max_retries_exceeded' }) }],
                });
                dlqMetric++;
                logger.error(SERVICE, `[dead_letter] Hết ${RETRY_MAX} lần retry. Đã chuyển sang dead_letter! Kiểm tra Kafka UI tại http://localhost:8080`);
            }
        },
    });
}

startService().catch(err => { logger.error(SERVICE, 'Khởi động thất bại.', err); process.exit(1); });
app.listen(PORT, () => {
    logger.success(SERVICE, `Đang chạy tại http://localhost:${PORT}`);
    logger.info(SERVICE, `Metrics: http://localhost:${PORT}/metrics`);
});
