// Khai báo tên tất cả Kafka topics dùng chung trong hệ thống
module.exports = {
    TOPICS: {
        RAW_EVENTS:      'raw_events',      // webhook-service → core-service
        REPLY_COMMANDS:  'reply_commands',  // core-service → backend-api
        SEND_RETRY:      'send_retry',      // retry-service → backend-api
        SEND_FAILED:     'send_failed',     // backend-api → retry-service
        DEAD_LETTER:     'dead_letter',     // retry-service (khi hết retry)
        MANUAL_REVIEW:   'manual_review',   // core-service → admin queue
    },
    RETRY_MAX: 3, // Số lần retry tối đa
};
