/**
 * logger.js - Format log chung cho tất cả services
 * Sử dụng: const logger = require('../../../shared/logger');
 *          logger.info('webhook-service', 'Message here');
 */
const colors = {
    reset:   '\x1b[0m',
    info:    '\x1b[36m',   // Cyan
    success: '\x1b[32m',   // Green
    warn:    '\x1b[33m',   // Yellow
    error:   '\x1b[31m',   // Red
};

function ts() {
    return new Date().toISOString();
}

const logger = {
    info: (service, msg) =>
        console.log(`${colors.info}[${ts()}] [${service}] INFO :${colors.reset} ${msg}`),

    success: (service, msg) =>
        console.log(`${colors.success}[${ts()}] [${service}] OK   :${colors.reset} ${msg}`),

    warn: (service, msg) =>
        console.warn(`${colors.warn}[${ts()}] [${service}] WARN :${colors.reset} ${msg}`),

    error: (service, msg, err) => {
        console.error(`${colors.error}[${ts()}] [${service}] ERROR:${colors.reset} ${msg}`);
        if (err) console.error(err?.message || err);
    },
};

module.exports = logger;
