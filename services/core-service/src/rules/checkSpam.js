/**
 * checkSpam.js - Logic phát hiện Spam
 * Tách riêng để dễ unit test và mở rộng rule
 */
const userSpamHistory = new Map();

/**
 * Kiểm tra xem nội dung comment có phải spam không.
 * @param {string} userId  - ID người dùng Facebook
 * @param {string} content - Nội dung bình luận
 * @returns {{ isSpam: boolean, type?: 'malicious_link'|'light_spam'|'repeated_3_times' }}
 */
function checkSpam(userId, content) {
    const now = Date.now();
    let history = userSpamHistory.get(userId) || {
        lastContent:    '',
        duplicateCount: 0,
        firstTime:      now,
    };

    // Reset counter sau 24 giờ
    if (now - history.firstTime > 24 * 60 * 60 * 1000) {
        history = { lastContent: '', duplicateCount: 0, firstTime: now };
    }

    // Kiểm tra lặp nội dung
    const isDuplicate = content?.trim() === history.lastContent.trim() && content?.trim() !== '';
    history.duplicateCount = isDuplicate ? history.duplicateCount + 1 : 0;
    history.lastContent    = content || '';
    userSpamHistory.set(userId, history);

    // Link độc hại / scam / bot → ẩn ngay + manual review
    if (/(https?:\/\/[^\s]+)/i.test(content)) {
        return { isSpam: true, type: 'malicious_link' };
    }

    // Spam lặp >= 3 lần trong 24h → Blacklist
    if (history.duplicateCount >= 2) {
        return { isSpam: true, type: 'repeated_3_times' };
    }

    // Spam nhẹ (lặp lần 1) → Ẩn ngay
    if (history.duplicateCount === 1) {
        return { isSpam: true, type: 'light_spam' };
    }

    return { isSpam: false };
}

module.exports = { checkSpam };
