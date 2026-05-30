/**
 * gemini.js - Phân tích Intent & Sentiment bằng Google Gemini AI
 *
 * Input : Chuỗi nội dung bình luận
 * Output: { intent: string, sentiment: string }
 * Fallback: Trả { intent: 'unknown', sentiment: 'unknown', error: true }
 *           khi Gemini API chậm hoặc lỗi
 */
const { GoogleGenerativeAI } = require('@google/generative-ai');

let _genAI = null;
function getGenAI() {
    if (!_genAI) _genAI = new GoogleGenerativeAI(process.env.GEMINI_API_KEY);
    return _genAI;
}

/**
 * Gọi Gemini 2.0 Flash để phân tích intent và sentiment.
 * Prompt ngắn gọn, enforce JSON output để dễ parse và retry.
 */
async function analyzeWithGemini(content) {
    if (!content?.trim()) {
        return { intent: 'khác', sentiment: 'trung tính' };
    }

    try {
        const model = getGenAI().getGenerativeModel({
            model:             'gemini-2.0-flash',
            generationConfig:  { responseMimeType: 'application/json' },
        });

        const prompt = `Phân tích bình luận Facebook của khách hàng.
Trả về JSON với 2 key:
- "intent": chọn 1 trong: "hỏi giá", "khiếu nại", "khen", "hỏi thông tin", "khác"
- "sentiment": chọn 1 trong: "tích cực", "tiêu cực", "trung tính"

Ví dụ:
"Shop ơi giá bao nhiêu?" -> {"intent":"hỏi giá","sentiment":"trung tính"}
"Mình chưa nhận được hàng" -> {"intent":"khiếu nại","sentiment":"tiêu cực"}
"Bài viết hay quá" -> {"intent":"khen","sentiment":"tích cực"}

Bình luận: "${content.replace(/"/g, "'")}"`;

        const result  = await model.generateContent(prompt);
        const parsed  = JSON.parse(result.response.text());
        return { intent: parsed.intent || 'khác', sentiment: parsed.sentiment || 'trung tính' };
    } catch (err) {
        console.error('[Gemini] Lỗi phân tích (có thể hết Quota API). Chuyển sang Rule-based NLP fallback!');
        
        const txt = content.toLowerCase();
        let fallbackIntent = 'khác';
        let fallbackSentiment = 'trung tính';

        if (txt.includes('nhanh') || txt.includes('tốt') || txt.includes('tuyệt') || txt.includes('ưng ý') || txt.includes('hay')) {
            fallbackIntent = 'khen';
            fallbackSentiment = 'tích cực';
        } else if (txt.includes('lâu') || txt.includes('tệ') || txt.includes('chán') || txt.includes('chưa nhận') || txt.includes('thất vọng')) {
            fallbackIntent = 'khiếu nại';
            fallbackSentiment = 'tiêu cực';
        } else if (txt.includes('giá') || txt.includes('nhiêu') || txt.includes('inbox')) {
            fallbackIntent = 'hỏi giá';
        }

        return { intent: fallbackIntent, sentiment: fallbackSentiment, is_fallback: true };
    }
}

module.exports = { analyzeWithGemini };
