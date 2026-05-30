const express = require('express');
const router  = express.Router();
const fbApi   = require('../facebook/graphApi');
const logger  = require('../../../../shared/logger');

const SERVICE = 'backend-api';

// ── GET /api/page/:pageId ─────────────────────────────────────
router.get('/page/:pageId', async (req, res) => {
    try {
        logger.info(SERVICE, `GET /page/${req.params.pageId}`);
        const r = await fbApi.getPage(req.params.pageId);
        res.json(r.data);
    } catch (e) { res.status(e.response?.status || 500).json(e.response?.data || { error: e.message }); }
});

// ── GET /api/page/:pageId/posts ───────────────────────────────
router.get('/page/:pageId/posts', async (req, res) => {
    try {
        logger.info(SERVICE, `GET /page/${req.params.pageId}/posts`);
        const r = await fbApi.getPosts(req.params.pageId);
        res.json(r.data);
    } catch (e) { res.status(e.response?.status || 500).json(e.response?.data || { error: e.message }); }
});

// ── POST /api/page/:pageId/posts ──────────────────────────────
router.post('/page/:pageId/posts', async (req, res) => {
    try {
        const { message } = req.body;
        if (!message) return res.status(400).json({ error: 'Thiếu trường message' });
        logger.info(SERVICE, `POST /page/${req.params.pageId}/posts | "${message}"`);
        const r = await fbApi.createPost(req.params.pageId, message);
        res.json({ success: true, data: r.data });
    } catch (e) { res.status(e.response?.status || 500).json(e.response?.data || { error: e.message }); }
});

// ── DELETE /api/page/post/:postId ────────────────────────────
router.delete('/page/post/:postId', async (req, res) => {
    try {
        logger.info(SERVICE, `DELETE /page/post/${req.params.postId}`);
        const r = await fbApi.deletePost(req.params.postId);
        res.json({ success: true, data: r.data });
    } catch (e) { res.status(e.response?.status || 500).json(e.response?.data || { error: e.message }); }
});

// ── GET /api/page/:pageId/insights ───────────────────────────
router.get('/page/:pageId/insights', async (req, res) => {
    try {
        logger.info(SERVICE, `GET /page/${req.params.pageId}/insights`);
        const r = await fbApi.getPage(req.params.pageId);
        res.json({ data: [
            { name: 'Tổng lượt thích trang', value: r.data.fan_count },
            { name: 'Tổng người theo dõi', value: r.data.followers_count },
        ]});
    } catch (e) { res.status(e.response?.status || 500).json(e.response?.data || { error: e.message }); }
});

// ── GET /api/page/post/:postId/comments ──────────────────────
router.get('/page/post/:postId/comments', async (req, res) => {
    try {
        logger.info(SERVICE, `GET /post/${req.params.postId}/comments`);
        const r = await fbApi.getComments(req.params.postId);
        res.json(r.data);
    } catch (e) { res.status(e.response?.status || 500).json(e.response?.data || { error: e.message }); }
});

// ── GET /api/page/post/:postId/likes ─────────────────────────
router.get('/page/post/:postId/likes', async (req, res) => {
    try {
        const r = await fbApi.getLikes(req.params.postId);
        res.json(r.data);
    } catch (e) { res.status(e.response?.status || 500).json(e.response?.data || { error: e.message }); }
});

module.exports = router;
