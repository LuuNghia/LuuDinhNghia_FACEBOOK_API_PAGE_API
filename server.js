require('dotenv').config();
const express = require('express');
const axios = require('axios');
const cors = require('cors');
const swaggerUi = require('swagger-ui-express');

const app = express();
app.use(express.json());
app.use(cors());

const FB_GRAPH_URL = process.env.FB_GRAPH_URL;
const ACCESS_TOKEN = process.env.PAGE_ACCESS_TOKEN;

const fbApi = axios.create({
    baseURL: FB_GRAPH_URL,
    params: { access_token: ACCESS_TOKEN }
});

// CẤU HÌNH SWAGGER 

const swaggerDocument = {
  openapi: '3.0.0',
  info: {
    title: 'Page API',
    version: '1.0.0',
    description: 'Endpoints cho Page Graph API (/{page-id}/...)',
  },
  tags: [{ name: 'Page API', description: '' }],
  paths: {
    '/api/page/{pageId}': {
      get: {
        tags: ['Page API'],
        parameters: [{ in: 'path', name: 'pageId', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: 'Thành công' } }
      }
    },
    '/api/page/{pageId}/posts': {
      get: {
        tags: ['Page API'],
        parameters: [{ in: 'path', name: 'pageId', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: 'Thành công' } }
      },
      post: {
        tags: ['Page API'],
        parameters: [{ in: 'path', name: 'pageId', required: true, schema: { type: 'string' } }],
        requestBody: {
          required: true,
          content: {
            'application/json': {
              schema: { type: 'object', properties: { message: { type: 'string', example: 'Xin chào từ API!' } } }
            }
          }
        },
        responses: { '200': { description: 'Thành công' } }
      }
    },
    '/api/page/post/{postId}': {
      delete: {
        tags: ['Page API'],
        parameters: [{ in: 'path', name: 'postId', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: 'Thành công' } }
      }
    },
    '/api/page/{pageId}/insights': {
      get: {
        tags: ['Page API'],
        parameters: [{ in: 'path', name: 'pageId', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: 'Thành công' } }
      }
    },
    '/api/page/post/{postId}/comments': {
      get: {
        tags: ['Page API'],
        parameters: [{ in: 'path', name: 'postId', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: 'Thành công' } }
      }
    },
    '/api/page/post/{postId}/likes': {
      get: {
        tags: ['Page API'],
        parameters: [{ in: 'path', name: 'postId', required: true, schema: { type: 'string' } }],
        responses: { '200': { description: 'Thành công' } }
      }
    }
  }
};

app.use('/api-docs', swaggerUi.serve, swaggerUi.setup(swaggerDocument, { explorer: true }));

// DANH SÁCH API XỬ LÝ LOGIC

app.get('/api/page/:pageId', async (req, res) => {
    try {
        const response = await fbApi.get(`/${req.params.pageId}`, { params: { fields: 'id,name,about,followers_count' } });
        res.json(response.data);
    } catch (error) { res.status(error.response?.status || 500).json(error.response?.data || { error: error.message }); }
});

app.get('/api/page/:pageId/posts', async (req, res) => {
    try {
        const response = await fbApi.get(`/${req.params.pageId}/posts`, { params: { fields: 'id,message,created_time' } });
        res.json(response.data);
    } catch (error) { res.status(error.response?.status || 500).json(error.response?.data || { error: error.message }); }
});

app.post('/api/page/:pageId/posts', async (req, res) => {
    try {
        const { message } = req.body;
        if (!message) return res.status(400).json({ error: "Thiếu message" });
        const response = await fbApi.post(`/${req.params.pageId}/feed`, { message });
        res.json({ success: true, data: response.data });
    } catch (error) { res.status(error.response?.status || 500).json(error.response?.data || { error: error.message }); }
});

app.delete('/api/page/post/:postId', async (req, res) => {
    try {
        const response = await fbApi.delete(`/${req.params.postId}`);
        res.json({ success: true, data: response.data });
    } catch (error) { res.status(error.response?.status || 500).json(error.response?.data || { error: error.message }); }
});

app.get('/api/page/:pageId/insights', async (req, res) => {
    try {
        const response = await fbApi.get(`/${req.params.pageId}`, { 
            params: { 
                fields: 'fan_count,followers_count,name' 
            } 
        });
        res.json({
            data: [
                { name: "Tổng lượt thích trang", value: response.data.fan_count },
                { name: "Tổng lượt người theo dõi", value: response.data.followers_count }
            ]
        });
    } catch (error) { 
        res.status(error.response?.status || 500).json(error.response?.data || { error: error.message }); 
    }
});

app.get('/api/page/post/:postId/comments', async (req, res) => {
    try {
        const response = await fbApi.get(`/${req.params.postId}/comments`, { params: { fields: 'id,message,created_time,from' } });
        res.json(response.data);
    } catch (error) { res.status(error.response?.status || 500).json(error.response?.data || { error: error.message }); }
});

app.get('/api/page/post/:postId/likes', async (req, res) => {
    try {
        const response = await fbApi.get(`/${req.params.postId}/likes`, { params: { summary: 'total_count' } });
        res.json(response.data);
    } catch (error) { res.status(error.response?.status || 500).json(error.response?.data || { error: error.message }); }
});
// KHỞI ĐỘNG SERVER
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    console.log(`Backend Server đang chạy tại: http://localhost:${PORT}`);
    console.log(`Xem giao diện Swagger UI tại: http://localhost:${PORT}/api-docs`);
});