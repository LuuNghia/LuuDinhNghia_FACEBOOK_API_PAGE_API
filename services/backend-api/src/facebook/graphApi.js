const axios = require('axios');

/**
 * Module gọi Facebook Graph API
 * Đây là nơi DUY NHẤT trong hệ thống được phép gọi ra Facebook
 */
const fbApi = axios.create({
    baseURL: process.env.FB_GRAPH_URL || 'https://graph.facebook.com/v21.0',
    params:  { access_token: process.env.PAGE_ACCESS_TOKEN },
    timeout: 10000, // 10 giây timeout
});

module.exports = {
    // Ẩn bình luận (Dùng native fetch để tránh lỗi timeout của Axios)
    hideComment: async (commentId) => {
        const url = `${process.env.FB_GRAPH_URL || 'https://graph.facebook.com/v21.0'}/${commentId}?is_hidden=true&access_token=${process.env.PAGE_ACCESS_TOKEN}`;
        const res = await fetch(url, { method: 'POST' });
        const data = await res.json();
        if (!res.ok) throw new Error(data.error?.message || 'Lỗi không xác định từ FB');
        return { data }; // Trả về định dạng giống Axios để code cũ không bị lỗi
    },

    // Reply bình luận
    replyComment: (commentId, message) => {
        return fbApi.post(`/${commentId}/comments`, { message });
    },

    // Lấy danh sách bài viết
    getPosts: (pageId) =>
        fbApi.get(`/${pageId}/posts`, { params: { fields: 'id,message,created_time' } }),

    // Đăng bài mới
    createPost: (pageId, message) => {
        return fbApi.post(`/${pageId}/feed`, { message });
    },

    // Xóa bài viết
    deletePost: (postId) =>
        fbApi.delete(`/${postId}`),

    // Lấy thông tin Page
    getPage: (pageId) =>
        fbApi.get(`/${pageId}`, { params: { fields: 'id,name,about,followers_count,fan_count' } }),

    // Lấy bình luận của bài viết
    getComments: (postId) =>
        fbApi.get(`/${postId}/comments`, { params: { fields: 'id,message,created_time,from' } }),

    // Lấy lượt thích
    getLikes: (postId) =>
        fbApi.get(`/${postId}/likes`, { params: { summary: 'total_count' } }),
};
