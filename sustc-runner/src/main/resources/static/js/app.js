// API 基础地址
const API_BASE = 'http://localhost:8080/api';

// 当前用户信息
let currentUser = null;
let currentCategory = '';

/**
 * 页面加载时初始化
 */
document.addEventListener('DOMContentLoaded', function() {
    loadFeed();
    // 点击用户菜单显示登录模态框
    document.querySelector('.user-menu').addEventListener('click', function() {
        if (!currentUser) {
            showLoginModal();
        }
    });
});

/**
 * 切换标签页（保留用于兼容）
 */
function switchTab(tabName) {
    // 可以在这里添加标签页切换逻辑
    console.log('切换到标签页:', tabName);
}

/**
 * 显示登录模态框
 */
function showLoginModal() {
    document.getElementById('login-modal').style.display = 'block';
}

/**
 * 关闭模态框
 */
function closeModal() {
    document.getElementById('login-modal').style.display = 'none';
}

/**
 * 切换模态框标签
 */
function switchModalTab(tab) {
    const loginForm = document.getElementById('login-form');
    const registerForm = document.getElementById('register-form');
    const tabs = document.querySelectorAll('.modal-tab');
    
    tabs.forEach(t => t.classList.remove('active'));
    
    if (tab === 'login') {
        loginForm.style.display = 'flex';
        registerForm.style.display = 'none';
        tabs[0].classList.add('active');
    } else {
        loginForm.style.display = 'none';
        registerForm.style.display = 'flex';
        tabs[1].classList.add('active');
    }
}

/**
 * 显示上传模态框
 */
function showUploadModal() {
    if (!currentUser) {
        showLoginModal();
        return;
    }
    alert('投稿功能开发中...');
}

/**
 * 全局搜索
 */
function globalSearch() {
    const keyword = document.getElementById('global-search').value;
    if (keyword) {
        searchRecipes(keyword);
    }
}

/**
 * 按分类筛选
 */
function filterByCategory(category) {
    currentCategory = category;
    loadFeed();
    // 更新分类标签状态
    document.querySelectorAll('.category-tag').forEach(tag => {
        tag.classList.remove('active');
    });
    event.target.classList.add('active');
}

/**
 * 加载动态/推荐内容
 */
async function loadFeed() {
    const grid = document.getElementById('recipe-grid');
    const loading = document.getElementById('loading');
    
    grid.innerHTML = '';
    loading.style.display = 'block';
    
    try {
        // 如果没有登录，使用默认参数获取热门内容
        const params = new URLSearchParams({
            page: '1',
            size: '20',
            ...(currentCategory && { category: currentCategory })
        });
        
        // 尝试获取热门食谱（不需要认证）
        const result = await apiCall(`${API_BASE}/recipes/search?${params}`);
        
        if (result && result.items) {
            renderRecipeGrid(result.items);
        }
    } catch (error) {
        console.error('加载失败:', error);
        grid.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">加载失败，请稍后重试</div>';
    } finally {
        loading.style.display = 'none';
    }
}

/**
 * 渲染食谱网格
 */
function renderRecipeGrid(recipes) {
    const grid = document.getElementById('recipe-grid');
    
    if (!recipes || recipes.length === 0) {
        grid.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">暂无食谱</div>';
        return;
    }
    
    grid.innerHTML = recipes.map(recipe => `
        <div class="recipe-card" onclick="viewRecipe(${recipe.recipeId})">
            <div class="recipe-thumbnail">
                🍳
                ${recipe.cookTime ? `<span class="recipe-duration">${formatDuration(recipe.cookTime)}</span>` : ''}
            </div>
            <div class="recipe-info">
                <div class="recipe-title">${recipe.name || '未命名食谱'}</div>
                <div class="recipe-meta">
                    <span class="recipe-author">${recipe.authorName || '未知'}</span>
                    <div class="recipe-stats">
                        ${recipe.aggregatedRating ? `<span class="recipe-stat">⭐ ${recipe.aggregatedRating.toFixed(1)}</span>` : ''}
                        ${recipe.reviewCount ? `<span class="recipe-stat">💬 ${recipe.reviewCount}</span>` : ''}
                    </div>
                </div>
            </div>
        </div>
    `).join('');
}

/**
 * 格式化时长
 */
function formatDuration(duration) {
    if (!duration) return '';
    // 处理 ISO 8601 格式的时长，如 PT30M
    const match = duration.match(/PT(?:(\d+)H)?(?:(\d+)M)?/);
    if (match) {
        const hours = match[1] || 0;
        const minutes = match[2] || 0;
        if (hours > 0) {
            return `${hours}:${minutes.toString().padStart(2, '0')}`;
        }
        return `${minutes}分钟`;
    }
    return duration;
}

/**
 * 查看食谱详情
 */
async function viewRecipe(recipeId) {
    try {
        const recipe = await apiCall(`${API_BASE}/recipes/${recipeId}`);
        // 可以打开详情页或显示详情模态框
        alert(`食谱: ${recipe.name}\n作者: ${recipe.authorName}\n评分: ${recipe.aggregatedRating || '暂无'}\n描述: ${recipe.description || '无描述'}`);
    } catch (error) {
        alert('获取食谱详情失败: ' + error.message);
    }
}

/**
 * API 调用封装
 */
async function apiCall(url, method = 'GET', body = null) {
    try {
        const options = {
            method,
            headers: {
                'Content-Type': 'application/json',
            }
        };
        if (body) {
            options.body = JSON.stringify(body);
        }
        const response = await fetch(url, options);
        const data = await response.json();
        if (!response.ok) {
            throw new Error(data.error || '请求失败');
        }
        return data;
    } catch (error) {
        throw error;
    }
}

/**
 * 用户注册
 */
async function register() {
    try {
        const req = {
            username: document.getElementById('reg-username').value,
            password: document.getElementById('reg-password').value,
            gender: document.getElementById('reg-gender').value,
            age: parseInt(document.getElementById('reg-age').value)
        };
        const result = await apiCall(`${API_BASE}/users/register`, 'POST', req);
        if (result.userId) {
            alert('注册成功！用户ID: ' + result.userId);
            closeModal();
            // 可以自动登录
            document.getElementById('login-id').value = result.userId;
            document.getElementById('login-password').value = req.password;
            login();
        }
    } catch (error) {
        alert('注册失败: ' + error.message);
    }
}

/**
 * 用户登录
 */
async function login() {
    try {
        const auth = {
            authorId: parseInt(document.getElementById('login-id').value),
            password: document.getElementById('login-password').value
        };
        const result = await apiCall(`${API_BASE}/users/login`, 'POST', auth);
        if (result.userId) {
            currentUser = { id: result.userId, ...auth };
            document.getElementById('current-user').textContent = '用户 ' + result.userId;
            closeModal();
            // 重新加载内容
            loadFeed();
        }
    } catch (error) {
        alert('登录失败: ' + error.message);
    }
}

/**
 * 搜索食谱
 */
async function searchRecipes(keyword = null) {
    const searchKeyword = keyword || document.getElementById('global-search')?.value;
    if (!searchKeyword) return;
    
    const grid = document.getElementById('recipe-grid');
    const loading = document.getElementById('loading');
    
    grid.innerHTML = '';
    loading.style.display = 'block';
    
    try {
        const params = new URLSearchParams({
            page: '1',
            size: '20',
            keyword: searchKeyword
        });
        const result = await apiCall(`${API_BASE}/recipes/search?${params}`);
        if (result && result.items) {
            renderRecipeGrid(result.items);
        }
    } catch (error) {
        console.error('搜索失败:', error);
        grid.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">搜索失败</div>';
    } finally {
        loading.style.display = 'none';
    }
}

/**
 * 获取用户信息
 */
async function getUser() {
    try {
        const userId = document.getElementById('get-user-id').value;
        const result = await apiCall(`${API_BASE}/users/${userId}`);
        showResult('user-result', result);
    } catch (error) {
        showResult('user-result', { error: error.message }, true);
    }
}

/**
 * 获取用户动态
 */
async function getFeed() {
    if (!currentUser) {
        alert('请先登录');
        return;
    }
    
    try {
        const category = document.getElementById('feed-category')?.value || '';
        const params = new URLSearchParams({
            page: '1',
            size: '20',
            ...(category && { category })
        });
        const result = await apiCall(`${API_BASE}/users/feed?${params}`, 'POST', {
            authorId: currentUser.id,
            password: currentUser.password
        });
        if (result && result.items) {
            renderRecipeGrid(result.items);
        }
    } catch (error) {
        alert('获取动态失败: ' + error.message);
    }
}

/**
 * 添加评论
 */
async function addReview() {
    if (!currentUser) {
        alert('请先登录');
        return;
    }
    
    try {
        const req = {
            authorId: currentUser.id,
            password: currentUser.password,
            recipeId: parseInt(document.getElementById('review-recipe-id').value),
            rating: parseInt(document.getElementById('review-rating').value),
            review: document.getElementById('review-content').value
        };
        const result = await apiCall(`${API_BASE}/reviews`, 'POST', req);
        alert('评论添加成功！');
        document.getElementById('review-content').value = '';
    } catch (error) {
        alert('添加评论失败: ' + error.message);
    }
}

/**
 * 获取食谱评论列表
 */
async function listReviews() {
    try {
        const recipeId = document.getElementById('list-recipe-id').value;
        const params = new URLSearchParams({
            page: '1',
            size: '10'
        });
        const result = await apiCall(`${API_BASE}/reviews/recipe/${recipeId}?${params}`);
        showResult('review-result', result);
    } catch (error) {
        showResult('review-result', { error: error.message }, true);
    }
}

/**
 * 显示结果（用于调试）
 */
function showResult(elementId, data, isError = false) {
    const element = document.getElementById(elementId);
    if (element) {
        element.style.display = 'block';
        element.className = 'result ' + (isError ? 'error' : 'success');
        element.textContent = JSON.stringify(data, null, 2);
    }
}

// 点击模态框外部关闭
window.onclick = function(event) {
    const modal = document.getElementById('login-modal');
    if (event.target === modal) {
        closeModal();
    }
}

// 回车键搜索
document.addEventListener('keypress', function(e) {
    if (e.key === 'Enter' && e.target.id === 'global-search') {
        globalSearch();
    }
});
