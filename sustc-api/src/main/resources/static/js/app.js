// API 基础地址
const API_BASE = 'http://localhost:8080/api';

let currentCategory = '';
let currentPage = 1;
let currentPageSize = 20;
let currentTotal = 0;
let currentKeyword = '';

/**
 * 页面加载时初始化
 */
document.addEventListener('DOMContentLoaded', function() {
    loadFeed();
});


/**
 * 全局搜索
 */
function globalSearch() {
    const keyword = document.getElementById('global-search').value.trim();
    if (keyword) {
        searchRecipes(keyword);
    } else {
        // 如果搜索框为空，清空搜索并重新加载
        currentKeyword = '';
        currentPage = 1;
        loadRecipes();
    }
}

/**
 * 按分类筛选
 */
function filterByCategory(category, event) {
    currentCategory = category;
    currentPage = 1;
    currentKeyword = '';
    document.getElementById('global-search').value = '';
    loadRecipes();
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
    currentPage = 1;
    currentKeyword = '';
    await loadRecipes();
}

/**
 * 加载食谱列表
 */
async function loadRecipes() {
    const grid = document.getElementById('recipe-grid');
    const loading = document.getElementById('loading');
    const pagination = document.getElementById('pagination');
    
    grid.innerHTML = '';
    loading.style.display = 'block';
    
    try {
        const params = new URLSearchParams({
            page: currentPage.toString(),
            size: currentPageSize.toString(),
            ...(currentCategory && { category: currentCategory }),
            ...(currentKeyword && { keyword: currentKeyword })
        });
        
        const result = await apiCall(`${API_BASE}/recipes/search?${params}`);
        
        if (result && result.items) {
            renderRecipeGrid(result.items);
            currentTotal = result.total || 0;
            updatePagination();
        }
    } catch (error) {
        console.error('加载失败:', error);
        grid.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">加载失败，请稍后重试</div>';
    } finally {
        loading.style.display = 'none';
    }
}

/**
 * 更新分页控件
 */
function updatePagination() {
    const pagination = document.getElementById('pagination');
    const pageInfo = document.getElementById('page-info');
    const prevBtn = document.getElementById('prev-page');
    const nextBtn = document.getElementById('next-page');
    
    if (!pagination || !pageInfo || !prevBtn || !nextBtn) {
        return;
    }
    
    const totalPages = Math.ceil(currentTotal / currentPageSize);
    
    if (totalPages <= 1 || currentTotal === 0) {
        pagination.style.display = 'none';
        return;
    }
    
    pagination.style.display = 'flex';
    pageInfo.textContent = `第 ${currentPage} 页 / 共 ${totalPages} 页`;
    
    prevBtn.disabled = currentPage <= 1;
    nextBtn.disabled = currentPage >= totalPages;
    
    if (prevBtn.disabled) {
        prevBtn.style.opacity = '0.5';
        prevBtn.style.cursor = 'not-allowed';
    } else {
        prevBtn.style.opacity = '1';
        prevBtn.style.cursor = 'pointer';
    }
    
    if (nextBtn.disabled) {
        nextBtn.style.opacity = '0.5';
        nextBtn.style.cursor = 'not-allowed';
    } else {
        nextBtn.style.opacity = '1';
        nextBtn.style.cursor = 'pointer';
    }
}

/**
 * 切换页码
 */
function changePage(delta) {
    const totalPages = Math.ceil(currentTotal / currentPageSize);
    const newPage = currentPage + delta;
    
    if (newPage >= 1 && newPage <= totalPages) {
        currentPage = newPage;
        loadRecipes();
        window.scrollTo({ top: 0, behavior: 'smooth' });
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
            </div>
            <div class="recipe-info">
                <div class="recipe-title">${escapeHtml(recipe.name || '未命名食谱')}</div>
                <div class="recipe-meta">
                    <span class="recipe-author">${escapeHtml(recipe.authorName || '未知')}</span>
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
 * 查看食谱详情
 */
let scrollPosition = 0;

async function viewRecipe(recipeId) {
    const modal = document.getElementById('recipe-modal');
    const detailDiv = document.getElementById('recipe-detail');
    
    scrollPosition = window.pageYOffset || document.documentElement.scrollTop;
    document.body.classList.add('modal-open');
    document.body.style.overflow = 'hidden';
    document.body.style.position = 'fixed';
    document.body.style.top = `-${scrollPosition}px`;
    document.body.style.width = '100%';
    
    modal.style.display = 'block';
    detailDiv.innerHTML = '<div style="text-align: center; padding: 20px;">加载中...</div>';
    
    try {
        const [recipe, reviews] = await Promise.all([
            apiCall(`${API_BASE}/recipes/${recipeId}`),
            apiCall(`${API_BASE}/reviews/recipe/${recipeId}?page=1&size=10`)
        ]);
        
        let reviewsHtml = '';
        if (reviews && reviews.items && reviews.items.length > 0) {
            reviewsHtml = reviews.items.map(review => `
                <div class="review-item">
                    <div class="review-header">
                        <span class="review-author">${review.authorName || '匿名'}</span>
                        <span class="review-rating">⭐ ${review.rating || 0}</span>
                        <span class="review-date">${formatDate(review.dateModified || review.dateSubmitted)}</span>
                    </div>
                    <div class="review-content">${escapeHtml(review.review || '无评论内容')}</div>
                </div>
            `).join('');
        } else {
            reviewsHtml = '<div style="text-align: center; padding: 20px; color: #999;">暂无评论</div>';
        }
        
        detailDiv.innerHTML = `
            <div class="recipe-detail-header">
                <h2>${escapeHtml(recipe.name || '未命名食谱')}</h2>
                <div class="recipe-detail-rating">
                    ${recipe.aggregatedRating ? `<span class="rating-badge">⭐ ${recipe.aggregatedRating.toFixed(1)}</span>` : '<span class="rating-badge">暂无评分</span>'}
                    <span class="review-count-badge">💬 ${recipe.reviewCount || 0} 条评论</span>
                </div>
            </div>
            <div class="recipe-detail-meta">
                <div class="meta-item"><strong>作者：</strong><span>${escapeHtml(recipe.authorName || '未知')}</span></div>
                <div class="meta-item"><strong>分类：</strong><span>${escapeHtml(recipe.recipeCategory || '未分类')}</span></div>
            </div>
            <div class="recipe-description">
                <h3>📝 简介</h3>
                <div class="description-content">${recipe.description ? escapeHtml(recipe.description) : '<p style="color: #999;">暂无描述</p>'}</div>
            </div>
            <div class="recipe-reviews">
                <h3>💬 评论 (${reviews?.total || 0})</h3>
                <div class="reviews-list">
                    ${reviewsHtml}
                </div>
            </div>
        `;
    } catch (error) {
        detailDiv.innerHTML = `<div style="text-align: center; padding: 20px; color: #f00;">获取详情失败: ${error.message}</div>`;
    }
}

/**
 * 关闭食谱详情模态框
 */
function closeRecipeModal() {
    document.getElementById('recipe-modal').style.display = 'none';
    document.body.classList.remove('modal-open');
    document.body.style.overflow = '';
    document.body.style.position = '';
    document.body.style.top = '';
    document.body.style.width = '';
    window.scrollTo(0, scrollPosition);
}

/**
 * 格式化日期
 */
function formatDate(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleString('zh-CN');
}

/**
 * 转义HTML，防止XSS
 */
function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML.replace(/\n/g, '<br>');
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
 * 搜索食谱
 */
async function searchRecipes(keyword = null) {
    const searchKeyword = keyword || document.getElementById('global-search')?.value;
    currentKeyword = searchKeyword || '';
    currentPage = 1;
    currentCategory = '';
    
    // 更新分类标签状态
    document.querySelectorAll('.category-tag').forEach(tag => {
        tag.classList.remove('active');
    });
    document.querySelectorAll('.category-tag')[0].classList.add('active');
    
    await loadRecipes();
}


// 回车键搜索
document.addEventListener('keypress', function(e) {
    if (e.key === 'Enter' && e.target.id === 'global-search') {
        globalSearch();
    }
});

// 点击模态框外部关闭
window.onclick = function(event) {
    const modal = document.getElementById('recipe-modal');
    if (event.target === modal) {
        closeRecipeModal();
    }
}

// 阻止模态框内容区域的点击事件冒泡
document.addEventListener('click', function(event) {
    const modal = document.getElementById('recipe-modal');
    const modalContent = modal?.querySelector('.modal-content');
    if (modal && modalContent && event.target.closest('.modal-content') && event.target !== modal) {
        event.stopPropagation();
    }
});
