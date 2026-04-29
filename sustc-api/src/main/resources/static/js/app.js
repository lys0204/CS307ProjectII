const API_BASE = 'http://localhost:8080/api';

let currentCategory = '';
let currentPage = 1;
let currentPageSize = 20;
let currentTotal = 0;
let currentKeyword = '';

document.addEventListener('DOMContentLoaded', function() {
    loadFeed();
});

function globalSearch() {
    const keyword = document.getElementById('global-search').value.trim();
    if (keyword) {
        searchRecipes(keyword);
    } else {
        currentKeyword = '';
        currentPage = 1;
        loadRecipes();
    }
}

function filterByCategory(category, event) {
    currentCategory = category;
    currentPage = 1;
    currentKeyword = '';
    document.getElementById('global-search').value = '';
    loadRecipes();
    document.querySelectorAll('.category-tag').forEach(tag => {
        tag.classList.remove('active');
    });
    event.target.classList.add('active');
}

async function loadFeed() {
    currentPage = 1;
    currentKeyword = '';
    await loadRecipes();
}

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
        console.error('Load failed:', error);
        grid.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">Failed to load. Please try again later.</div>';
    } finally {
        loading.style.display = 'none';
    }
}

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
    pageInfo.textContent = `Page ${currentPage} / ${totalPages}`;

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

function changePage(delta) {
    const totalPages = Math.ceil(currentTotal / currentPageSize);
    const newPage = currentPage + delta;

    if (newPage >= 1 && newPage <= totalPages) {
        currentPage = newPage;
        loadRecipes();
        window.scrollTo({ top: 0, behavior: 'smooth' });
    }
}

function renderRecipeGrid(recipes) {
    const grid = document.getElementById('recipe-grid');

    if (!recipes || recipes.length === 0) {
        grid.innerHTML = '<div style="text-align: center; padding: 40px; color: #999;">No recipes found</div>';
        return;
    }

    grid.innerHTML = recipes.map(recipe => `
        <div class="recipe-card" onclick="viewRecipe(${recipe.recipeId})">
            <div class="recipe-thumbnail">
                🍳
            </div>
            <div class="recipe-info">
                <div class="recipe-title">${escapeHtml(recipe.name || 'Untitled')}</div>
                <div class="recipe-meta">
                    <span class="recipe-author">${escapeHtml(recipe.authorName || 'Unknown')}</span>
                    <div class="recipe-stats">
                        ${recipe.aggregatedRating ? `<span class="recipe-stat">⭐ ${recipe.aggregatedRating.toFixed(1)}</span>` : ''}
                        ${recipe.reviewCount ? `<span class="recipe-stat">💬 ${recipe.reviewCount}</span>` : ''}
                    </div>
                </div>
            </div>
        </div>
    `).join('');
}

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
    detailDiv.innerHTML = '<div style="text-align: center; padding: 20px;">Loading...</div>';

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
                        <span class="review-author">${review.authorName || 'Anonymous'}</span>
                        <span class="review-rating">⭐ ${review.rating || 0}</span>
                        <span class="review-date">${formatDate(review.dateModified || review.dateSubmitted)}</span>
                    </div>
                    <div class="review-content">${escapeHtml(review.review || 'No review content')}</div>
                </div>
            `).join('');
        } else {
            reviewsHtml = '<div style="text-align: center; padding: 20px; color: #999;">No reviews yet</div>';
        }

        detailDiv.innerHTML = `
            <div class="recipe-detail-header">
                <h2>${escapeHtml(recipe.name || 'Untitled')}</h2>
                <div class="recipe-detail-rating">
                    ${recipe.aggregatedRating ? `<span class="rating-badge">⭐ ${recipe.aggregatedRating.toFixed(1)}</span>` : '<span class="rating-badge">No rating</span>'}
                    <span class="review-count-badge">💬 ${recipe.reviewCount || 0} reviews</span>
                </div>
            </div>
            <div class="recipe-detail-meta">
                <div class="meta-item"><strong>Author:</strong><span>${escapeHtml(recipe.authorName || 'Unknown')}</span></div>
                <div class="meta-item"><strong>Category:</strong><span>${escapeHtml(recipe.recipeCategory || 'Uncategorized')}</span></div>
            </div>
            <div class="recipe-description">
                <h3>Description</h3>
                <div class="description-content">${recipe.description ? escapeHtml(recipe.description) : '<p style="color: #999;">No description</p>'}</div>
            </div>
            <div class="recipe-reviews">
                <h3>Reviews (${reviews?.total || 0})</h3>
                <div class="reviews-list">
                    ${reviewsHtml}
                </div>
            </div>
        `;
    } catch (error) {
        detailDiv.innerHTML = `<div style="text-align: center; padding: 20px; color: #f00;">Failed to load details: ${error.message}</div>`;
    }
}

function closeRecipeModal() {
    document.getElementById('recipe-modal').style.display = 'none';
    document.body.classList.remove('modal-open');
    document.body.style.overflow = '';
    document.body.style.position = '';
    document.body.style.top = '';
    document.body.style.width = '';
    window.scrollTo(0, scrollPosition);
}

function formatDate(dateStr) {
    if (!dateStr) return '';
    const date = new Date(dateStr);
    return date.toLocaleString('en-US');
}

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML.replace(/\n/g, '<br>');
}

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
            throw new Error(data.error || 'Request failed');
        }
        return data;
    } catch (error) {
        throw error;
    }
}

async function searchRecipes(keyword = null) {
    const searchKeyword = keyword || document.getElementById('global-search')?.value;
    currentKeyword = searchKeyword || '';
    currentPage = 1;
    currentCategory = '';

    document.querySelectorAll('.category-tag').forEach(tag => {
        tag.classList.remove('active');
    });
    document.querySelectorAll('.category-tag')[0].classList.add('active');

    await loadRecipes();
}

document.addEventListener('keypress', function(e) {
    if (e.key === 'Enter' && e.target.id === 'global-search') {
        globalSearch();
    }
});

window.onclick = function(event) {
    const modal = document.getElementById('recipe-modal');
    if (event.target === modal) {
        closeRecipeModal();
    }
}

document.addEventListener('click', function(event) {
    const modal = document.getElementById('recipe-modal');
    const modalContent = modal?.querySelector('.modal-content');
    if (modal && modalContent && event.target.closest('.modal-content') && event.target !== modal) {
        event.stopPropagation();
    }
});

// ===== AI Chat =====
function toggleChat() {
    const panel = document.getElementById('chat-panel');
    panel.style.display = panel.style.display === 'none' ? 'flex' : 'none';
}

async function sendChat() {
    const input = document.getElementById('chat-input');
    const message = input.value.trim();
    if (!message) return;

    appendChatMsg('user', message);
    input.value = '';

    const typing = appendChatMsg('assistant', 'Thinking...');

    try {
        const response = await fetch(`${API_BASE}/chat`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ message: message })
        });

        const data = await response.json();
        typing.remove();

        let replyHtml = escapeHtml(data.reply || 'Sorry, no results.');

        if (data.recipes && data.recipes.length > 0) {
            replyHtml += '\n\nRecommended recipes:';
            data.recipes.forEach(r => {
                replyHtml += `\n  🍳 <span class="recipe-link" onclick="viewRecipe(${r.recipeId}); closeChatPanel();">${escapeHtml(r.name)}</span> ⭐${(r.aggregatedRating||0).toFixed(1)}`;
            });
        }

        appendChatMsg('assistant', replyHtml, true);
    } catch (e) {
        typing.remove();
        appendChatMsg('assistant', 'Sorry, the AI service is currently unavailable: ' + escapeHtml(e.message));
    }
}

function appendChatMsg(role, text, isHtml) {
    const container = document.getElementById('chat-messages');
    const div = document.createElement('div');
    div.className = 'chat-msg ' + role;
    if (isHtml) {
        div.innerHTML = text.replace(/\n/g, '<br>');
        div.querySelectorAll('.recipe-link').forEach(link => {
            link.onclick = function() { eval(this.getAttribute('onclick')); };
        });
    } else {
        div.textContent = text;
    }
    container.appendChild(div);
    container.scrollTop = container.scrollHeight;
    return div;
}

function closeChatPanel() {
    document.getElementById('chat-panel').style.display = 'none';
}
