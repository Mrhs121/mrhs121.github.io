// Common Theme Management
function initTheme() {
  const savedTheme = localStorage.getItem('theme') || 'dark';
  document.documentElement.setAttribute('data-theme', savedTheme);
  updateThemeIcon(savedTheme);

  const toggleBtn = document.getElementById('themeToggle');
  if (toggleBtn) {
    toggleBtn.addEventListener('click', () => {
      const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
      const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
      document.documentElement.setAttribute('data-theme', newTheme);
      localStorage.setItem('theme', newTheme);
      updateThemeIcon(newTheme);
    });
  }
}

function updateThemeIcon(theme) {
  const icon = document.getElementById('themeIcon');
  if (!icon) return;
  if (theme === 'light') {
    icon.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>`;
  } else {
    icon.innerHTML = `<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"></circle><line x1="12" y1="1" x2="12" y2="3"></line><line x1="12" y1="21" x2="12" y2="23"></line><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line><line x1="1" y1="12" x2="3" y2="12"></line><line x1="21" y1="12" x2="23" y2="12"></line><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line></svg>`;
  }
}

// Blog Home Logic
let allPosts = [];
let activeTag = 'ALL';
let activeSeries = 'ALL';

async function loadPosts() {
  const postsContainer = document.getElementById('postsGrid');
  const tagsContainer = document.getElementById('tagsFilter');
  const seriesContainer = document.getElementById('seriesCatalog');
  if (!postsContainer) return;

  try {
    const res = await fetch('posts/manifest.json');
    allPosts = await res.json();

    renderSeriesCatalog(seriesContainer);
    renderTags(tagsContainer);
    renderPosts(allPosts);
    setupSearch();
  } catch (err) {
    console.error('Failed to load posts:', err);
    postsContainer.innerHTML = `<div style="color: var(--text-muted); padding: 2rem; text-align: center;">加载博文列表失败，请稍后刷新。</div>`;
  }
}

function renderSeriesCatalog(container) {
  if (!container) return;
  // Preserve manifest order (newest first) while grouping
  const seriesOrder = [];
  const seriesCount = {};
  allPosts.forEach(p => {
    const s = p.series || '其他';
    if (!(s in seriesCount)) {
      seriesOrder.push(s);
      seriesCount[s] = 0;
    }
    seriesCount[s]++;
  });

  const cards = [
    { name: 'ALL', label: '全部文章', count: allPosts.length },
    ...seriesOrder.map(s => ({ name: s, label: s, count: seriesCount[s] }))
  ];

  container.innerHTML = cards.map(c => `
    <button class="series-card ${c.name === activeSeries ? 'active' : ''}" data-series="${c.name}">
      <span class="series-name">${c.label}</span>
      <span class="series-count">${c.count} 篇</span>
    </button>
  `).join('');

  container.querySelectorAll('.series-card').forEach(card => {
    card.addEventListener('click', () => {
      activeSeries = card.getAttribute('data-series');
      container.querySelectorAll('.series-card').forEach(x => x.classList.remove('active'));
      card.classList.add('active');
      filterAndRender();
    });
  });
}

function renderTags(container) {
  if (!container) return;
  const tagSet = new Set();
  allPosts.forEach(p => (p.tags || []).forEach(t => tagSet.add(t)));
  
  const tags = ['ALL', ...Array.from(tagSet)];
  container.innerHTML = tags.map(tag => `
    <span class="tag-pill ${tag === activeTag ? 'active' : ''}" data-tag="${tag}">
      ${tag === 'ALL' ? '全部 ALL' : '#' + tag}
    </span>
  `).join('');

  container.querySelectorAll('.tag-pill').forEach(pill => {
    pill.addEventListener('click', () => {
      activeTag = pill.getAttribute('data-tag');
      container.querySelectorAll('.tag-pill').forEach(p => p.classList.remove('active'));
      pill.classList.add('active');
      filterAndRender();
    });
  });
}

function setupSearch() {
  const searchInput = document.getElementById('searchInput');
  if (!searchInput) return;
  
  searchInput.addEventListener('input', () => {
    filterAndRender();
  });
}

function filterAndRender() {
  const searchInput = document.getElementById('searchInput');
  const query = (searchInput ? searchInput.value : '').toLowerCase().trim();

  const filtered = allPosts.filter(post => {
    const matchesSeries = activeSeries === 'ALL' || (post.series || '其他') === activeSeries;
    const matchesTag = activeTag === 'ALL' || (post.tags && post.tags.includes(activeTag));
    const matchesQuery = !query ||
      post.title.toLowerCase().includes(query) ||
      (post.subtitle && post.subtitle.toLowerCase().includes(query)) ||
      (post.excerpt && post.excerpt.toLowerCase().includes(query)) ||
      (post.tags && post.tags.some(t => t.toLowerCase().includes(query)));
    return matchesSeries && matchesTag && matchesQuery;
  });

  renderPosts(filtered);
}

function renderPosts(posts) {
  const container = document.getElementById('postsGrid');
  if (!container) return;

  if (posts.length === 0) {
    container.innerHTML = `
      <div style="background: var(--bg-card); border: 1px dashed var(--border-color); border-radius: 12px; padding: 3rem; text-align: center; color: var(--text-muted); font-family: var(--font-mono);">
        $ find . -name "*.post" -not -found<br/>
        未找到匹配的文章，请尝试其他关键词。
      </div>
    `;
    return;
  }

  container.innerHTML = posts.map(post => `
    <a href="post.html?id=${post.id}" class="post-card">
      <div>
        <div class="post-meta">
          <span>📅 ${post.date}</span>
          <span>👤 ${post.author || 'mrhs121'}</span>
          ${post.series ? `<span class="post-series-badge">📚 ${post.series}</span>` : ''}
        </div>
        <h2 class="post-title">${post.title}</h2>
        ${post.subtitle ? `<div class="post-subtitle">${post.subtitle}</div>` : ''}
        <p class="post-excerpt">${post.excerpt || ''}</p>
      </div>
      <div class="post-tags">
        ${(post.tags || []).map(t => `<span class="post-tag">#${t}</span>`).join('')}
      </div>
    </a>
  `).join('');
}

document.addEventListener('DOMContentLoaded', () => {
  initTheme();
  loadPosts();
});
