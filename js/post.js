// Post Article Renderer & Interactive Features

document.addEventListener('DOMContentLoaded', async () => {
  initTheme();
  initProgressBar();
  await loadPostContent();
});

function initProgressBar() {
  const bar = document.getElementById('progressBar');
  if (!bar) return;

  window.addEventListener('scroll', () => {
    const scrollTop = window.scrollY || document.documentElement.scrollTop;
    const docHeight = document.documentElement.scrollHeight - document.documentElement.clientHeight;
    const progress = docHeight > 0 ? (scrollTop / docHeight) * 100 : 0;
    bar.style.width = `${Math.min(progress, 100)}%`;
  });
}

async function loadPostContent() {
  const contentEl = document.getElementById('postContent');
  const titleEl = document.getElementById('postTitle');
  const metaEl = document.getElementById('postMeta');
  const tocEl = document.getElementById('tocList');
  if (!contentEl) return;

  const urlParams = new URLSearchParams(window.location.search);
  const postId = urlParams.get('id') || 'spark-iceberg-orc-write-pipeline';

  try {
    const manifestRes = await fetch('posts/manifest.json');
    const manifest = await manifestRes.json();
    const postInfo = manifest.find(p => p.id === postId) || manifest[0];

    if (titleEl && postInfo) {
      document.title = `${postInfo.title} | Huang Sheng's Blog`;
      titleEl.innerText = postInfo.title;
    }

    if (metaEl && postInfo) {
      metaEl.innerHTML = `
        <div class="article-meta-item"><span>📅 ${postInfo.date}</span></div>
        <div class="article-meta-item"><span>👤 ${postInfo.author || 'mrhs121'}</span></div>
        ${(postInfo.tags || []).map(t => `<span class="post-tag">#${t}</span>`).join('')}
      `;
    }

    const postFilePath = postInfo ? postInfo.file : `posts/${postId}.md`;
    const mdRes = await fetch(postFilePath);
    if (!mdRes.ok) throw new Error(`HTTP error! status: ${mdRes.status}`);
    const mdText = await mdRes.text();

    // Configure marked options
    marked.setOptions({
      highlight: function(code, lang) {
        if (Prism.languages[lang]) {
          return Prism.highlight(code, Prism.languages[lang], lang);
        } else {
          return Prism.highlight(code, Prism.languages.markup || Prism.languages.clike, 'markup');
        }
      },
      gfm: true,
      breaks: true
    });

    const parsedHtml = marked.parse(mdText);
    contentEl.innerHTML = parsedHtml;

    // Enhance Code Blocks
    enhanceCodeBlocks(contentEl);

    // Generate Table of Contents
    generateTOC(contentEl, tocEl);

    // Initialize Prism
    if (window.Prism) {
      Prism.highlightAllUnder(contentEl);
    }

    // Render Mermaid Diagrams
    renderMermaidDiagrams(contentEl);
  } catch (err) {
    console.error('Failed to load post content:', err);
    contentEl.innerHTML = `
      <div style="background: var(--bg-card); border: 1px dashed var(--border-color); border-radius: 12px; padding: 3rem; text-align: center; color: var(--text-muted); font-family: var(--font-mono);">
        $ curl -s ${postId}.md ──> 404 Not Found<br/>
        文章加载失败，请返回 <a href="index.html" style="color: var(--accent-cyan);">首页</a> 浏览其他内容。
      </div>
    `;
  }
}

function enhanceCodeBlocks(container) {
  const pres = container.querySelectorAll('pre');
  pres.forEach((pre) => {
    const code = pre.querySelector('code');
    const langClass = code ? Array.from(code.classList).find(c => c.startsWith('language-')) : null;

    // Mermaid blocks are rendered as diagrams, skip code-block decoration
    if (langClass === 'language-mermaid') return;
    const lang = langClass ? langClass.replace('language-', '').toUpperCase() : 'CODE';

    const wrapper = document.createElement('div');
    wrapper.className = 'code-block-wrapper';

    const header = document.createElement('div');
    header.className = 'code-header';
    header.innerHTML = `
      <span>${lang}</span>
      <button class="copy-btn">📋 复制</button>
    `;

    pre.parentNode.insertBefore(wrapper, pre);
    wrapper.appendChild(header);
    wrapper.appendChild(pre);

    const copyBtn = header.querySelector('.copy-btn');
    copyBtn.addEventListener('click', () => {
      const textToCopy = code ? code.innerText : pre.innerText;
      navigator.clipboard.writeText(textToCopy).then(() => {
        copyBtn.innerText = '✅ 已复制';
        setTimeout(() => {
          copyBtn.innerText = '📋 复制';
        }, 2000);
      }).catch(err => {
        console.error('Copy failed', err);
      });
    });
  });
}

function renderMermaidDiagrams(container) {
  if (!window.mermaid) return;
  const mermaidBlocks = container.querySelectorAll('pre > code.language-mermaid');
  if (mermaidBlocks.length === 0) return;

  const currentTheme = document.documentElement.getAttribute('data-theme') || 'dark';
  mermaid.initialize({ startOnLoad: false, theme: currentTheme === 'dark' ? 'dark' : 'default', securityLevel: 'loose' });

  const nodes = [];
  mermaidBlocks.forEach((code, idx) => {
    const div = document.createElement('div');
    div.className = 'mermaid';
    div.setAttribute('data-mermaid-id', `mermaid-diagram-${idx}`);
    div.textContent = code.innerText;
    code.parentNode.replaceWith(div);
    nodes.push(div);
  });

  mermaid.run({ nodes }).catch(err => {
    console.error('Mermaid rendering failed:', err);
  });
}

function generateTOC(contentEl, tocContainer) {
  if (!tocContainer) return;
  const headings = contentEl.querySelectorAll('h2, h3');
  if (headings.length === 0) {
    document.querySelector('.toc-sidebar')?.remove();
    return;
  }

  const tocItems = [];
  headings.forEach((h, idx) => {
    const id = h.id || `heading-${idx}`;
    h.id = id;
    const depth = h.tagName.toLowerCase() === 'h2' ? 2 : 3;
    tocItems.push({ id, text: h.innerText, depth });
  });

  tocContainer.innerHTML = tocItems.map(item => `
    <li class="toc-item depth-${item.depth}">
      <a href="#${item.id}" class="toc-link" data-id="${item.id}">${item.text}</a>
    </li>
  `).join('');

  // ScrollSpy
  setupScrollSpy(headings);
}

function setupScrollSpy(headings) {
  const links = document.querySelectorAll('.toc-link');
  if (!links.length) return;

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const id = entry.target.getAttribute('id');
        links.forEach(l => {
          if (l.getAttribute('data-id') === id) {
            l.classList.add('active');
          } else {
            l.classList.remove('active');
          }
        });
      }
    });
  }, {
    rootMargin: '0px 0px -60% 0px',
    threshold: 0
  });

  headings.forEach(h => observer.observe(h));
}
