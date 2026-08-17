# huangsheng.dev — Engineering Blog

Huang Sheng (mrhs121) 的个人开源技术博客，专注于分布式计算、存储引擎内核与湖仓一体表格式（Apache Spark / Flink / Apache Iceberg）底层深度剖析。

## 🌟 特性
- **极客终端风格（Geek / Hacker Aesthetic）**：基于 `JetBrains Mono` / `Inter` 排版与暗黑 Slate 配色。
- **开箱即用（Zero Build Dependencies）**：纯原生 HTML5 + Modern CSS + Vanilla JS，直接托管在 GitHub Pages。
- **完善的阅读体验**：
  - 自动生成文章目录导航（Interactive TOC with ScrollSpy）
  - 顶部滚动阅读进度条（Reading Progress Bar）
  - 代码块语法高亮（Prism.js）与一键复制代码
  - 动态标签过滤（Tag Filtering）与实时全文搜索

## 📝 如何发布新文章
1. 在 `posts/` 目录下添加 Markdown 文件（例如 `posts/2026-xx-xx-your-title.md`）。
2. 在 `posts/manifest.json` 中追加文章元数据：
```json
{
  "id": "your-article-id",
  "title": "文章标题",
  "subtitle": "副标题（可选）",
  "date": "2026-xx-xx",
  "author": "Huang Sheng (mrhs121)",
  "tags": ["Apache Iceberg", "Spark"],
  "file": "posts/2026-xx-xx-your-title.md",
  "excerpt": "文章摘要介绍..."
}
```
3. `git push` 到 GitHub 即可自动通过 GitHub Pages 上线生效。
