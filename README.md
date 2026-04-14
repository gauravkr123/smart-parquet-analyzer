# Parquet Explorer (Web)

A fully client-side Parquet file explorer that runs entirely in your browser. No server, no uploads, no data leaves your machine. Powered by [DuckDB-WASM](https://duckdb.org/docs/api/wasm/overview.html).

**Live demo:** `https://<your-username>.github.io/parquet-explorer-web/`

## Deploy to GitHub Pages

```bash
# 1. Create a repo on GitHub, then:
cd parquet-explorer-web
git init
git add .
git commit -m "Initial commit"
git remote add origin git@github.com:<your-username>/parquet-explorer-web.git
git push -u origin main

# 2. Go to repo Settings → Pages → Source: Deploy from a branch → main / root
# 3. Your site will be live in ~1 minute
```

Or just open `index.html` locally in any modern browser — it works without a server.

## Features

### File Management
- **Drag & drop** parquet files or click to browse
- Multiple files loaded simultaneously, each queryable as a SQL table
- All processing happens in-browser via DuckDB-WASM

### Data Viewer
- Paginated table with configurable page size
- Sortable columns (click headers)
- Visual filter builder (12 operators)
- Full-text search across all columns
- **Hide empty columns** toggle — auto-detects and hides columns where all values are null or empty
- Export to CSV or JSON

### Schema Inspector
- Column names, data types (color-coded badges), nullable info

### SQL Query Engine
- Full SQL support via DuckDB-WASM (same SQL dialect as DuckDB)
- Ctrl+Enter to run queries
- Query history and bookmarks (stored in browser localStorage)
- Clickable table name chips

### Statistics & Profiling
- Per-column stats: count, nulls, unique, min, max, mean, std dev
- Click any column for percentiles (p25–p99), value frequency, histogram

### Charts
- Bar, Line, Scatter, Pie, Doughnut charts via Chart.js
- Configurable aggregations

### Parquet Metadata
- Row groups, compression, column physical/logical types
- Uses DuckDB's `parquet_metadata()` and `parquet_schema()` functions

### Transforms
- Rename, cast, drop, computed columns, filter rows, deduplicate
- Each transform creates a new queryable view

### Customization
- Dark/Light theme, 6 accent colors
- Table density and font size options
- Configurable null display and number formatting
- All settings persist in localStorage

## Tech Stack

| Component | Technology |
|-----------|-----------|
| SQL Engine | DuckDB-WASM (in-browser) |
| Charts | Chart.js 4.x (CDN) |
| Frontend | Vanilla JavaScript ES Modules |
| Styling | CSS Custom Properties |

## Project Structure

```
parquet-explorer-web/
├── index.html      # Single-page application
├── css/
│   └── styles.css  # Themed styles
└── js/
    └── app.js      # Complete application (ES module)
```

## Browser Requirements

- Modern browser with WebAssembly support (Chrome 80+, Firefox 78+, Safari 14+, Edge 80+)
- JavaScript ES modules support
- ~15MB DuckDB-WASM download on first load (cached by browser)
