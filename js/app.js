// ==========================================================================
//  Parquet Explorer — Client-Side DuckDB-WASM Application
//  Fully self-contained ES module. No backend server required.
// ==========================================================================

import * as duckdb from 'https://cdn.jsdelivr.net/npm/@duckdb/duckdb-wasm@1.29.0/+esm';

// ==========================================================================
//  Utility Functions
// ==========================================================================

function generateUUID() {
  return crypto.randomUUID();
}

function formatFileSize(bytes) {
  if (bytes == null || isNaN(bytes)) return '0 B';
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0;
  let size = bytes;
  while (size >= 1024 && i < units.length - 1) {
    size /= 1024;
    i++;
  }
  return `${size.toFixed(i === 0 ? 0 : 1)} ${units[i]}`;
}

function formatNumber(num) {
  if (num == null || isNaN(num)) return '0';
  return Number(num).toLocaleString();
}

function formatCellValue(val, nullDisplay) {
  if (val === null || val === undefined) {
    return `<span class="null-value">${escapeHtml(nullDisplay || 'NULL')}</span>`;
  }
  if (typeof val === 'bigint') {
    return escapeHtml(String(Number(val)));
  }
  if (val instanceof Date) {
    return escapeHtml(val.toISOString());
  }
  if (typeof val === 'object') {
    return escapeHtml(JSON.stringify(val));
  }
  return escapeHtml(String(val));
}

function escapeHtml(str) {
  if (str == null) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}

function isNumericType(type) {
  if (!type) return false;
  const t = type.toLowerCase();
  return /int|float|double|decimal|numeric|real|number|bigint|hugeint|smallint|tinyint|ubigint|uinteger|usmallint|utinyint/.test(t);
}

function isStringType(type) {
  if (!type) return false;
  const t = type.toLowerCase();
  return /varchar|string|text|utf8|char|blob/.test(t);
}

function isDateType(type) {
  if (!type) return false;
  const t = type.toLowerCase();
  return /date|time|timestamp|interval/.test(t);
}

function isBoolType(type) {
  if (!type) return false;
  return /bool/i.test(type);
}

function debounce(fn, delay) {
  let timer;
  return function (...args) {
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(this, args), delay);
  };
}

function getTypeBadgeClass(type) {
  if (!type) return 'type-badge-other';
  const t = type.toLowerCase();
  if (isNumericType(t)) {
    if (/float|double|decimal|real|numeric/.test(t)) return 'type-badge-float';
    return 'type-badge-int';
  }
  if (isStringType(t)) return 'type-badge-string';
  if (isBoolType(t)) return 'type-badge-bool';
  if (isDateType(t)) return 'type-badge-date';
  return 'type-badge-other';
}

function truncateText(str, maxLen = 50) {
  if (!str) return '';
  const s = String(str);
  if (s.length <= maxLen) return s;
  return s.substring(0, maxLen) + '...';
}

function convertValue(val) {
  if (typeof val === 'bigint') {
    if (val >= Number.MIN_SAFE_INTEGER && val <= Number.MAX_SAFE_INTEGER) {
      return Number(val);
    }
    return Number(val);
  }
  if (val instanceof Date) return val.toISOString();
  if (val !== null && typeof val === 'object' && val.constructor && val.constructor.name === 'Vector') {
    return val.toJSON();
  }
  return val;
}

function convertRow(row, fields) {
  const obj = {};
  for (const field of fields) {
    obj[field.name] = convertValue(row[field.name]);
  }
  return obj;
}

function sanitizeViewName(fileName) {
  return fileName.replace(/\./g, '_').replace(/-/g, '_').replace(/\s/g, '_');
}

function makeSerializable(val) {
  if (val === null || val === undefined) return null;
  if (typeof val === 'bigint') return Number(val);
  if (typeof val === 'number') {
    if (!isFinite(val)) return null;
    return val;
  }
  if (val instanceof Date) return val.toISOString();
  if (typeof val === 'object') return JSON.stringify(val);
  return val;
}

// ==========================================================================
//  Toast Notifications
// ==========================================================================

const Toast = {
  _container: null,

  _getContainer() {
    if (!this._container) {
      this._container = document.getElementById('toast-container');
    }
    return this._container;
  },

  _icons: {
    success: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M20 6L9 17l-5-5"/></svg>',
    error: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>',
    info: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="16" x2="12" y2="12"/><line x1="12" y1="8" x2="12.01" y2="8"/></svg>',
    warning: '<svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M10.29 3.86L1.82 18a2 2 0 001.71 3h16.94a2 2 0 001.71-3L13.71 3.86a2 2 0 00-3.42 0z"/><line x1="12" y1="9" x2="12" y2="13"/><line x1="12" y1="17" x2="12.01" y2="17"/></svg>',
  },

  show(message, type = 'info', duration = 4000) {
    const container = this._getContainer();
    if (!container) return;

    const el = document.createElement('div');
    el.className = `toast toast-${type}`;
    el.innerHTML = `
      <span class="toast-icon">${this._icons[type] || this._icons.info}</span>
      <span class="toast-message">${escapeHtml(message)}</span>
      <button class="toast-close" aria-label="Close">&times;</button>
    `;
    container.appendChild(el);

    requestAnimationFrame(() => {
      el.classList.add('toast-enter');
    });

    const dismiss = () => {
      el.classList.remove('toast-enter');
      el.classList.add('toast-exit');
      setTimeout(() => el.remove(), 350);
    };

    el.querySelector('.toast-close').addEventListener('click', dismiss);
    if (duration > 0) {
      setTimeout(dismiss, duration);
    }
  },

  success(msg) { this.show(msg, 'success'); },
  error(msg) { this.show(msg, 'error', 6000); },
  info(msg) { this.show(msg, 'info'); },
  warning(msg) { this.show(msg, 'warning'); },
};

// ==========================================================================
//  Cell Expand Modal
// ==========================================================================

function showCellExpand(columnName, rawValue) {
  const existing = document.getElementById('cell-expand-modal');
  if (existing) existing.remove();

  const overlay = document.createElement('div');
  overlay.id = 'cell-expand-modal';
  overlay.className = 'cell-expand-overlay';
  overlay.innerHTML = `
    <div class="cell-expand-box">
      <div class="cell-expand-header">
        <span>${escapeHtml(columnName)}</span>
        <button class="modal-close" data-action="close">&times;</button>
      </div>
      <div class="cell-expand-body">${escapeHtml(rawValue)}</div>
      <div class="cell-expand-footer">
        <button class="btn btn-secondary btn-sm" id="btn-copy-cell">Copy</button>
        <button class="btn btn-ghost btn-sm" data-action="close">Close</button>
      </div>
    </div>
  `;
  document.body.appendChild(overlay);

  overlay.addEventListener('click', (e) => {
    if (e.target === overlay || e.target.closest('[data-action="close"]')) {
      overlay.remove();
    }
  });

  overlay.querySelector('#btn-copy-cell').addEventListener('click', () => {
    navigator.clipboard.writeText(rawValue).then(() => {
      Toast.success('Copied to clipboard');
    }).catch(() => {
      Toast.warning('Copy failed — select text manually');
    });
  });
}

// ==========================================================================
//  State Management
// ==========================================================================

const State = {
  files: [],
  activeFileId: null,
  activeTab: 'data',
  currentPage: 1,
  pageSize: 50,
  sortCol: null,
  sortDir: 'asc',
  filters: [],
  searchQuery: '',
  hideEmptyColumns: false,
  hiddenColumns: [],
  _emptyColumnsCache: {},
  bookmarks: [],
  settings: {
    theme: 'light',
    accentColor: 'blue',
    density: 'normal',
    fontSize: 'medium',
    pageSize: 50,
    nullDisplay: 'NULL',
    numberFormat: 'comma',
  },
  queryHistory: [],
  transforms: [],
  _schema: null,
  _totalRows: 0,

  loadSettings() {
    try {
      const raw = localStorage.getItem('parquet-explorer-settings');
      if (raw) {
        const saved = JSON.parse(raw);
        Object.assign(this.settings, saved);
        this.pageSize = this.settings.pageSize || 50;
      }
    } catch (e) { /* ignore */ }
  },

  saveSettings() {
    try {
      localStorage.setItem('parquet-explorer-settings', JSON.stringify(this.settings));
    } catch (e) { /* ignore */ }
  },

  loadBookmarks() {
    try {
      const raw = localStorage.getItem('parquet-explorer-bookmarks');
      if (raw) this.bookmarks = JSON.parse(raw);
    } catch (e) { this.bookmarks = []; }
  },

  saveBookmarks() {
    try {
      localStorage.setItem('parquet-explorer-bookmarks', JSON.stringify(this.bookmarks));
    } catch (e) { /* ignore */ }
  },

  loadQueryHistory() {
    try {
      const raw = localStorage.getItem('parquet-explorer-query-history');
      if (raw) this.queryHistory = JSON.parse(raw);
    } catch (e) { this.queryHistory = []; }
  },

  saveQueryHistory() {
    try {
      localStorage.setItem('parquet-explorer-query-history', JSON.stringify(this.queryHistory));
    } catch (e) { /* ignore */ }
  },

  addQueryToHistory(sql) {
    const trimmed = sql.trim();
    if (!trimmed) return;
    this.queryHistory = this.queryHistory.filter(q => q !== trimmed);
    this.queryHistory.unshift(trimmed);
    if (this.queryHistory.length > 20) this.queryHistory = this.queryHistory.slice(0, 20);
    this.saveQueryHistory();
  },

  resetPagination() {
    this.currentPage = 1;
    this.sortCol = null;
    this.sortDir = 'asc';
    this.filters = [];
    this.searchQuery = '';
    this.hideEmptyColumns = false;
    this.hiddenColumns = [];
    this._schema = null;
    this._totalRows = 0;
    this._emptyColumnsCache = {};
  },
};

// ==========================================================================
//  DB Layer (DuckDB-WASM)
// ==========================================================================

const DB = {
  db: null,
  conn: null,
  files: {},

  _ready: false,

  async init() {
    const statusEl = document.getElementById('db-status');
    try {
      if (statusEl) statusEl.textContent = 'Loading DuckDB bundles...';
      const BUNDLES = duckdb.getJsDelivrBundles();
      const bundle = await duckdb.selectBundle(BUNDLES);

      if (statusEl) statusEl.textContent = 'Starting worker...';
      // Browsers block new Worker() from cross-origin URLs.
      // Workaround: create a blob that imports the remote script.
      const workerUrl = bundle.mainWorker;
      const blob = new Blob(
        [`importScripts("${workerUrl}");`],
        { type: 'text/javascript' }
      );
      const worker = new Worker(URL.createObjectURL(blob));

      if (statusEl) statusEl.textContent = 'Instantiating DuckDB...';
      const logger = new duckdb.ConsoleLogger();
      this.db = new duckdb.AsyncDuckDB(logger, worker);
      await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);

      if (statusEl) statusEl.textContent = 'Connecting...';
      this.conn = await this.db.connect();
      this._ready = true;
      if (statusEl) statusEl.textContent = 'Ready';
    } catch (err) {
      console.error('DuckDB-WASM init failed:', err);
      if (statusEl) statusEl.textContent = 'DuckDB failed';
      throw err;
    }
  },

  async registerFile(browserFile) {
    const fileId = generateUUID();
    const fileName = browserFile.name;
    const viewName = sanitizeViewName(fileName);

    const buffer = new Uint8Array(await browserFile.arrayBuffer());
    await this.db.registerFileBuffer(fileName, buffer);

    await this.conn.query(`CREATE OR REPLACE VIEW "${viewName}" AS SELECT * FROM '${fileName}'`);

    let numRows = 0;
    let numColumns = 0;
    try {
      const countResult = await this.conn.query(`SELECT COUNT(*) as cnt FROM "${viewName}"`);
      const countRows = countResult.toArray();
      numRows = Number(countRows[0].cnt);
    } catch (e) { /* ignore */ }

    try {
      const descResult = await this.conn.query(`DESCRIBE "${viewName}"`);
      numColumns = descResult.toArray().length;
    } catch (e) { /* ignore */ }

    const info = {
      id: fileId,
      name: fileName.replace(/\.[^/.]+$/, ''),
      fileName: fileName,
      viewName: viewName,
      numRows: numRows,
      numColumns: numColumns,
      sizeBytes: browserFile.size,
    };
    this.files[fileId] = info;
    return info;
  },

  async removeFile(fileId) {
    const info = this.files[fileId];
    if (!info) return;
    try {
      await this.conn.query(`DROP VIEW IF EXISTS "${info.viewName}"`);
    } catch (e) { /* ignore */ }
    try {
      await this.db.dropFile(info.fileName);
    } catch (e) { /* ignore */ }
    delete this.files[fileId];
  },

  async getSchema(fileId) {
    const viewName = this._getViewName(fileId);
    if (!viewName) throw new Error('File not found');
    const result = await this.conn.query(`DESCRIBE "${viewName}"`);
    const rows = result.toArray();
    return rows.map(r => ({
      name: r.column_name,
      type: r.column_type,
      nullable: r.null === 'YES',
    }));
  },

  async getData(fileId, { page = 1, pageSize = 50, sortCol = null, sortDir = 'asc', filters = [], hiddenColumns = [] } = {}) {
    const viewName = this._getViewName(fileId);
    if (!viewName) throw new Error('File not found');

    const whereClauses = this._buildWhereClauses(filters);
    const whereSql = whereClauses.length > 0 ? ' WHERE ' + whereClauses.join(' AND ') : '';
    const orderSql = sortCol ? ` ORDER BY "${sortCol}" ${sortDir === 'desc' ? 'DESC' : 'ASC'}` : '';
    const offset = (page - 1) * pageSize;

    const countResult = await this.conn.query(`SELECT COUNT(*) as cnt FROM "${viewName}"${whereSql}`);
    const totalRows = Number(countResult.toArray()[0].cnt);
    const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));

    let selectCols = '*';
    if (hiddenColumns.length > 0) {
      selectCols = `* EXCLUDE (${hiddenColumns.map(c => `"${c}"`).join(', ')})`;
    }

    const sql = `SELECT ${selectCols} FROM "${viewName}"${whereSql}${orderSql} LIMIT ${pageSize} OFFSET ${offset}`;
    const result = await this.conn.query(sql);
    const fields = result.schema.fields;
    const columns = fields.map(f => f.name);
    const data = result.toArray().map(row => convertRow(row, fields));

    return { columns, data, totalRows, totalPages, page, pageSize };
  },

  async search(fileId, query) {
    const viewName = this._getViewName(fileId);
    if (!viewName) throw new Error('File not found');

    const schema = await this.getSchema(fileId);
    const stringCols = schema.filter(c => isStringType(c.type));

    if (stringCols.length === 0) {
      const allCols = schema.map(c => c.name);
      const conditions = allCols.map(c => `CAST("${c}" AS VARCHAR) ILIKE '%${query.replace(/'/g, "''")}%'`).join(' OR ');
      const sql = `SELECT * FROM "${viewName}" WHERE ${conditions} LIMIT 200`;
      const result = await this.conn.query(sql);
      const fields = result.schema.fields;
      return {
        columns: fields.map(f => f.name),
        data: result.toArray().map(row => convertRow(row, fields)),
        rowCount: result.toArray().length,
      };
    }

    const escapedQuery = query.replace(/'/g, "''");
    const conditions = stringCols.map(c => `CAST("${c.name}" AS VARCHAR) ILIKE '%${escapedQuery}%'`).join(' OR ');
    const sql = `SELECT * FROM "${viewName}" WHERE ${conditions} LIMIT 200`;
    const result = await this.conn.query(sql);
    const fields = result.schema.fields;
    return {
      columns: fields.map(f => f.name),
      data: result.toArray().map(row => convertRow(row, fields)),
      rowCount: result.toArray().length,
    };
  },

  async runQuery(sql) {
    const start = performance.now();
    const result = await this.conn.query(sql);
    const elapsed = ((performance.now() - start) / 1000).toFixed(4);
    const fields = result.schema.fields;
    const columns = fields.map(f => ({ name: f.name, type: String(f.type) }));
    const rows = result.toArray();
    const data = rows.map(row => convertRow(row, fields));
    return {
      columns,
      data,
      rowCount: data.length,
      executionTime: parseFloat(elapsed),
    };
  },

  async getStats(fileId) {
    const viewName = this._getViewName(fileId);
    if (!viewName) throw new Error('File not found');

    const schema = await this.getSchema(fileId);
    const stats = {};

    for (const col of schema) {
      const colName = col.name;
      const colType = col.type;

      try {
        const basicResult = await this.conn.query(`
          SELECT
            COUNT("${colName}") as non_null_count,
            COUNT(*) - COUNT("${colName}") as null_count,
            COUNT(DISTINCT "${colName}") as unique_count
          FROM "${viewName}"
        `);
        const basicRow = basicResult.toArray()[0];
        const colStats = {
          count: Number(basicRow.non_null_count),
          nulls: Number(basicRow.null_count),
          unique: Number(basicRow.unique_count),
        };

        if (isNumericType(colType)) {
          try {
            const numResult = await this.conn.query(`
              SELECT
                MIN("${colName}") as min_val,
                MAX("${colName}") as max_val,
                AVG("${colName}") as mean_val,
                STDDEV("${colName}") as std_val,
                MEDIAN("${colName}") as median_val
              FROM "${viewName}"
            `);
            const numRow = numResult.toArray()[0];
            colStats.min = makeSerializable(numRow.min_val);
            colStats.max = makeSerializable(numRow.max_val);
            colStats.mean = makeSerializable(numRow.mean_val);
            colStats.std = makeSerializable(numRow.std_val);
            colStats.median = makeSerializable(numRow.median_val);
          } catch (e) { /* ignore */ }

          try {
            const histResult = await this.conn.query(`
              SELECT HISTOGRAM("${colName}") as hist FROM "${viewName}"
            `);
            const histRow = histResult.toArray()[0];
            if (histRow && histRow.hist) {
              const histMap = histRow.hist;
              if (histMap instanceof Map || typeof histMap === 'object') {
                const entries = histMap instanceof Map ? Array.from(histMap.entries()) : Object.entries(histMap);
                const sortedEntries = entries.sort((a, b) => {
                  const aNum = parseFloat(String(a[0]));
                  const bNum = parseFloat(String(b[0]));
                  if (!isNaN(aNum) && !isNaN(bNum)) return aNum - bNum;
                  return String(a[0]).localeCompare(String(b[0]));
                });
                const labels = sortedEntries.map(e => String(e[0]));
                const counts = sortedEntries.map(e => Number(e[1]));
                if (labels.length > 0) {
                  colStats.histogram = { labels, counts };
                }
              }
            }
          } catch (e) { /* ignore */ }
        } else if (isStringType(colType)) {
          try {
            const topResult = await this.conn.query(`
              SELECT "${colName}" as val, COUNT(*) as cnt
              FROM "${viewName}"
              WHERE "${colName}" IS NOT NULL
              GROUP BY "${colName}"
              ORDER BY cnt DESC
              LIMIT 10
            `);
            const topRows = topResult.toArray();
            colStats.top_values = topRows.map(r => ({
              value: makeSerializable(r.val),
              count: Number(r.cnt),
            }));
          } catch (e) { /* ignore */ }

          try {
            const minMaxResult = await this.conn.query(`
              SELECT MIN("${colName}") as min_val, MAX("${colName}") as max_val
              FROM "${viewName}" WHERE "${colName}" IS NOT NULL
            `);
            const mmRow = minMaxResult.toArray()[0];
            colStats.min = makeSerializable(mmRow.min_val);
            colStats.max = makeSerializable(mmRow.max_val);
          } catch (e) { /* ignore */ }
        } else if (isDateType(colType)) {
          try {
            const dtResult = await this.conn.query(`
              SELECT MIN("${colName}") as min_val, MAX("${colName}") as max_val
              FROM "${viewName}" WHERE "${colName}" IS NOT NULL
            `);
            const dtRow = dtResult.toArray()[0];
            colStats.min = makeSerializable(dtRow.min_val);
            colStats.max = makeSerializable(dtRow.max_val);
          } catch (e) { /* ignore */ }
        }

        stats[colName] = colStats;
      } catch (e) {
        stats[colName] = { count: 0, nulls: 0, unique: 0, error: e.message };
      }
    }

    return { stats };
  },

  async getColumnStats(fileId, column) {
    const viewName = this._getViewName(fileId);
    if (!viewName) throw new Error('File not found');

    const schema = await this.getSchema(fileId);
    const colMeta = schema.find(c => c.name === column);
    if (!colMeta) throw new Error(`Column not found: ${column}`);

    const colType = colMeta.type;

    const basicResult = await this.conn.query(`
      SELECT
        COUNT("${column}") as non_null_count,
        COUNT(*) as total_count,
        COUNT(*) - COUNT("${column}") as null_count,
        COUNT(DISTINCT "${column}") as unique_count
      FROM "${viewName}"
    `);
    const b = basicResult.toArray()[0];
    const total = Number(b.total_count);
    const nulls = Number(b.null_count);

    const result = {
      column: column,
      dtype: colType,
      count: Number(b.non_null_count),
      nulls: nulls,
      null_pct: total > 0 ? parseFloat(((nulls / total) * 100).toFixed(2)) : 0,
      unique: Number(b.unique_count),
    };

    if (isNumericType(colType)) {
      try {
        const numResult = await this.conn.query(`
          SELECT
            MIN("${column}") as min_val,
            MAX("${column}") as max_val,
            AVG("${column}") as mean_val,
            STDDEV("${column}") as std_val,
            MEDIAN("${column}") as median_val,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY "${column}") as p25,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY "${column}") as p50,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY "${column}") as p75,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "${column}") as p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY "${column}") as p99
          FROM "${viewName}"
        `);
        const nr = numResult.toArray()[0];
        result.min = makeSerializable(nr.min_val);
        result.max = makeSerializable(nr.max_val);
        result.mean = makeSerializable(nr.mean_val);
        result.std = makeSerializable(nr.std_val);
        result.median = makeSerializable(nr.median_val);
        result.p25 = makeSerializable(nr.p25);
        result.p50 = makeSerializable(nr.p50);
        result.p75 = makeSerializable(nr.p75);
        result.p95 = makeSerializable(nr.p95);
        result.p99 = makeSerializable(nr.p99);
      } catch (e) { /* ignore */ }

      try {
        const histResult = await this.conn.query(`
          SELECT HISTOGRAM("${column}") as hist FROM "${viewName}"
        `);
        const hr = histResult.toArray()[0];
        if (hr && hr.hist) {
          const histMap = hr.hist;
          const entries = histMap instanceof Map ? Array.from(histMap.entries()) : Object.entries(histMap);
          const sortedEntries = entries.sort((a, b) => {
            const aNum = parseFloat(String(a[0]));
            const bNum = parseFloat(String(b[0]));
            if (!isNaN(aNum) && !isNaN(bNum)) return aNum - bNum;
            return String(a[0]).localeCompare(String(b[0]));
          });
          result.histogram = {
            labels: sortedEntries.map(e => String(e[0])),
            counts: sortedEntries.map(e => Number(e[1])),
          };
        }
      } catch (e) { /* ignore */ }
    }

    try {
      const freqResult = await this.conn.query(`
        SELECT "${column}" as val, COUNT(*) as cnt
        FROM "${viewName}"
        WHERE "${column}" IS NOT NULL
        GROUP BY "${column}"
        ORDER BY cnt DESC
        LIMIT 50
      `);
      const freqRows = freqResult.toArray();
      result.value_frequency = freqRows.map(r => ({
        value: makeSerializable(r.val),
        count: Number(r.cnt),
      }));
    } catch (e) {
      result.value_frequency = [];
    }

    return result;
  },

  async getMetadata(fileId) {
    const info = this.files[fileId];
    if (!info) throw new Error('File not found');

    let metaData = [];
    let schemaData = [];

    try {
      const metaResult = await this.conn.query(`SELECT * FROM parquet_metadata('${info.fileName}')`);
      metaData = metaResult.toArray().map(r => convertRow(r, metaResult.schema.fields));
    } catch (e) { /* ignore */ }

    try {
      const schemaResult = await this.conn.query(`SELECT * FROM parquet_schema('${info.fileName}')`);
      schemaData = schemaResult.toArray().map(r => convertRow(r, schemaResult.schema.fields));
    } catch (e) { /* ignore */ }

    const numRowGroups = metaData.length > 0 ? new Set(metaData.map(m => m.row_group_id)).size : 0;
    const rowGroups = [];
    const seenRg = new Set();
    for (const m of metaData) {
      const rgId = m.row_group_id;
      if (rgId !== undefined && !seenRg.has(rgId)) {
        seenRg.add(rgId);
        rowGroups.push({
          index: rgId,
          num_rows: m.row_group_num_rows || m.num_values,
          total_byte_size: m.total_compressed_size || 0,
        });
      }
    }

    const columnsFromMeta = [];
    const seenCols = new Set();
    for (const m of metaData) {
      const name = m.path_in_schema;
      if (name && !seenCols.has(name)) {
        seenCols.add(name);
        columnsFromMeta.push({
          name: name,
          physical_type: m.type || null,
          compression: m.compression || null,
          total_compressed_size: m.total_compressed_size || 0,
          total_uncompressed_size: m.total_uncompressed_size || 0,
          encodings: m.encodings || null,
        });
      }
    }

    const columnsFromSchema = schemaData.map(s => ({
      name: s.name,
      type: s.type,
      type_length: s.type_length,
      repetition_type: s.repetition_type,
      num_children: s.num_children,
      converted_type: s.converted_type,
      logical_type: s.logical_type,
    }));

    return {
      file_id: fileId,
      file_name: info.fileName,
      file_size_bytes: info.sizeBytes,
      num_rows: info.numRows,
      num_columns: info.numColumns,
      num_row_groups: numRowGroups,
      row_groups: rowGroups,
      columns_meta: columnsFromMeta,
      columns_schema: columnsFromSchema,
      raw_metadata: metaData.slice(0, 100),
    };
  },

  async getEmptyColumns(fileId) {
    const viewName = this._getViewName(fileId);
    if (!viewName) return [];

    const schema = await this.getSchema(fileId);
    const emptyColumns = [];

    for (const col of schema) {
      try {
        const result = await this.conn.query(`
          SELECT
            COUNT("${col.name}") as non_null,
            SUM(CASE WHEN CAST("${col.name}" AS VARCHAR) = '' THEN 1 ELSE 0 END) as empty_str
          FROM "${viewName}"
        `);
        const row = result.toArray()[0];
        const nonNull = Number(row.non_null);
        const emptyStr = Number(row.empty_str);

        if (nonNull === 0 || nonNull === emptyStr) {
          emptyColumns.push(col.name);
        }
      } catch (e) {
        /* ignore */
      }
    }

    return emptyColumns;
  },

  async exportData(fileId, format) {
    const viewName = this._getViewName(fileId);
    if (!viewName) throw new Error('File not found');

    const whereClauses = this._buildWhereClauses(State.filters);
    const whereSql = whereClauses.length > 0 ? ' WHERE ' + whereClauses.join(' AND ') : '';
    const orderSql = State.sortCol ? ` ORDER BY "${State.sortCol}" ${State.sortDir === 'desc' ? 'DESC' : 'ASC'}` : '';

    let selectCols = '*';
    if (State.hideEmptyColumns && State.hiddenColumns.length > 0) {
      selectCols = `* EXCLUDE (${State.hiddenColumns.map(c => `"${c}"`).join(', ')})`;
    }

    const sql = `SELECT ${selectCols} FROM "${viewName}"${whereSql}${orderSql}`;
    const result = await this.conn.query(sql);
    const fields = result.schema.fields;
    const columns = fields.map(f => f.name);
    const rows = result.toArray().map(row => convertRow(row, fields));

    let blob;
    let filename;

    if (format === 'csv') {
      const lines = [];
      lines.push(columns.map(c => `"${c.replace(/"/g, '""')}"`).join(','));
      for (const row of rows) {
        const vals = columns.map(c => {
          const val = row[c];
          if (val === null || val === undefined) return '';
          const str = String(val);
          if (str.includes(',') || str.includes('"') || str.includes('\n')) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        });
        lines.push(vals.join(','));
      }
      blob = new Blob([lines.join('\n')], { type: 'text/csv' });
      filename = `export_${Date.now()}.csv`;
    } else if (format === 'json') {
      blob = new Blob([JSON.stringify(rows, null, 2)], { type: 'application/json' });
      filename = `export_${Date.now()}.json`;
    } else {
      throw new Error(`Unsupported format: ${format}`);
    }

    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  },

  async transform(fileId, operation, params) {
    const viewName = this._getViewName(fileId);
    const info = this.files[fileId];
    if (!viewName || !info) throw new Error('File not found');

    const newFileId = generateUUID();
    const baseName = info.name;
    const newName = `${baseName}_${operation}`;
    const newViewName = sanitizeViewName(newName + '_' + newFileId.substring(0, 8));

    let sql;
    switch (operation) {
      case 'rename': {
        const oldCol = params.old_name;
        const newCol = params.new_name;
        sql = `CREATE OR REPLACE VIEW "${newViewName}" AS SELECT * REPLACE ("${oldCol}" AS "${newCol}") FROM "${viewName}"`;
        break;
      }
      case 'cast': {
        const col = params.column;
        const newType = params.new_type;
        sql = `CREATE OR REPLACE VIEW "${newViewName}" AS SELECT * REPLACE (CAST("${col}" AS ${newType}) AS "${col}") FROM "${viewName}"`;
        break;
      }
      case 'drop': {
        const cols = params.columns;
        const excludeList = cols.map(c => `"${c}"`).join(', ');
        sql = `CREATE OR REPLACE VIEW "${newViewName}" AS SELECT * EXCLUDE (${excludeList}) FROM "${viewName}"`;
        break;
      }
      case 'compute': {
        const name = params.name;
        const expression = params.expression;
        sql = `CREATE OR REPLACE VIEW "${newViewName}" AS SELECT *, (${expression}) AS "${name}" FROM "${viewName}"`;
        break;
      }
      case 'filter': {
        const where = params.sql_where;
        sql = `CREATE OR REPLACE VIEW "${newViewName}" AS SELECT * FROM "${viewName}" WHERE ${where}`;
        break;
      }
      case 'deduplicate': {
        const subsetCols = params.columns;
        if (subsetCols && subsetCols.length > 0) {
          const onCols = subsetCols.map(c => `"${c}"`).join(', ');
          sql = `CREATE OR REPLACE VIEW "${newViewName}" AS SELECT DISTINCT ON (${onCols}) * FROM "${viewName}"`;
        } else {
          sql = `CREATE OR REPLACE VIEW "${newViewName}" AS SELECT DISTINCT * FROM "${viewName}"`;
        }
        break;
      }
      default:
        throw new Error(`Unknown operation: ${operation}`);
    }

    await this.conn.query(sql);

    let numRows = 0;
    let numColumns = 0;
    try {
      const countResult = await this.conn.query(`SELECT COUNT(*) as cnt FROM "${newViewName}"`);
      numRows = Number(countResult.toArray()[0].cnt);
    } catch (e) { /* ignore */ }
    try {
      const descResult = await this.conn.query(`DESCRIBE "${newViewName}"`);
      numColumns = descResult.toArray().length;
    } catch (e) { /* ignore */ }

    const newInfo = {
      id: newFileId,
      name: newName,
      fileName: newName + '.parquet',
      viewName: newViewName,
      numRows: numRows,
      numColumns: numColumns,
      sizeBytes: 0,
    };
    this.files[newFileId] = newInfo;
    return newInfo;
  },

  _getViewName(fileId) {
    return this.files[fileId]?.viewName || null;
  },

  _getFileName(fileId) {
    return this.files[fileId]?.fileName || null;
  },

  _buildWhereClauses(filters) {
    const opMap = {
      eq: '=', ne: '!=', gt: '>', lt: '<',
      gte: '>=', lte: '<=', contains: 'LIKE', not_contains: 'NOT LIKE',
      is_null: 'IS NULL', is_not_null: 'IS NOT NULL',
    };
    const clauses = [];
    for (const f of filters) {
      const col = f.column;
      const op = f.operator || 'eq';
      const val = f.value;
      const sqlOp = opMap[op] || '=';

      if (op === 'is_null' || op === 'is_not_null') {
        clauses.push(`"${col}" ${sqlOp}`);
      } else if (op === 'contains' || op === 'not_contains') {
        clauses.push(`CAST("${col}" AS VARCHAR) ${sqlOp} '%${String(val).replace(/'/g, "''")}%'`);
      } else {
        const escaped = String(val).replace(/'/g, "''");
        if (typeof val === 'number' || (typeof val === 'string' && !isNaN(val) && val.trim() !== '')) {
          clauses.push(`"${col}" ${sqlOp} ${escaped}`);
        } else {
          clauses.push(`"${col}" ${sqlOp} '${escaped}'`);
        }
      }
    }
    return clauses;
  },
};

// ==========================================================================
//  FileManager
// ==========================================================================

const FileManager = {
  init() {
    const dropZone = document.getElementById('drop-zone');
    const fileInput = document.getElementById('file-upload-input');

    if (dropZone) {
      dropZone.addEventListener('click', () => fileInput.click());

      dropZone.addEventListener('dragenter', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.add('drop-zone-active');
      });

      dropZone.addEventListener('dragover', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.add('drop-zone-active');
      });

      dropZone.addEventListener('dragleave', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.remove('drop-zone-active');
      });

      dropZone.addEventListener('drop', (e) => {
        e.preventDefault();
        e.stopPropagation();
        dropZone.classList.remove('drop-zone-active');
        if (e.dataTransfer.files.length > 0) {
          this.handleFiles(e.dataTransfer.files);
        }
      });
    }

    if (fileInput) {
      fileInput.addEventListener('change', (e) => {
        if (e.target.files.length > 0) {
          this.handleFiles(e.target.files);
          e.target.value = '';
        }
      });
    }
  },

  async handleFiles(fileList) {
    if (!DB._ready) {
      Toast.warning('DuckDB is still loading. Please wait a moment and try again.');
      return;
    }
    for (const file of fileList) {
      const ext = file.name.toLowerCase();
      if (!ext.endsWith('.parquet') && !ext.endsWith('.parq') && !ext.endsWith('.pq')) {
        Toast.warning(`Skipped "${file.name}" — not a .parquet file`);
        continue;
      }

      try {
        Toast.info(`Loading "${file.name}"...`);
        const info = await DB.registerFile(file);
        State.files.push(info);
        Toast.success(`Loaded "${file.name}" (${formatNumber(info.numRows)} rows)`);
      } catch (err) {
        Toast.error(`Failed to load "${file.name}": ${err.message}`);
      }
    }

    this.renderFileList();

    if (!State.activeFileId && State.files.length > 0) {
      await this.selectFile(State.files[0].id);
    }
  },

  renderFileList() {
    const container = document.getElementById('file-list');
    if (!container) return;

    if (State.files.length === 0) {
      container.innerHTML = '<div class="file-list-empty">No files loaded</div>';
      return;
    }

    container.innerHTML = State.files.map(f => `
      <div class="file-item ${f.id === State.activeFileId ? 'file-item-active' : ''}"
           onclick="window.__fileSelect('${f.id}')">
        <div class="file-item-info">
          <div class="file-item-name" title="${escapeHtml(f.fileName)}">${escapeHtml(f.name)}</div>
          <div class="file-item-meta">${formatNumber(f.numRows)} rows &middot; ${formatFileSize(f.sizeBytes)}</div>
        </div>
        <button class="file-item-delete" onclick="event.stopPropagation(); window.__fileDelete('${f.id}')" title="Remove file">&times;</button>
      </div>
    `).join('');
  },

  async selectFile(fileId) {
    State.activeFileId = fileId;
    State.resetPagination();
    State.pageSize = State.settings.pageSize || 50;

    try {
      State._schema = await DB.getSchema(fileId);
    } catch (e) {
      State._schema = null;
    }

    this.renderFileList();
    await loadCurrentTab();
  },

  async deleteFile(fileId) {
    if (!confirm('Remove this file?')) return;

    try {
      await DB.removeFile(fileId);
      State.files = State.files.filter(f => f.id !== fileId);

      if (State.activeFileId === fileId) {
        State.activeFileId = State.files.length > 0 ? State.files[0].id : null;
        State.resetPagination();
        if (State.activeFileId) {
          State._schema = await DB.getSchema(State.activeFileId);
        }
      }

      this.renderFileList();
      await loadCurrentTab();
      Toast.success('File removed');
    } catch (err) {
      Toast.error('Failed to remove file: ' + err.message);
    }
  },
};

// Global handlers for onclick in HTML strings
window.__fileSelect = (id) => FileManager.selectFile(id);
window.__fileDelete = (id) => FileManager.deleteFile(id);

// ==========================================================================
//  TabManager
// ==========================================================================

const TabManager = {
  init() {
    const tabButtons = document.querySelectorAll('[data-tab]');
    tabButtons.forEach(btn => {
      btn.addEventListener('click', () => {
        const tab = btn.getAttribute('data-tab');
        this.switchTab(tab);
      });
    });
  },

  switchTab(tab) {
    State.activeTab = tab;

    document.querySelectorAll('[data-tab]').forEach(btn => {
      btn.classList.toggle('tab-active', btn.getAttribute('data-tab') === tab);
    });

    document.querySelectorAll('[data-tab-content]').forEach(panel => {
      const isActive = panel.getAttribute('data-tab-content') === tab;
      panel.classList.toggle('tab-content-active', isActive);
      panel.style.display = isActive ? 'block' : 'none';
    });

    loadCurrentTab();
  },
};

// ==========================================================================
//  Load Current Tab
// ==========================================================================

async function loadCurrentTab() {
  try {
    switch (State.activeTab) {
      case 'data': await DataTable.load(); break;
      case 'schema': await SchemaViewer.load(); break;
      case 'sql': SQLEditor.load(); break;
      case 'stats': await StatsViewer.load(); break;
      case 'charts': ChartBuilder.load(); break;
      case 'metadata': await MetadataViewer.load(); break;
      case 'transforms': TransformManager.load(); break;
    }
  } catch (err) {
    Toast.error('Error loading tab: ' + err.message);
  }
}

// ==========================================================================
//  DataTable
// ==========================================================================

const DataTable = {
  async load() {
    const container = document.querySelector('[data-tab-content="data"]');
    if (!container) return;

    if (!State.activeFileId) {
      container.innerHTML = `
        <div class="empty-state">
          <p>No file selected</p>
          <p>Upload a .parquet file using the sidebar to get started</p>
        </div>
      `;
      return;
    }

    container.innerHTML = '<div class="loading-spinner"><div class="spinner"></div>Loading data...</div>';

    try {
      let columns, data, totalRows, totalPages, schema;

      if (State._schema) {
        schema = State._schema;
      } else {
        schema = await DB.getSchema(State.activeFileId);
        State._schema = schema;
      }

      if (State.searchQuery) {
        const searchResult = await DB.search(State.activeFileId, State.searchQuery);
        columns = searchResult.columns;
        data = searchResult.data;
        totalRows = searchResult.rowCount;
        totalPages = 1;
      } else {
        const hiddenCols = State.hideEmptyColumns ? State.hiddenColumns : [];
        const result = await DB.getData(State.activeFileId, {
          page: State.currentPage,
          pageSize: State.pageSize,
          sortCol: State.sortCol,
          sortDir: State.sortDir,
          filters: State.filters,
          hiddenColumns: hiddenCols,
        });
        columns = result.columns;
        data = result.data;
        totalRows = result.totalRows;
        totalPages = result.totalPages;
        State._totalRows = totalRows;
      }

      this._render(container, columns, data, schema, totalRows, totalPages);
    } catch (err) {
      container.innerHTML = `<div class="error-state">Error loading data: ${escapeHtml(err.message)}</div>`;
    }
  },

  _render(container, columns, data, schema, totalRows, totalPages) {
    const schemaMap = {};
    if (schema) {
      for (const s of schema) {
        schemaMap[s.name] = s;
      }
    }

    // Toolbar
    let html = `<div class="data-toolbar">
      <div class="data-toolbar-left">
        <div class="search-box">
          <input class="input" type="text" placeholder="Search across text columns..."
                 value="${escapeHtml(State.searchQuery)}" id="data-search-input">
        </div>
        <button class="btn btn-secondary btn-sm" id="btn-add-filter">+ Filter</button>
        <label style="display:inline-flex;align-items:center;gap:4px;font-size:var(--font-sm);cursor:pointer;white-space:nowrap;">
          <input type="checkbox" id="chk-hide-empty" ${State.hideEmptyColumns ? 'checked' : ''}>
          Hide empty columns
        </label>
      </div>
      <div class="data-toolbar-right">
        <div class="export-dropdown">
          <button class="btn btn-ghost btn-sm" id="btn-export-toggle">Export &#9662;</button>
          <div class="export-dropdown-menu" id="export-menu">
            <button class="export-option" data-format="csv">CSV</button>
            <button class="export-option" data-format="json">JSON</button>
          </div>
        </div>
      </div>
    </div>`;

    // Active filters
    if (State.filters.length > 0) {
      html += '<div class="active-filters">';
      State.filters.forEach((f, idx) => {
        html += `<span class="filter-chip">
          ${escapeHtml(f.column)} ${escapeHtml(f.operator)} ${f.value !== undefined ? escapeHtml(String(f.value)) : ''}
          <button class="filter-chip-remove" data-filter-idx="${idx}">&times;</button>
        </span>`;
      });
      html += '</div>';
    }

    // Table
    if (data.length === 0) {
      html += '<div class="empty-state">No data to display</div>';
    } else {
      html += '<div class="table-wrapper"><table class="data-table"><thead><tr>';
      for (const col of columns) {
        const isSorted = State.sortCol === col;
        const arrow = isSorted ? (State.sortDir === 'asc' ? ' &#9650;' : ' &#9660;') : '';
        const colInfo = schemaMap[col];
        const isNum = colInfo ? isNumericType(colInfo.type) : false;
        html += `<th class="sortable-header ${isNum ? 'text-right' : ''}" data-sort-col="${escapeHtml(col)}">${escapeHtml(col)}${arrow}</th>`;
      }
      html += '</tr></thead><tbody>';

      const nullDisplay = State.settings.nullDisplay || 'NULL';
      for (const row of data) {
        html += '<tr>';
        for (const col of columns) {
          const val = row[col];
          const colInfo = schemaMap[col];
          const isNum = colInfo ? isNumericType(colInfo.type) : false;
          const cellClass = isNum ? 'text-right' : '';
          const rawStr = val != null ? String(val) : '';
          html += `<td class="${cellClass} cell-truncate" data-col="${escapeHtml(col)}" data-raw="${escapeHtml(rawStr)}">${formatCellValue(val, nullDisplay)}</td>`;
        }
        html += '</tr>';
      }
      html += '</tbody></table></div>';
    }

    // Pagination
    if (!State.searchQuery) {
      const startRow = data.length > 0 ? (State.currentPage - 1) * State.pageSize + 1 : 0;
      const endRow = startRow + data.length - 1;
      html += `<div class="pagination">
        <div class="pagination-info">
          Showing ${formatNumber(startRow)}-${formatNumber(endRow)} of ${formatNumber(totalRows)} rows
        </div>
        <div class="pagination-controls">
          <button class="btn btn-ghost btn-sm" id="btn-page-first" ${State.currentPage <= 1 ? 'disabled' : ''}>&laquo;</button>
          <button class="btn btn-ghost btn-sm" id="btn-page-prev" ${State.currentPage <= 1 ? 'disabled' : ''}>&lsaquo;</button>
          <span class="pagination-current">Page ${State.currentPage} of ${totalPages}</span>
          <button class="btn btn-ghost btn-sm" id="btn-page-next" ${State.currentPage >= totalPages ? 'disabled' : ''}>&rsaquo;</button>
          <button class="btn btn-ghost btn-sm" id="btn-page-last" ${State.currentPage >= totalPages ? 'disabled' : ''}>&raquo;</button>
          <select class="input page-size-select" id="page-size-select" style="width:auto;min-width:80px;">
            ${[25, 50, 100, 250, 500].map(ps => `<option value="${ps}" ${ps === State.pageSize ? 'selected' : ''}>${ps} / page</option>`).join('')}
          </select>
        </div>
      </div>`;
    }

    container.innerHTML = html;

    // Bind events
    this._bindEvents(container, totalPages);
  },

  _bindEvents(container, totalPages) {
    // Search
    const searchInput = container.querySelector('#data-search-input');
    if (searchInput) {
      const debouncedSearch = debounce((val) => {
        State.searchQuery = val;
        State.currentPage = 1;
        this.load();
      }, 300);
      searchInput.addEventListener('input', (e) => debouncedSearch(e.target.value));
    }

    // Add filter
    const addFilterBtn = container.querySelector('#btn-add-filter');
    if (addFilterBtn) {
      addFilterBtn.addEventListener('click', () => FilterModal.open());
    }

    // Hide empty columns toggle
    const hideEmptyChk = container.querySelector('#chk-hide-empty');
    if (hideEmptyChk) {
      hideEmptyChk.addEventListener('change', async (e) => {
        State.hideEmptyColumns = e.target.checked;
        if (State.hideEmptyColumns) {
          try {
            Toast.info('Detecting empty columns...');
            State.hiddenColumns = await DB.getEmptyColumns(State.activeFileId);
            if (State.hiddenColumns.length > 0) {
              Toast.success(`Hiding ${State.hiddenColumns.length} empty column(s)`);
            } else {
              Toast.info('No empty columns found');
            }
          } catch (err) {
            Toast.error('Failed to detect empty columns: ' + err.message);
            State.hideEmptyColumns = false;
            State.hiddenColumns = [];
          }
        } else {
          State.hiddenColumns = [];
        }
        State.currentPage = 1;
        this.load();
      });
    }

    // Export dropdown
    const exportToggle = container.querySelector('#btn-export-toggle');
    const exportMenu = container.querySelector('#export-menu');
    if (exportToggle && exportMenu) {
      exportToggle.addEventListener('click', (e) => {
        e.stopPropagation();
        exportMenu.classList.toggle('show');
      });
      document.addEventListener('click', () => exportMenu.classList.remove('show'), { once: true });
    }

    // Export options
    container.querySelectorAll('.export-option').forEach(btn => {
      btn.addEventListener('click', async () => {
        const format = btn.getAttribute('data-format');
        try {
          Toast.info(`Exporting as ${format.toUpperCase()}...`);
          await DB.exportData(State.activeFileId, format);
          Toast.success('Export complete');
        } catch (err) {
          Toast.error('Export failed: ' + err.message);
        }
        if (exportMenu) exportMenu.classList.remove('show');
      });
    });

    // Filter chip removal
    container.querySelectorAll('.filter-chip-remove').forEach(btn => {
      btn.addEventListener('click', () => {
        const idx = parseInt(btn.getAttribute('data-filter-idx'));
        State.filters.splice(idx, 1);
        State.currentPage = 1;
        this.load();
      });
    });

    // Click cell to expand
    container.querySelectorAll('.cell-truncate').forEach(td => {
      td.addEventListener('dblclick', () => {
        const col = td.getAttribute('data-col') || '';
        const raw = td.getAttribute('data-raw') || '';
        if (raw === '' && !td.querySelector('.null-value')) return;
        showCellExpand(col, raw);
      });
    });

    // Sortable headers
    container.querySelectorAll('.sortable-header').forEach(th => {
      th.addEventListener('click', () => {
        const col = th.getAttribute('data-sort-col');
        if (State.sortCol === col) {
          State.sortDir = State.sortDir === 'asc' ? 'desc' : 'asc';
        } else {
          State.sortCol = col;
          State.sortDir = 'asc';
        }
        State.currentPage = 1;
        this.load();
      });
    });

    // Pagination
    const btnFirst = container.querySelector('#btn-page-first');
    const btnPrev = container.querySelector('#btn-page-prev');
    const btnNext = container.querySelector('#btn-page-next');
    const btnLast = container.querySelector('#btn-page-last');
    const pageSizeSelect = container.querySelector('#page-size-select');

    if (btnFirst) btnFirst.addEventListener('click', () => { State.currentPage = 1; this.load(); });
    if (btnPrev) btnPrev.addEventListener('click', () => { if (State.currentPage > 1) { State.currentPage--; this.load(); } });
    if (btnNext) btnNext.addEventListener('click', () => { if (State.currentPage < totalPages) { State.currentPage++; this.load(); } });
    if (btnLast) btnLast.addEventListener('click', () => { State.currentPage = totalPages; this.load(); });
    if (pageSizeSelect) {
      pageSizeSelect.addEventListener('change', (e) => {
        State.pageSize = parseInt(e.target.value);
        State.settings.pageSize = State.pageSize;
        State.saveSettings();
        State.currentPage = 1;
        this.load();
      });
    }
  },
};

// ==========================================================================
//  FilterModal
// ==========================================================================

const FilterModal = {
  open() {
    if (!State._schema || State._schema.length === 0) {
      Toast.warning('No schema available. Load a file first.');
      return;
    }

    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
      <div class="modal-content">
        <div class="modal-header">
          <h3>Add Filter</h3>
          <button class="modal-close">&times;</button>
        </div>
        <div class="modal-body">
          <div class="form-group">
            <label class="form-label">Column</label>
            <select class="input" id="filter-column">
              ${State._schema.map(c => `<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)} (${escapeHtml(c.type)})</option>`).join('')}
            </select>
          </div>
          <div class="form-group">
            <label class="form-label">Operator</label>
            <select class="input" id="filter-operator">
              <option value="eq">Equals (=)</option>
              <option value="ne">Not equals (!=)</option>
              <option value="gt">Greater than (&gt;)</option>
              <option value="lt">Less than (&lt;)</option>
              <option value="gte">Greater or equal (&gt;=)</option>
              <option value="lte">Less or equal (&lt;=)</option>
              <option value="contains">Contains</option>
              <option value="not_contains">Not contains</option>
              <option value="is_null">Is NULL</option>
              <option value="is_not_null">Is NOT NULL</option>
            </select>
          </div>
          <div class="form-group" id="filter-value-group">
            <label class="form-label">Value</label>
            <input class="input" type="text" id="filter-value" placeholder="Enter value...">
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-ghost" id="filter-cancel">Cancel</button>
          <button class="btn btn-primary" id="filter-apply">Apply Filter</button>
        </div>
      </div>
    `;

    document.body.appendChild(overlay);

    const close = () => overlay.remove();
    overlay.querySelector('.modal-close').addEventListener('click', close);
    overlay.querySelector('#filter-cancel').addEventListener('click', close);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

    const operatorSelect = overlay.querySelector('#filter-operator');
    const valueGroup = overlay.querySelector('#filter-value-group');
    operatorSelect.addEventListener('change', () => {
      const op = operatorSelect.value;
      valueGroup.style.display = (op === 'is_null' || op === 'is_not_null') ? 'none' : 'block';
    });

    overlay.querySelector('#filter-apply').addEventListener('click', () => {
      const column = overlay.querySelector('#filter-column').value;
      const operator = operatorSelect.value;
      const value = overlay.querySelector('#filter-value').value;

      const filter = { column, operator };
      if (operator !== 'is_null' && operator !== 'is_not_null') {
        filter.value = value;
      }

      State.filters.push(filter);
      State.currentPage = 1;
      close();
      DataTable.load();
    });
  },
};

// ==========================================================================
//  ExportManager (standalone helper)
// ==========================================================================

const ExportManager = {
  async exportFromSQL(sql, format) {
    try {
      const result = await DB.runQuery(sql);
      const rows = result.data;
      const columns = result.columns.map(c => c.name);

      let blob;
      let filename;

      if (format === 'csv') {
        const lines = [];
        lines.push(columns.map(c => `"${c.replace(/"/g, '""')}"`).join(','));
        for (const row of rows) {
          const vals = columns.map(c => {
            const val = row[c];
            if (val === null || val === undefined) return '';
            const str = String(val);
            if (str.includes(',') || str.includes('"') || str.includes('\n')) {
              return `"${str.replace(/"/g, '""')}"`;
            }
            return str;
          });
          lines.push(vals.join(','));
        }
        blob = new Blob([lines.join('\n')], { type: 'text/csv' });
        filename = `sql_export_${Date.now()}.csv`;
      } else {
        blob = new Blob([JSON.stringify(rows, null, 2)], { type: 'application/json' });
        filename = `sql_export_${Date.now()}.json`;
      }

      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = filename;
      document.body.appendChild(a);
      a.click();
      document.body.removeChild(a);
      URL.revokeObjectURL(url);
    } catch (err) {
      Toast.error('Export failed: ' + err.message);
    }
  },
};

// ==========================================================================
//  SchemaViewer
// ==========================================================================

const SchemaViewer = {
  async load() {
    const container = document.querySelector('[data-tab-content="schema"]');
    if (!container) return;

    if (!State.activeFileId) {
      container.innerHTML = '<div class="empty-state">No file selected</div>';
      return;
    }

    container.innerHTML = '<div class="loading-spinner"><div class="spinner"></div>Loading schema...</div>';

    try {
      const schema = await DB.getSchema(State.activeFileId);
      State._schema = schema;
      this._render(container, schema);
    } catch (err) {
      container.innerHTML = `<div class="error-state">Error: ${escapeHtml(err.message)}</div>`;
    }
  },

  _render(container, schema) {
    const fileInfo = DB.files[State.activeFileId];
    let html = `<div class="schema-view">`;
    html += `<p class="schema-summary">${schema.length} columns in "${escapeHtml(fileInfo?.name || '')}"</p>`;
    html += `<div class="table-wrapper"><table class="data-table">
      <thead><tr>
        <th>#</th>
        <th>Column Name</th>
        <th>Type</th>
        <th>Nullable</th>
      </tr></thead><tbody>`;

    schema.forEach((col, idx) => {
      const badgeClass = getTypeBadgeClass(col.type);
      html += `<tr>
        <td>${idx + 1}</td>
        <td><strong>${escapeHtml(col.name)}</strong></td>
        <td><span class="type-badge ${badgeClass}">${escapeHtml(col.type)}</span></td>
        <td>${col.nullable ? 'Yes' : 'No'}</td>
      </tr>`;
    });

    html += '</tbody></table></div></div>';
    container.innerHTML = html;
  },
};

// ==========================================================================
//  SQLEditor
// ==========================================================================

const SQLEditor = {
  _lastSQL: '',

  load() {
    const container = document.querySelector('[data-tab-content="sql"]');
    if (!container) return;

    // Only re-render if empty (preserve state across tab switches)
    if (container.querySelector('.sql-editor-wrapper')) return;

    const files = Object.values(DB.files);

    let tableChipsHtml = '';
    if (files.length > 0) {
      tableChipsHtml = files.map(f =>
        `<span class="table-chip" data-view="${escapeHtml(f.viewName)}" title="Click to insert">${escapeHtml(f.viewName)}</span>`
      ).join(' ');
    } else {
      tableChipsHtml = '<span class="sql-ref-none">No files loaded</span>';
    }

    let bookmarksHtml = '';
    if (State.bookmarks.length > 0) {
      bookmarksHtml = `<div class="form-group">
        <label class="form-label">Bookmarks</label>
        <div style="display:flex;flex-wrap:wrap;gap:4px;">
          ${State.bookmarks.map(b => `
            <span class="table-chip" style="cursor:pointer;" data-bookmark-sql="${escapeHtml(b.sql)}" title="${escapeHtml(b.name)}">
              ${escapeHtml(b.name)}
            </span>
          `).join('')}
        </div>
      </div>`;
    }

    let historyHtml = '';
    if (State.queryHistory.length > 0) {
      historyHtml = `<div class="form-group">
        <label class="form-label">History</label>
        <select class="input" id="sql-history-select" style="font-family:var(--mono-font);font-size:var(--font-xs);">
          <option value="">Select from history...</option>
          ${State.queryHistory.map(q => `<option value="${escapeHtml(q)}">${escapeHtml(truncateText(q, 80))}</option>`).join('')}
        </select>
      </div>`;
    }

    const defaultSQL = this._lastSQL || (files.length > 0 ? `SELECT * FROM "${files[0].viewName}" LIMIT 100` : '-- Load a file first');

    container.innerHTML = `
      <div class="sql-editor-wrapper">
        <div class="sql-toolbar">
          <div class="sql-toolbar-left">
            <button class="btn btn-primary btn-sm" id="btn-run-sql">&#9654; Run (Ctrl+Enter)</button>
            <button class="btn btn-ghost btn-sm" id="btn-clear-sql">Clear</button>
          </div>
          <div class="sql-toolbar-right">
            <button class="btn btn-ghost btn-sm" id="btn-bookmark-sql">&#9733; Bookmark</button>
          </div>
        </div>

        <div class="sql-tables-reference">
          <span class="sql-ref-label">Tables:</span>
          ${tableChipsHtml}
        </div>

        ${bookmarksHtml}
        ${historyHtml}

        <textarea class="sql-textarea" id="sql-textarea" placeholder="Enter SQL query..." spellcheck="false">${escapeHtml(defaultSQL)}</textarea>

        <div class="sql-status" id="sql-status"></div>
        <div class="sql-results" id="sql-results"></div>
      </div>
    `;

    this._bindEvents(container);
  },

  _bindEvents(container) {
    const textarea = container.querySelector('#sql-textarea');
    const runBtn = container.querySelector('#btn-run-sql');
    const clearBtn = container.querySelector('#btn-clear-sql');
    const bookmarkBtn = container.querySelector('#btn-bookmark-sql');
    const historySelect = container.querySelector('#sql-history-select');

    if (runBtn) {
      runBtn.addEventListener('click', () => this._runQuery());
    }

    if (clearBtn) {
      clearBtn.addEventListener('click', () => {
        textarea.value = '';
        this._lastSQL = '';
        container.querySelector('#sql-status').textContent = '';
        container.querySelector('#sql-results').innerHTML = '';
      });
    }

    if (bookmarkBtn) {
      bookmarkBtn.addEventListener('click', () => {
        const sql = textarea.value.trim();
        if (!sql) {
          Toast.warning('No SQL to bookmark');
          return;
        }
        const name = prompt('Bookmark name:');
        if (!name) return;
        State.bookmarks.push({
          id: generateUUID(),
          name: name,
          sql: sql,
          created_at: new Date().toISOString(),
        });
        State.saveBookmarks();
        Toast.success('Bookmark saved');
        // Re-render to show the new bookmark
        const wrapper = container.querySelector('.sql-editor-wrapper');
        if (wrapper) wrapper.remove();
        this.load();
      });
    }

    if (historySelect) {
      historySelect.addEventListener('change', (e) => {
        if (e.target.value) {
          textarea.value = e.target.value;
          this._lastSQL = e.target.value;
        }
      });
    }

    // Table chip click to insert
    container.querySelectorAll('.table-chip[data-view]').forEach(chip => {
      chip.addEventListener('click', () => {
        const viewName = chip.getAttribute('data-view');
        const cursorPos = textarea.selectionStart;
        const before = textarea.value.substring(0, cursorPos);
        const after = textarea.value.substring(cursorPos);
        textarea.value = before + `"${viewName}"` + after;
        textarea.focus();
      });
    });

    // Bookmark chip click
    container.querySelectorAll('.table-chip[data-bookmark-sql]').forEach(chip => {
      chip.addEventListener('click', () => {
        textarea.value = chip.getAttribute('data-bookmark-sql');
        this._lastSQL = textarea.value;
      });
    });

    // Ctrl+Enter
    textarea.addEventListener('keydown', (e) => {
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        e.preventDefault();
        this._runQuery();
      }
    });
  },

  async _runQuery() {
    const textarea = document.querySelector('#sql-textarea');
    const statusEl = document.querySelector('#sql-status');
    const resultsEl = document.querySelector('#sql-results');
    if (!textarea || !statusEl || !resultsEl) return;

    const sql = textarea.value.trim();
    if (!sql) {
      Toast.warning('Enter a SQL query first');
      return;
    }

    this._lastSQL = sql;
    statusEl.className = 'sql-status sql-running';
    statusEl.textContent = 'Running...';
    resultsEl.innerHTML = '';

    try {
      const result = await DB.runQuery(sql);
      State.addQueryToHistory(sql);

      statusEl.className = 'sql-status sql-success';
      statusEl.textContent = `${result.rowCount} row(s) returned in ${result.executionTime}s`;

      if (result.columns.length > 0 && result.data.length > 0) {
        const columns = result.columns.map(c => c.name);
        let tableHtml = '<div class="table-wrapper"><table class="data-table"><thead><tr>';
        for (const col of columns) {
          tableHtml += `<th>${escapeHtml(col)}</th>`;
        }
        tableHtml += '</tr></thead><tbody>';
        for (const row of result.data.slice(0, 1000)) {
          tableHtml += '<tr>';
          for (const col of columns) {
            tableHtml += `<td class="cell-truncate">${formatCellValue(row[col], State.settings.nullDisplay)}</td>`;
          }
          tableHtml += '</tr>';
        }
        tableHtml += '</tbody></table></div>';
        if (result.data.length > 1000) {
          tableHtml += `<p class="text-muted" style="margin-top:8px;">Showing first 1000 of ${result.rowCount} rows</p>`;
        }
        resultsEl.innerHTML = tableHtml;
      } else {
        resultsEl.innerHTML = '<div class="empty-state">Query executed successfully. No rows returned.</div>';
      }
    } catch (err) {
      statusEl.className = 'sql-status sql-error';
      statusEl.textContent = `Error: ${err.message}`;
    }
  },
};

// ==========================================================================
//  StatsViewer
// ==========================================================================

const StatsViewer = {
  async load() {
    const container = document.querySelector('[data-tab-content="stats"]');
    if (!container) return;

    if (!State.activeFileId) {
      container.innerHTML = '<div class="empty-state">No file selected</div>';
      return;
    }

    container.innerHTML = '<div class="loading-spinner"><div class="spinner"></div>Computing statistics...</div>';

    try {
      const result = await DB.getStats(State.activeFileId);
      const schema = State._schema || await DB.getSchema(State.activeFileId);
      this._render(container, result.stats, schema);
    } catch (err) {
      container.innerHTML = `<div class="error-state">Error: ${escapeHtml(err.message)}</div>`;
    }
  },

  _render(container, stats, schema) {
    const fileInfo = DB.files[State.activeFileId];
    const colNames = Object.keys(stats);
    const totalRows = fileInfo?.numRows || 0;
    const totalNulls = colNames.reduce((sum, c) => sum + (stats[c]?.nulls || 0), 0);
    const totalCells = totalRows * colNames.length;
    const nullPct = totalCells > 0 ? ((totalNulls / totalCells) * 100).toFixed(1) : '0';

    let html = `<div class="stats-cards">
      <div class="stat-card">
        <span class="stat-card-value">${formatNumber(totalRows)}</span>
        <span class="stat-card-label">Total Rows</span>
      </div>
      <div class="stat-card">
        <span class="stat-card-value">${colNames.length}</span>
        <span class="stat-card-label">Columns</span>
      </div>
      <div class="stat-card">
        <span class="stat-card-value">${nullPct}%</span>
        <span class="stat-card-label">Null %</span>
      </div>
      <div class="stat-card">
        <span class="stat-card-value">${formatFileSize(fileInfo?.sizeBytes || 0)}</span>
        <span class="stat-card-label">File Size</span>
      </div>
    </div>`;

    html += `<h3 class="section-title">Column Statistics</h3>`;
    html += `<div class="table-wrapper"><table class="data-table">
      <thead><tr>
        <th>Column</th><th>Type</th><th>Count</th><th>Nulls</th><th>Unique</th>
        <th>Min</th><th>Max</th><th>Mean</th><th>Std Dev</th>
      </tr></thead><tbody>`;

    for (const col of colNames) {
      const s = stats[col];
      const colSchema = schema.find(c => c.name === col);
      const badgeClass = getTypeBadgeClass(colSchema?.type);
      const isNum = colSchema ? isNumericType(colSchema.type) : false;

      html += `<tr class="stats-column-row" style="cursor:pointer" data-col-detail="${escapeHtml(col)}">
        <td><strong>${escapeHtml(col)}</strong></td>
        <td><span class="type-badge ${badgeClass}">${escapeHtml(colSchema?.type || 'unknown')}</span></td>
        <td class="text-right">${formatNumber(s.count)}</td>
        <td class="text-right">${formatNumber(s.nulls)}</td>
        <td class="text-right">${formatNumber(s.unique)}</td>
        <td class="text-right">${s.min !== undefined && s.min !== null ? escapeHtml(truncateText(String(s.min), 20)) : '<span class="null-value">-</span>'}</td>
        <td class="text-right">${s.max !== undefined && s.max !== null ? escapeHtml(truncateText(String(s.max), 20)) : '<span class="null-value">-</span>'}</td>
        <td class="text-right">${isNum && s.mean != null ? Number(s.mean).toFixed(2) : '<span class="null-value">-</span>'}</td>
        <td class="text-right">${isNum && s.std != null ? Number(s.std).toFixed(2) : '<span class="null-value">-</span>'}</td>
      </tr>`;
    }

    html += '</tbody></table></div>';
    container.innerHTML = html;

    // Bind click for detail
    container.querySelectorAll('.stats-column-row').forEach(row => {
      row.addEventListener('click', () => {
        const col = row.getAttribute('data-col-detail');
        this._showColumnDetail(col);
      });
    });
  },

  async _showColumnDetail(column) {
    try {
      Toast.info(`Loading details for "${column}"...`);
      const detail = await DB.getColumnStats(State.activeFileId, column);
      this._renderDetailModal(detail);
    } catch (err) {
      Toast.error('Failed to load column details: ' + err.message);
    }
  },

  _renderDetailModal(detail) {
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';

    const isNum = isNumericType(detail.dtype);
    const badgeClass = getTypeBadgeClass(detail.dtype);

    let bodyHtml = `
      <div style="margin-bottom:16px;">
        <span class="type-badge ${badgeClass}">${escapeHtml(detail.dtype)}</span>
      </div>
      <div class="stats-cards">
        <div class="stat-card"><span class="stat-card-value">${formatNumber(detail.count)}</span><span class="stat-card-label">Non-null</span></div>
        <div class="stat-card"><span class="stat-card-value">${formatNumber(detail.nulls)}</span><span class="stat-card-label">Nulls (${detail.null_pct}%)</span></div>
        <div class="stat-card"><span class="stat-card-value">${formatNumber(detail.unique)}</span><span class="stat-card-label">Unique</span></div>
      </div>
    `;

    if (isNum) {
      bodyHtml += `<h4 class="section-title" style="margin-top:16px;">Percentiles</h4>
        <div class="percentile-grid">
          ${['min', 'p25', 'median', 'p50', 'p75', 'p95', 'p99', 'max'].map(k => {
            const val = detail[k];
            return `<div class="percentile-item">
              <span class="percentile-key">${k}</span>
              <span class="percentile-val">${val != null ? Number(val).toFixed(2) : '-'}</span>
            </div>`;
          }).join('')}
        </div>
        <div class="stats-cards">
          <div class="stat-card"><span class="stat-card-value">${detail.mean != null ? Number(detail.mean).toFixed(2) : '-'}</span><span class="stat-card-label">Mean</span></div>
          <div class="stat-card"><span class="stat-card-value">${detail.std != null ? Number(detail.std).toFixed(2) : '-'}</span><span class="stat-card-label">Std Dev</span></div>
        </div>
      `;

      if (detail.histogram) {
        bodyHtml += `<h4 class="section-title">Distribution</h4>
          <div class="chart-canvas-wrapper">
            <canvas id="stats-detail-chart"></canvas>
          </div>`;
      }
    }

    if (detail.value_frequency && detail.value_frequency.length > 0) {
      bodyHtml += `<h4 class="section-title">Top Values</h4>
        <div class="table-wrapper"><table class="data-table">
          <thead><tr><th>Value</th><th class="text-right">Count</th></tr></thead>
          <tbody>`;
      for (const vf of detail.value_frequency.slice(0, 20)) {
        bodyHtml += `<tr>
          <td class="cell-truncate">${formatCellValue(vf.value, State.settings.nullDisplay)}</td>
          <td class="text-right">${formatNumber(vf.count)}</td>
        </tr>`;
      }
      bodyHtml += '</tbody></table></div>';
    }

    overlay.innerHTML = `
      <div class="modal-content modal-large">
        <div class="modal-header">
          <h3>${escapeHtml(detail.column)}</h3>
          <button class="modal-close">&times;</button>
        </div>
        <div class="modal-body">${bodyHtml}</div>
        <div class="modal-footer">
          <button class="btn btn-ghost modal-close-btn">Close</button>
        </div>
      </div>
    `;

    document.body.appendChild(overlay);

    const close = () => overlay.remove();
    overlay.querySelector('.modal-close').addEventListener('click', close);
    overlay.querySelector('.modal-close-btn').addEventListener('click', close);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

    // Render histogram chart
    if (isNum && detail.histogram && typeof Chart !== 'undefined') {
      setTimeout(() => {
        const canvas = overlay.querySelector('#stats-detail-chart');
        if (canvas) {
          new Chart(canvas, {
            type: 'bar',
            data: {
              labels: detail.histogram.labels,
              datasets: [{
                label: 'Count',
                data: detail.histogram.counts,
                backgroundColor: 'rgba(66, 99, 235, 0.6)',
                borderColor: 'rgba(66, 99, 235, 1)',
                borderWidth: 1,
              }],
            },
            options: {
              responsive: true,
              maintainAspectRatio: false,
              plugins: { legend: { display: false } },
              scales: {
                x: { title: { display: true, text: detail.column } },
                y: { title: { display: true, text: 'Count' }, beginAtZero: true },
              },
            },
          });
        }
      }, 100);
    }
  },
};

// ==========================================================================
//  ChartBuilder
// ==========================================================================

const ChartBuilder = {
  _chartInstance: null,

  load() {
    const container = document.querySelector('[data-tab-content="charts"]');
    if (!container) return;

    if (!State.activeFileId) {
      container.innerHTML = '<div class="empty-state">No file selected</div>';
      return;
    }

    // Only re-render controls if not already present
    if (container.querySelector('.chart-builder')) return;

    const schema = State._schema || [];
    const columnOptions = schema.map(c => `<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)}</option>`).join('');
    const viewName = DB.files[State.activeFileId]?.viewName || '';

    container.innerHTML = `
      <div class="chart-builder">
        <div class="chart-controls">
          <div class="form-group">
            <label class="form-label">Chart Type</label>
            <select class="input" id="chart-type">
              <option value="bar">Bar</option>
              <option value="line">Line</option>
              <option value="pie">Pie</option>
              <option value="doughnut">Doughnut</option>
              <option value="scatter">Scatter</option>
            </select>
          </div>
          <div class="form-group">
            <label class="form-label">X Axis / Labels</label>
            <select class="input" id="chart-x-col">${columnOptions}</select>
          </div>
          <div class="form-group">
            <label class="form-label">Y Axis / Values</label>
            <select class="input" id="chart-y-col">${columnOptions}</select>
          </div>
          <div class="form-group">
            <label class="form-label">Aggregation</label>
            <select class="input" id="chart-agg">
              <option value="count">COUNT</option>
              <option value="sum">SUM</option>
              <option value="avg">AVG</option>
              <option value="min">MIN</option>
              <option value="max">MAX</option>
              <option value="none">None (raw values)</option>
            </select>
          </div>
          <div class="form-group">
            <label class="form-label">Limit</label>
            <input class="input" type="number" id="chart-limit" value="20" min="1" max="1000">
          </div>
          <div class="form-group" style="align-self:flex-end;">
            <button class="btn btn-primary" id="btn-build-chart">Build Chart</button>
          </div>
        </div>
        <div class="chart-canvas-wrapper">
          <div class="chart-container">
            <canvas id="main-chart-canvas"></canvas>
          </div>
        </div>
      </div>
    `;

    container.querySelector('#btn-build-chart').addEventListener('click', () => this._buildChart(viewName));
  },

  async _buildChart(viewName) {
    if (typeof Chart === 'undefined') {
      Toast.error('Chart.js is not loaded');
      return;
    }

    const chartType = document.querySelector('#chart-type').value;
    const xCol = document.querySelector('#chart-x-col').value;
    const yCol = document.querySelector('#chart-y-col').value;
    const agg = document.querySelector('#chart-agg').value;
    const limit = parseInt(document.querySelector('#chart-limit').value) || 20;

    let sql;
    if (agg === 'none') {
      sql = `SELECT "${xCol}", "${yCol}" FROM "${viewName}" WHERE "${yCol}" IS NOT NULL LIMIT ${limit}`;
    } else {
      const aggFn = agg.toUpperCase();
      if (agg === 'count') {
        sql = `SELECT "${xCol}" as label, COUNT(*) as value FROM "${viewName}" GROUP BY "${xCol}" ORDER BY value DESC LIMIT ${limit}`;
      } else {
        sql = `SELECT "${xCol}" as label, ${aggFn}("${yCol}") as value FROM "${viewName}" GROUP BY "${xCol}" ORDER BY value DESC LIMIT ${limit}`;
      }
    }

    try {
      const result = await DB.runQuery(sql);
      const rows = result.data;

      if (rows.length === 0) {
        Toast.warning('No data for chart');
        return;
      }

      let labels, values;
      if (agg === 'none') {
        labels = rows.map(r => r[xCol] != null ? String(r[xCol]) : 'null');
        values = rows.map(r => typeof r[yCol] === 'number' ? r[yCol] : parseFloat(r[yCol]) || 0);
      } else {
        labels = rows.map(r => r.label != null ? String(r.label) : 'null');
        values = rows.map(r => typeof r.value === 'number' ? r.value : parseFloat(r.value) || 0);
      }

      const canvas = document.querySelector('#main-chart-canvas');
      if (!canvas) return;

      if (this._chartInstance) {
        this._chartInstance.destroy();
      }

      const colors = [
        '#4263eb', '#2b8a3e', '#e8590c', '#7048e8', '#e03131', '#0c8599',
        '#f59f00', '#d6336c', '#5c940d', '#364fc7', '#862e9c', '#0b7285',
        '#f76707', '#c92a2a', '#1971c2', '#6741d9', '#087f5b', '#f08c00',
        '#ae3ec9', '#2f9e44',
      ];

      const bgColors = chartType === 'pie' || chartType === 'doughnut'
        ? labels.map((_, i) => colors[i % colors.length])
        : colors[0];

      const borderColors = chartType === 'pie' || chartType === 'doughnut'
        ? labels.map(() => '#fff')
        : colors[0];

      const datasets = [{
        label: agg === 'none' ? yCol : `${agg.toUpperCase()}(${yCol})`,
        data: chartType === 'scatter' ? labels.map((l, i) => ({ x: parseFloat(l) || i, y: values[i] })) : values,
        backgroundColor: bgColors,
        borderColor: borderColors,
        borderWidth: chartType === 'pie' || chartType === 'doughnut' ? 2 : 1,
        fill: chartType === 'line' ? false : undefined,
        tension: chartType === 'line' ? 0.3 : undefined,
      }];

      this._chartInstance = new Chart(canvas, {
        type: chartType,
        data: {
          labels: chartType === 'scatter' ? undefined : labels,
          datasets: datasets,
        },
        options: {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: chartType === 'pie' || chartType === 'doughnut' },
            title: { display: true, text: `${agg === 'none' ? '' : agg.toUpperCase() + ' of '}${yCol} by ${xCol}` },
          },
          scales: chartType === 'pie' || chartType === 'doughnut' ? {} : {
            x: { title: { display: true, text: xCol } },
            y: { title: { display: true, text: agg === 'none' ? yCol : `${agg.toUpperCase()}(${yCol})` }, beginAtZero: true },
          },
        },
      });

      Toast.success('Chart generated');
    } catch (err) {
      Toast.error('Chart error: ' + err.message);
    }
  },
};

// ==========================================================================
//  MetadataViewer
// ==========================================================================

const MetadataViewer = {
  async load() {
    const container = document.querySelector('[data-tab-content="metadata"]');
    if (!container) return;

    if (!State.activeFileId) {
      container.innerHTML = '<div class="empty-state">No file selected</div>';
      return;
    }

    container.innerHTML = '<div class="loading-spinner"><div class="spinner"></div>Loading metadata...</div>';

    try {
      const meta = await DB.getMetadata(State.activeFileId);
      this._render(container, meta);
    } catch (err) {
      container.innerHTML = `<div class="error-state">Error: ${escapeHtml(err.message)}</div>`;
    }
  },

  _render(container, meta) {
    let html = `<div class="stats-cards">
      <div class="stat-card"><span class="stat-card-value">${formatNumber(meta.num_rows)}</span><span class="stat-card-label">Rows</span></div>
      <div class="stat-card"><span class="stat-card-value">${meta.num_columns}</span><span class="stat-card-label">Columns</span></div>
      <div class="stat-card"><span class="stat-card-value">${meta.num_row_groups}</span><span class="stat-card-label">Row Groups</span></div>
      <div class="stat-card"><span class="stat-card-value">${formatFileSize(meta.file_size_bytes)}</span><span class="stat-card-label">File Size</span></div>
    </div>`;

    html += `<div style="margin-bottom:16px;font-size:var(--font-sm);color:var(--text-muted);">
      File: ${escapeHtml(meta.file_name)}
    </div>`;

    // Row Groups
    if (meta.row_groups.length > 0) {
      html += `<h3 class="section-title">Row Groups</h3>
        <div class="table-wrapper"><table class="data-table">
          <thead><tr><th>Index</th><th class="text-right">Rows</th><th class="text-right">Size</th></tr></thead>
          <tbody>`;
      for (const rg of meta.row_groups) {
        html += `<tr>
          <td>${rg.index}</td>
          <td class="text-right">${formatNumber(rg.num_rows)}</td>
          <td class="text-right">${formatFileSize(rg.total_byte_size)}</td>
        </tr>`;
      }
      html += '</tbody></table></div>';
    }

    // Column metadata
    if (meta.columns_meta.length > 0) {
      html += `<h3 class="section-title">Column Metadata</h3>
        <div class="table-wrapper"><table class="data-table">
          <thead><tr><th>Name</th><th>Physical Type</th><th>Compression</th>
          <th class="text-right">Compressed</th><th class="text-right">Uncompressed</th><th class="text-right">Ratio</th></tr></thead>
          <tbody>`;
      for (const c of meta.columns_meta) {
        const ratio = c.total_uncompressed_size > 0
          ? (c.total_compressed_size / c.total_uncompressed_size * 100).toFixed(1) + '%'
          : '-';
        html += `<tr>
          <td>${escapeHtml(c.name)}</td>
          <td>${escapeHtml(c.physical_type || '-')}</td>
          <td>${escapeHtml(c.compression || '-')}</td>
          <td class="text-right">${formatFileSize(c.total_compressed_size)}</td>
          <td class="text-right">${formatFileSize(c.total_uncompressed_size)}</td>
          <td class="text-right">${ratio}</td>
        </tr>`;
      }
      html += '</tbody></table></div>';
    }

    // Schema from parquet_schema
    if (meta.columns_schema.length > 0) {
      html += `<h3 class="section-title">Parquet Schema</h3>
        <div class="table-wrapper"><table class="data-table">
          <thead><tr><th>Name</th><th>Type</th><th>Repetition</th><th>Logical Type</th><th>Converted Type</th></tr></thead>
          <tbody>`;
      for (const c of meta.columns_schema) {
        html += `<tr>
          <td>${escapeHtml(c.name || '-')}</td>
          <td>${escapeHtml(c.type || '-')}</td>
          <td>${escapeHtml(c.repetition_type || '-')}</td>
          <td>${escapeHtml(c.logical_type ? JSON.stringify(c.logical_type) : '-')}</td>
          <td>${escapeHtml(c.converted_type || '-')}</td>
        </tr>`;
      }
      html += '</tbody></table></div>';
    }

    container.innerHTML = html;
  },
};

// ==========================================================================
//  TransformManager
// ==========================================================================

const TransformManager = {
  load() {
    const container = document.querySelector('[data-tab-content="transforms"]');
    if (!container) return;

    if (!State.activeFileId) {
      container.innerHTML = '<div class="empty-state">No file selected</div>';
      return;
    }

    const schema = State._schema || [];
    const columnOptions = schema.map(c => `<option value="${escapeHtml(c.name)}">${escapeHtml(c.name)}</option>`).join('');

    const duckdbTypes = ['INTEGER', 'BIGINT', 'DOUBLE', 'FLOAT', 'VARCHAR', 'BOOLEAN', 'DATE', 'TIMESTAMP', 'DECIMAL(18,2)'];
    const typeOptions = duckdbTypes.map(t => `<option value="${t}">${t}</option>`).join('');

    container.innerHTML = `
      <div class="transform-wrapper">
        <div class="transform-form">
          <div class="form-group">
            <label class="form-label">Operation</label>
            <select class="input" id="transform-operation">
              <option value="rename">Rename Column</option>
              <option value="cast">Cast Column Type</option>
              <option value="drop">Drop Columns</option>
              <option value="compute">Add Computed Column</option>
              <option value="filter">Filter Rows</option>
              <option value="deduplicate">Deduplicate</option>
            </select>
          </div>

          <div id="transform-params">
            <div class="form-group" id="param-rename" style="display:none;">
              <label class="form-label">Column to Rename</label>
              <select class="input" id="rename-column">${columnOptions}</select>
              <label class="form-label" style="margin-top:8px;">New Name</label>
              <input class="input" type="text" id="rename-new-name" placeholder="new_column_name">
            </div>
            <div class="form-group" id="param-cast" style="display:none;">
              <label class="form-label">Column</label>
              <select class="input" id="cast-column">${columnOptions}</select>
              <label class="form-label" style="margin-top:8px;">New Type</label>
              <select class="input" id="cast-type">${typeOptions}</select>
            </div>
            <div class="form-group" id="param-drop" style="display:none;">
              <label class="form-label">Columns to Drop</label>
              <div class="checkbox-grid">
                ${schema.map(c => `<label class="checkbox-label"><input type="checkbox" value="${escapeHtml(c.name)}"> ${escapeHtml(c.name)}</label>`).join('')}
              </div>
            </div>
            <div class="form-group" id="param-compute" style="display:none;">
              <label class="form-label">Column Name</label>
              <input class="input" type="text" id="compute-name" placeholder="new_column">
              <label class="form-label" style="margin-top:8px;">Expression (SQL)</label>
              <input class="input" type="text" id="compute-expression" placeholder='e.g., "price" * "quantity"'>
            </div>
            <div class="form-group" id="param-filter" style="display:none;">
              <label class="form-label">WHERE clause (SQL)</label>
              <input class="input" type="text" id="filter-where" placeholder='e.g., "age" > 30 AND "status" = &#39;active&#39;'>
            </div>
            <div class="form-group" id="param-deduplicate" style="display:none;">
              <label class="form-label">Columns (leave empty for all)</label>
              <div class="checkbox-grid" id="dedup-columns">
                ${schema.map(c => `<label class="checkbox-label"><input type="checkbox" value="${escapeHtml(c.name)}"> ${escapeHtml(c.name)}</label>`).join('')}
              </div>
            </div>
          </div>

          <button class="btn btn-primary" id="btn-apply-transform" style="margin-top:16px;">Apply Transform</button>
        </div>

        <div class="transform-history">
          <h3 class="section-title">Transform History</h3>
          <div id="transform-history-list">
            ${State.transforms.length === 0 ? '<div class="empty-state" style="min-height:100px;">No transforms applied yet</div>' : ''}
            ${State.transforms.map((t, i) => `
              <div class="transform-history-item">
                <span class="transform-number">${i + 1}</span>
                <span class="transform-desc">${escapeHtml(t.description)}</span>
                <span class="transform-time">${escapeHtml(t.time)}</span>
              </div>
            `).join('')}
          </div>
        </div>
      </div>
    `;

    this._bindEvents(container);
  },

  _bindEvents(container) {
    const opSelect = container.querySelector('#transform-operation');
    const paramSections = ['rename', 'cast', 'drop', 'compute', 'filter', 'deduplicate'];

    const showParams = () => {
      const op = opSelect.value;
      for (const p of paramSections) {
        const el = container.querySelector(`#param-${p}`);
        if (el) el.style.display = p === op ? 'block' : 'none';
      }
    };
    opSelect.addEventListener('change', showParams);
    showParams();

    container.querySelector('#btn-apply-transform').addEventListener('click', () => this._applyTransform(container));
  },

  async _applyTransform(container) {
    const op = container.querySelector('#transform-operation').value;
    let params = {};
    let description = '';

    try {
      switch (op) {
        case 'rename': {
          const col = container.querySelector('#rename-column').value;
          const newName = container.querySelector('#rename-new-name').value.trim();
          if (!newName) { Toast.warning('Enter a new column name'); return; }
          params = { old_name: col, new_name: newName };
          description = `Rename "${col}" to "${newName}"`;
          break;
        }
        case 'cast': {
          const col = container.querySelector('#cast-column').value;
          const newType = container.querySelector('#cast-type').value;
          params = { column: col, new_type: newType };
          description = `Cast "${col}" to ${newType}`;
          break;
        }
        case 'drop': {
          const checked = Array.from(container.querySelectorAll('#param-drop input[type=checkbox]:checked'));
          const cols = checked.map(cb => cb.value);
          if (cols.length === 0) { Toast.warning('Select columns to drop'); return; }
          params = { columns: cols };
          description = `Drop columns: ${cols.join(', ')}`;
          break;
        }
        case 'compute': {
          const name = container.querySelector('#compute-name').value.trim();
          const expression = container.querySelector('#compute-expression').value.trim();
          if (!name || !expression) { Toast.warning('Enter column name and expression'); return; }
          params = { name, expression };
          description = `Add computed column "${name}" = ${expression}`;
          break;
        }
        case 'filter': {
          const where = container.querySelector('#filter-where').value.trim();
          if (!where) { Toast.warning('Enter a WHERE clause'); return; }
          params = { sql_where: where };
          description = `Filter: WHERE ${where}`;
          break;
        }
        case 'deduplicate': {
          const checked = Array.from(container.querySelectorAll('#dedup-columns input[type=checkbox]:checked'));
          const cols = checked.map(cb => cb.value);
          params = { columns: cols };
          description = cols.length > 0 ? `Deduplicate on: ${cols.join(', ')}` : 'Deduplicate (all columns)';
          break;
        }
      }

      Toast.info('Applying transform...');
      const newInfo = await DB.transform(State.activeFileId, op, params);
      State.files.push(newInfo);
      State.transforms.push({
        operation: op,
        description: description,
        time: new Date().toLocaleTimeString(),
        newFileId: newInfo.id,
      });

      FileManager.renderFileList();
      Toast.success(`Transform applied. New file: "${newInfo.name}" (${formatNumber(newInfo.numRows)} rows)`);

      // Refresh the transforms tab
      this.load();
    } catch (err) {
      Toast.error('Transform failed: ' + err.message);
    }
  },
};

// ==========================================================================
//  SettingsManager
// ==========================================================================

const SettingsManager = {
  init() {
    const btn = document.getElementById('btn-settings');
    if (btn) {
      btn.addEventListener('click', () => this.openModal());
    }
  },

  applySettings() {
    const root = document.documentElement;
    root.setAttribute('data-theme', State.settings.theme);
    root.setAttribute('data-accent', State.settings.accentColor);
    root.setAttribute('data-density', State.settings.density);
    root.setAttribute('data-font-size', State.settings.fontSize);
  },

  openModal() {
    const overlay = document.createElement('div');
    overlay.className = 'modal-overlay';
    overlay.innerHTML = `
      <div class="modal-content modal-large">
        <div class="modal-header">
          <h3>Settings</h3>
          <button class="modal-close">&times;</button>
        </div>
        <div class="modal-body">
          <div class="form-group">
            <label class="form-label">Theme</label>
            <div class="toggle-group">
              <button class="toggle-btn ${State.settings.theme === 'light' ? 'toggle-active' : ''}" data-theme-val="light">Light</button>
              <button class="toggle-btn ${State.settings.theme === 'dark' ? 'toggle-active' : ''}" data-theme-val="dark">Dark</button>
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Accent Color</label>
            <div class="color-buttons">
              ${['blue', 'green', 'purple', 'orange', 'red', 'teal'].map(c => `
                <button class="color-btn color-btn-${c} ${State.settings.accentColor === c ? 'color-active' : ''}" data-accent-val="${c}"></button>
              `).join('')}
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Density</label>
            <div class="toggle-group">
              ${['compact', 'normal', 'comfortable'].map(d => `
                <button class="toggle-btn ${State.settings.density === d ? 'toggle-active' : ''}" data-density-val="${d}">${d.charAt(0).toUpperCase() + d.slice(1)}</button>
              `).join('')}
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Font Size</label>
            <div class="toggle-group">
              ${['small', 'medium', 'large'].map(f => `
                <button class="toggle-btn ${State.settings.fontSize === f ? 'toggle-active' : ''}" data-fontsize-val="${f}">${f.charAt(0).toUpperCase() + f.slice(1)}</button>
              `).join('')}
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Page Size</label>
            <div class="toggle-group">
              ${[25, 50, 100, 250, 500].map(ps => `
                <button class="toggle-btn ${State.settings.pageSize === ps ? 'toggle-active' : ''}" data-pagesize-val="${ps}">${ps}</button>
              `).join('')}
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Null Display</label>
            <div class="toggle-group">
              ${['NULL', 'null', '-', '(empty)', 'N/A'].map(n => `
                <button class="toggle-btn ${State.settings.nullDisplay === n ? 'toggle-active' : ''}" data-null-val="${n}">${escapeHtml(n)}</button>
              `).join('')}
            </div>
          </div>
          <div class="form-group">
            <label class="form-label">Number Format</label>
            <div class="toggle-group">
              <button class="toggle-btn ${State.settings.numberFormat === 'comma' ? 'toggle-active' : ''}" data-numfmt-val="comma">1,234,567</button>
              <button class="toggle-btn ${State.settings.numberFormat === 'plain' ? 'toggle-active' : ''}" data-numfmt-val="plain">1234567</button>
            </div>
          </div>
        </div>
        <div class="modal-footer">
          <button class="btn btn-ghost" id="settings-close">Close</button>
        </div>
      </div>
    `;

    document.body.appendChild(overlay);

    const close = () => {
      overlay.remove();
      State.saveSettings();
    };

    overlay.querySelector('.modal-close').addEventListener('click', close);
    overlay.querySelector('#settings-close').addEventListener('click', close);
    overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });

    // Theme
    overlay.querySelectorAll('[data-theme-val]').forEach(btn => {
      btn.addEventListener('click', () => {
        State.settings.theme = btn.getAttribute('data-theme-val');
        this.applySettings();
        overlay.querySelectorAll('[data-theme-val]').forEach(b => b.classList.remove('toggle-active'));
        btn.classList.add('toggle-active');
      });
    });

    // Accent color
    overlay.querySelectorAll('[data-accent-val]').forEach(btn => {
      btn.addEventListener('click', () => {
        State.settings.accentColor = btn.getAttribute('data-accent-val');
        this.applySettings();
        overlay.querySelectorAll('[data-accent-val]').forEach(b => b.classList.remove('color-active'));
        btn.classList.add('color-active');
      });
    });

    // Density
    overlay.querySelectorAll('[data-density-val]').forEach(btn => {
      btn.addEventListener('click', () => {
        State.settings.density = btn.getAttribute('data-density-val');
        this.applySettings();
        overlay.querySelectorAll('[data-density-val]').forEach(b => b.classList.remove('toggle-active'));
        btn.classList.add('toggle-active');
      });
    });

    // Font size
    overlay.querySelectorAll('[data-fontsize-val]').forEach(btn => {
      btn.addEventListener('click', () => {
        State.settings.fontSize = btn.getAttribute('data-fontsize-val');
        this.applySettings();
        overlay.querySelectorAll('[data-fontsize-val]').forEach(b => b.classList.remove('toggle-active'));
        btn.classList.add('toggle-active');
      });
    });

    // Page size
    overlay.querySelectorAll('[data-pagesize-val]').forEach(btn => {
      btn.addEventListener('click', () => {
        State.settings.pageSize = parseInt(btn.getAttribute('data-pagesize-val'));
        State.pageSize = State.settings.pageSize;
        overlay.querySelectorAll('[data-pagesize-val]').forEach(b => b.classList.remove('toggle-active'));
        btn.classList.add('toggle-active');
      });
    });

    // Null display
    overlay.querySelectorAll('[data-null-val]').forEach(btn => {
      btn.addEventListener('click', () => {
        State.settings.nullDisplay = btn.getAttribute('data-null-val');
        overlay.querySelectorAll('[data-null-val]').forEach(b => b.classList.remove('toggle-active'));
        btn.classList.add('toggle-active');
      });
    });

    // Number format
    overlay.querySelectorAll('[data-numfmt-val]').forEach(btn => {
      btn.addEventListener('click', () => {
        State.settings.numberFormat = btn.getAttribute('data-numfmt-val');
        overlay.querySelectorAll('[data-numfmt-val]').forEach(b => b.classList.remove('toggle-active'));
        btn.classList.add('toggle-active');
      });
    });
  },
};

// ==========================================================================
//  KeyboardShortcuts
// ==========================================================================

const KeyboardShortcuts = {
  init() {
    document.addEventListener('keydown', (e) => {
      // Ctrl/Cmd+Enter: run SQL if on SQL tab
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        if (State.activeTab === 'sql') {
          e.preventDefault();
          SQLEditor._runQuery();
        }
      }

      // Escape: close modals
      if (e.key === 'Escape') {
        const modal = document.querySelector('.modal-overlay');
        if (modal) modal.remove();
      }
    });
  },
};

// ==========================================================================
//  Initialization
// ==========================================================================

async function initApp() {
  State.loadSettings();
  State.loadQueryHistory();
  State.loadBookmarks();
  SettingsManager.applySettings();

  // Wire up all UI immediately — don't wait for DuckDB
  FileManager.init();
  TabManager.init();
  SettingsManager.init();
  KeyboardShortcuts.init();

  const sidebarToggle = document.getElementById('btn-toggle-sidebar');
  const sidebar = document.getElementById('sidebar');
  if (sidebarToggle && sidebar) {
    sidebarToggle.addEventListener('click', () => sidebar.classList.toggle('sidebar--collapsed'));
  }

  // Show initial empty state
  const dataContainer = document.querySelector('[data-tab-content="data"]');
  if (dataContainer) {
    dataContainer.innerHTML = `
      <div class="empty-state">
        <p>Loading DuckDB engine...</p>
        <p style="font-size:var(--font-xs);color:var(--text-muted);">First load downloads ~15MB of WebAssembly (cached after)</p>
      </div>
    `;
  }

  // Load DuckDB in background — UI is already interactive
  try {
    await DB.init();
    Toast.success('Parquet Explorer ready');
    if (dataContainer) {
      dataContainer.innerHTML = `
        <div class="empty-state">
          <p>No file loaded</p>
          <p>Drop a .parquet file on the sidebar or click the upload area to get started</p>
        </div>
      `;
    }
  } catch (err) {
    Toast.error('Failed to initialize DuckDB: ' + err.message);
    console.error('DuckDB init error:', err);
    if (dataContainer) {
      dataContainer.innerHTML = `
        <div class="error-state">
          <p><strong>DuckDB-WASM failed to load</strong></p>
          <p>${escapeHtml(err.message)}</p>
          <p style="margin-top:12px;font-size:var(--font-xs);color:var(--text-muted);">
            Make sure you're serving this via HTTP (not file://). Try: <code>make serve</code>
          </p>
        </div>
      `;
    }
  }
}

initApp();
