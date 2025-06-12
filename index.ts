/*
  Lightweight Prerender.io-style server — v1.1 (device-aware + JS-free)
  Bun + Puppeteer · Port 4000
  ──────────────────────────────────────────────────────────────────
  Snapshot-only output: fully rendered DOM + inlined same-origin CSS.
  ALL JavaScript is stripped **after** the page finishes rendering.
  Asset loading is allowed for any sub-domain of the requested domain
  (e.g. static.example.com, img.cdn.example.com …).

  v1.1 changes:
  • Removed all console.log statements to keep stdout clean.
  • Hardened iframe sanitisation: absolutely no <iframe> or sub-frame
    network requests are allowed unless the host is explicitly whitelisted.
*/

import { Database } from "bun:sqlite";
import puppeteer from "puppeteer";
import { minify } from "html-minifier-terser";
import * as zlib from "zlib";
import { XMLParser } from "fast-xml-parser";

/*──────────────────────────── Config ───────────────────────────*/
const PORT          = 4000;
const REFRESH_MS    = 1000 * 60 * 60 * 24 * 7;   // 7 days
const PURGE_MS      = 1000 * 60 * 60 * 24 * 30;  // 30 days
const PAGE_TIMEOUT  = 25_000;

const MINIFY_OPTS = {
  collapseWhitespace: true,
  removeComments: true,
  removeRedundantAttributes: true,
  minifyCSS: true,
  minifyJS: true,
};

const BLOCKED_SCRIPT_PATTERNS = [
  /google(apis|tagmanager|analytics)\.com/i,
  /gtag\/js/i,
  /googletagmanager\.com/i,
  /facebook\.com/i,
  /connect\.facebook\.net/i,
  /intercom\.io/i,
  /hotjar\.com/i,
  /mixpanel\.com/i,
  /clarity\.ms/i,
];

/*
  Explicit iframe allow-list.  If empty (default), **all** iframe/sub-frame
  requests are aborted.  Populate with hosts ("example.com") to permit them.
*/
const ALLOWED_IFRAME_HOSTS = [];

/*──────────────────── Device detection ───────────────────────*/
function deviceFromUA(ua = "") {
  const low = ua.toLowerCase();
  const tablet = /(ipad|tablet|kindle|playbook|silk)/.test(low) && !/mobi/.test(low);
  const mobile = /mobi|android|iphone|ipod|blackberry|phone/.test(low);
  if (tablet) return "tablet";
  if (mobile) return "mobile";
  return "desktop";
}

const DEFAULT_UA = {
  mobile:   "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) prerender headlesschrome AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  tablet:   "Mozilla/5.0 (iPad; CPU OS 17_0 like Mac OS X) prerender headlesschrome AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
  desktop:  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) prerender headlesschrome AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
};

const VIEWPORT = {
  mobile:  { width: 390,  height: 844,  isMobile: true  },
  tablet:  { width: 820,  height: 1180, isMobile: false },
  desktop: { width: 1366, height: 768,  isMobile: false },
};

/*────────────────────────── SQLite schema ─────────────────────*/
const db = new Database("cache.sqlite", { create: true });

// Performance-optimized pragma settings for caching workload
db.exec(`
  -- WAL mode for better concurrent read/write performance
  PRAGMA journal_mode = WAL;
  
  -- Reduce fsync frequency (cache data can be regenerated if lost)
  PRAGMA synchronous = NORMAL;
  
  -- Increase cache size to 64MB (adjust based on available RAM)
  PRAGMA cache_size = -65536;
  
  -- Use memory for temporary operations
  PRAGMA temp_store = MEMORY;
  
  -- Enable memory-mapped I/O for better performance (256MB)
  PRAGMA mmap_size = 268435456;
  
  -- Set busy timeout to handle concurrent access (5 seconds)
  PRAGMA busy_timeout = 5000;
  
  -- Enable foreign key constraints for data integrity
  PRAGMA foreign_keys = ON;
  
  -- Auto-vacuum to manage database growth over time
  PRAGMA auto_vacuum = INCREMENTAL;
  
  -- Optimize for bulk operations and reduce checkpointing frequency
  PRAGMA wal_autocheckpoint = 10000;
  
  -- Analysis and optimization settings
  PRAGMA optimize = 0x02;
  PRAGMA analysis_limit = 1000;
  `);

db.exec(`
CREATE TABLE IF NOT EXISTS pages (
  url TEXT,
  device TEXT,
  html TEXT,
  status INTEGER NOT NULL DEFAULT 200,
  fetched_at INTEGER NOT NULL,
  PRIMARY KEY (url, device)
);
CREATE TABLE IF NOT EXISTS domains (
  domain TEXT PRIMARY KEY,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS queue (
  url TEXT,
  device TEXT,
  enqueued_at INTEGER NOT NULL,
  PRIMARY KEY (url, device)
);
CREATE TABLE IF NOT EXISTS errors (
  url TEXT,
  device TEXT,
  status INTEGER NOT NULL,
  first_hit INTEGER NOT NULL,
  PRIMARY KEY (url, device)
);

-- Performance indexes
CREATE INDEX IF NOT EXISTS idx_pages_url ON pages(url);
CREATE INDEX IF NOT EXISTS idx_pages_fetched_at ON pages(fetched_at);
CREATE INDEX IF NOT EXISTS idx_queue_enqueued_at ON queue(enqueued_at);
CREATE INDEX IF NOT EXISTS idx_errors_first_hit ON errors(first_hit);
CREATE INDEX IF NOT EXISTS idx_domains_active ON domains(active);
`);

/*────────────────────────── Helpers ───────────────────────────*/
function norm(raw) {
  if (!/^https?:\/\//i.test(raw)) raw = `https://${raw}`;
  const u = new URL(raw);
  const path = u.pathname.endsWith("/") && u.pathname !== "/" ? u.pathname.slice(0, -1) : u.pathname;
  return `${u.host}${path}`;
}
function allowed(host) {
  const r = db.query("SELECT active FROM domains WHERE domain = ?").get(host);
  return !!r?.active;
}
function save(url, dev, html, status = 200) {
  db.query("INSERT OR REPLACE INTO pages (url, device, html, status, fetched_at) VALUES (?,?,?,?,?)")
    .run(url, dev, html, status, Date.now());
}
function get(url, dev) {
  return db.query("SELECT html, status, fetched_at FROM pages WHERE url = ? AND device = ?").get(url, dev) || null;
}
function push(url, dev) {
  db.query("INSERT OR IGNORE INTO queue (url, device, enqueued_at) VALUES (?,?,?)").run(url, dev, Date.now());
}

/* robots.txt sitemap extractor */
const extractMaps = txt => txt.split(/\r?\n/).map(l => l.trim()).filter(l => /^sitemap:/i.test(l)).map(l => l.split(/\s+/)[1]).filter(Boolean);

/*──────────────────────── Puppeteer pool ──────────────────────*/
const BROWSER = puppeteer.launch({ headless: "new", args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"] });

function rootDomain(h) {
  return h.split('.').slice(-2).join('.');
}

async function inlineCSS(page, origin) {
  await page.evaluate(async (origin) => {
    const links = Array.from(document.querySelectorAll('link[rel="stylesheet"][href]'));
    for (const link of links) {
      const href = link.getAttribute("href");
      if (!href || (!href.startsWith("/") && !href.startsWith(origin))) continue;
      try {
        const res = await fetch(href.startsWith("/") ? origin + href : href);
        if (!res.ok) continue;
        const css = await res.text();
        const s = document.createElement("style");
        s.textContent = css;
        link.replaceWith(s);
      } catch {}
    }
  }, origin);
}

async function sanitizeJS(page) {
  await page.evaluate(() => {
    /* Remove every resource that can execute JS */
    document.querySelectorAll('script, link[rel="modulepreload"], link[as="script"]').forEach(n => n.remove());

    /* Strip inline on* attributes */
    document.querySelectorAll('*').forEach(el => {
      for (const attr of Array.from(el.attributes)) {
        if (/^on/i.test(attr.name)) el.removeAttribute(attr.name);
      }
    });

    /* Remove javascript: href/src */
    document.querySelectorAll('[href^="javascript:"], [src^="javascript:"]').forEach(n => {
      n.removeAttribute('href');
      n.removeAttribute('src');
    });
  });
}

async function render(full, dev, ua) {
  const browser = await BROWSER;
  const page    = await browser.newPage();
  const host    = new URL(full).host;
  const root    = rootDomain(host);

  await page.setUserAgent(ua || DEFAULT_UA[dev]);
  await page.setViewport(VIEWPORT[dev]);

  const FONT_OK = [host, `fonts.gstatic.com`, `cdnjs.cloudflare.com`, `cdn.materialdesignicons.com`];

  /*──── Request interception ────*/
  await page.setRequestInterception(true);
  page.on("request", req => {
    const type = req.resourceType();
    const url  = req.url();

    /* Absolutely block every iframe/sub-frame unless explicitly allowed */
    if (type === "sub_frame") {
      try {
        const h = new URL(url).host;
        if (!ALLOWED_IFRAME_HOSTS.includes(h)) return req.abort();
      } catch { return req.abort(); }
    }

    const sameSite = (() => {
      try {
        const h = new URL(url).host;
        return h === host || h.endsWith(`.${host}`) || rootDomain(h) === root;
      } catch { return false; }
    })();

    /* Block requests from frames that aren't the main frame */
    if (req.frame() !== page.mainFrame()) return req.abort();

    /* Block media to speed-up snapshot */
    if (type === "media") return req.abort();

    /* Block external object/embed elements */
    if ((type === "other" || type === "object") && !sameSite) return req.abort();

    /* Scripts: block external + known tracking patterns */
    if (type === "script") {
      if (!sameSite && BLOCKED_SCRIPT_PATTERNS.some(re => re.test(url))) return req.abort();
      if (!sameSite) return req.abort(); // no external scripts at all
    }

    /* Fonts: only allow from trusted hosts */
    if (type === "font" && !(sameSite || FONT_OK.includes(new URL(url).host))) return req.abort();

    /* Block external stylesheets that might load external content */
    if (type === "stylesheet" && !sameSite && !FONT_OK.includes(new URL(url).host)) return req.abort();

    /* Otherwise allow */
    req.continue();
  });

  let status = 0;
  try {
    status = (await page.goto(full, { waitUntil: "networkidle0", timeout: PAGE_TIMEOUT }))?.status() ?? 0;
  } catch {}

  if (status === 200) {
    /* Extra wait to let late JS mutations finish */
    await new Promise(resolve => setTimeout(resolve, 1000));

    /* Scroll to the bottom so lazy-loaded images appear */
    await page.evaluate(async () => {
      window.scrollTo(0, document.body.scrollHeight);
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
    });

    /* Fix lazy-loaded media → absolute URLs */
    await page.evaluate(() => {
      const abs = (u) => /^(https?:)?\/\//i.test(u) || u.startsWith("data:") ? u : (u.startsWith("/") ? location.origin + u : location.origin + "/" + u);
      document.querySelectorAll('img[src], image[href], use[href], use[xlink\\:href], source[srcset]').forEach(el => {
        if (el instanceof HTMLImageElement) el.src = abs(el.getAttribute('src') || '');
        else if (el.tagName === 'image' || el.tagName === 'use') {
          const h = el.getAttribute('href') || el.getAttribute('xlink:href'); if (h) el.setAttribute('href', abs(h));
        }
      });
    });

    await inlineCSS(page, `https://${host}`);
    await sanitizeJS(page);  // strip JS only after everything rendered
  }

  const html = status === 200 ? await page.content() : null;
  await page.close();
  return { html, status };
}

/*────────────────────── Queue worker ─────────────────────────*/
(async function worker() {
  const next = db.query("SELECT url, device FROM queue ORDER BY enqueued_at ASC LIMIT 1");
  const del  = db.query("DELETE FROM queue WHERE url = ? AND device = ?");
  while (true) {
    const r = next.get();
    if (!r) { await Bun.sleep(1000); continue; }
    del.run(r.url, r.device);
    if (!allowed(r.url.split('/')[0])) continue;
    const snap = get(r.url, r.device);
    if (snap && Date.now() - snap.fetched_at < PURGE_MS) continue;
    try {
      const { html, status } = await render(`https://${r.url}`, r.device, DEFAULT_UA[r.device]);
      if (status === 404) db.query("INSERT OR IGNORE INTO errors (url, device, status, first_hit) VALUES (?,?,?,?)").run(r.url, r.device, status, Date.now());
      save(r.url, r.device, html ? await minify(html, MINIFY_OPTS) : null, status || 0);
    } catch (e) {
      console.error("Render error", r, e);
    }
  }
})();

/*────────────── robots.txt & sitemap ingestion ───────────────*/
async function robots(host) {
  try { const res = await fetch(`https://${host}/robots.txt`); if (!res.ok) return; for (const sm of extractMaps(await res.text())) await sitemap(sm); } catch {}
}
async function sitemap(url) {
  try {
    const res = await fetch(url); if (!res.ok) return;
    let buf = Buffer.from(await res.arrayBuffer());
    if (url.endsWith('.gz')) buf = zlib.gunzipSync(buf);
    const xml = new XMLParser({ ignoreAttributes: false }).parse(buf.toString());
    const ins = db.query("INSERT OR IGNORE INTO queue (url, device, enqueued_at) VALUES (?,?,?)");
    const p   = u => ins.run(norm(u), 'desktop', Date.now());
    if (xml.urlset?.url) xml.urlset.url.forEach(u => p(u.loc));
    if (xml.sitemapindex?.sitemap) for (const s of xml.sitemapindex.sitemap) await sitemap(s.loc);
  } catch {}
}

/*──────────────────────── HTTP server ─────────────────────────*/
Bun.serve({
  port: PORT,
  async fetch(req) {
    const { pathname, searchParams } = new URL(req.url);
    if (pathname === '/health') return new Response('ok');

    /* Admin API */
    if (pathname.startsWith('/admin')) {
      if (pathname === '/admin/domains' && req.method === 'POST') {
        const { domain, active = true } = await req.json();
        db.query('INSERT OR REPLACE INTO domains (domain, active) VALUES (?,?)').run(domain, active ? 1 : 0);
        if (active) robots(domain);
        return new Response('saved');
      }
      if (pathname === '/admin/domains' && req.method === 'GET') {
        return Response.json(db.query('SELECT domain, active FROM domains').all());
      }
      if (pathname === '/admin/flush') { db.exec('DELETE FROM pages; DELETE FROM errors;'); return new Response('cache cleared'); }
      return new Response('Not Found', { status: 404 });
    }

    /* Render endpoint */
    if (pathname === '/render') {
      const target = searchParams.get('url'); if (!target) return new Response('Missing url', { status: 400 });
      const ua      = req.headers.get('user-agent') + ' prerender' || '';
      const device  = deviceFromUA(ua);
      const urlKey  = norm(target);
      if (!allowed(urlKey.split('/')[0])) return new Response('Domain not allowed', { status: 403 });

      const err = db.query('SELECT status FROM errors WHERE url = ? AND device = ?').get(urlKey, device); if (err) return new Response('Not found', { status: err.status, headers: { 'X-Prerender-Cache': '404' } });
      const snap = get(urlKey, device);
      if (snap) {
        if (Date.now() - snap.fetched_at > REFRESH_MS) push(urlKey, device);
        return new Response(snap.html ?? '', { status: snap.status || 200, headers: { 'Content-Type': 'text/html; charset=utf-8', 'X-Prerender-Cache': 'HIT' } });
      }

      const { html, status } = await render(`https://${urlKey}`, device, ua);
      const min = html ? await minify(html, MINIFY_OPTS) : null;
      if (status === 404) db.query('INSERT OR IGNORE INTO errors (url, device, status, first_hit) VALUES (?,?,?,?)').run(urlKey, device, status, Date.now());
      save(urlKey, device, min, status || 0);
      return new Response(min ?? '', { status: status || 200, headers: { 'Content-Type': 'text/html; charset=utf-8', 'X-Prerender-Cache': 'MISS' } });
    }

    return new Response('Not Found', { status: 404 });
  }
});

/*──────────── Boot ───────────*/
for (const { domain } of db.query('SELECT domain FROM domains WHERE active = 1').all()) robots(domain);
