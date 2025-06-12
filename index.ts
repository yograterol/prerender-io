/*
  Lightweight Prerender.io‑style server — v1.2 (device‑aware + JS‑free)
  Bun + Puppeteer · Port 4000
  ──────────────────────────────────────────────────────────────────
  Snapshot‑only output: fully rendered DOM + inlined same‑origin CSS.
  ALL JavaScript is stripped **after** the page finishes rendering.
  Asset loading is allowed for any sub‑domain of the requested domain
  (e.g. static.example.com, img.cdn.example.com …).

  v1.2 changes (2025‑06‑12):
  • /render now queues first‑time URLs with high priority and waits for the
    worker, instead of spawning its own Puppeteer instance.
  • Queue table gains a `priority` column (0 = background, 1 = urgent).
  • Worker drains urgent jobs before normal ones.
  • Helper `push()` accepts an `urgent` flag.
  • FIXED: Avoid caching empty HTML, requeue and remove bad entries
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
  Explicit iframe allow‑list.  If empty (default), **all** iframe/sub‑frame
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

// Performance‑optimized pragma settings for caching workload
db.exec(`
  PRAGMA journal_mode = WAL;
  PRAGMA synchronous = NORMAL;
  PRAGMA cache_size = -65536;          -- 64 MB
  PRAGMA temp_store = MEMORY;
  PRAGMA mmap_size = 268435456;        -- 256 MB
  PRAGMA busy_timeout = 5000;
  PRAGMA foreign_keys = ON;
  PRAGMA auto_vacuum = INCREMENTAL;
  PRAGMA wal_autocheckpoint = 10000;
  PRAGMA optimize = 0x02;
  PRAGMA analysis_limit = 1000;
`);

// Main tables
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
  priority INTEGER NOT NULL DEFAULT 0,  -- 0 = background, 1 = urgent
  PRIMARY KEY (url, device)
);

CREATE TABLE IF NOT EXISTS errors (
  url TEXT,
  device TEXT,
  status INTEGER NOT NULL,
  first_hit INTEGER NOT NULL,
  PRIMARY KEY (url, device)
);

/*──────── Indexes ────────*/
CREATE INDEX IF NOT EXISTS idx_pages_url          ON pages(url);
CREATE INDEX IF NOT EXISTS idx_pages_fetched_at   ON pages(fetched_at);
CREATE INDEX IF NOT EXISTS idx_queue_priority     ON queue(priority, enqueued_at);
CREATE INDEX IF NOT EXISTS idx_errors_first_hit   ON errors(first_hit);
CREATE INDEX IF NOT EXISTS idx_domains_active     ON domains(active);
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

function push(url, dev, urgent = false) {
  db.query("INSERT OR REPLACE INTO queue (url, device, enqueued_at, priority) VALUES (?,?,?,?)")
    .run(url, dev, Date.now(), urgent ? 1 : 0);
}

// NEW: Helper to check if HTML content is valid (not empty/null)
function isValidHTML(html) {
  if (!html || typeof html !== 'string') return false;
  const trimmed = html.trim();
  if (!trimmed) return false;
  // Basic check for minimal HTML structure
  return trimmed.length > 50 && (trimmed.includes('<html') || trimmed.includes('<body') || trimmed.includes('<!DOCTYPE'));
}

// NEW: Remove invalid cache entry
function removeFromCache(url, dev) {
  db.query("DELETE FROM pages WHERE url = ? AND device = ?").run(url, dev);
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

    /* Absolutely block every iframe/sub‑frame unless explicitly allowed */
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

    /* Block media to speed‑up snapshot */
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

    /* Scroll to the bottom so lazy‑loaded images appear */
    await page.evaluate(async () => {
      window.scrollTo(0, document.body.scrollHeight);
      await new Promise(r => requestAnimationFrame(() => requestAnimationFrame(r)));
    });

    /* Fix lazy‑loaded media → absolute URLs */
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
  const next = db.query("SELECT url, device FROM queue ORDER BY priority DESC, enqueued_at ASC LIMIT 1");
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
      
      // Handle different scenarios
      if (status === 404) {
        // Store 404 errors in errors table
        db.query("INSERT OR IGNORE INTO errors (url, device, status, first_hit) VALUES (?,?,?,?)").run(r.url, r.device, status, Date.now());
      } else if (status === 200 && html) {
        // Try to minify the HTML
        let minifiedHtml;
        try {
          minifiedHtml = await minify(html, MINIFY_OPTS);
        } catch (minifyError) {
          console.warn("Minification failed for", r.url, minifyError);
          minifiedHtml = html; // Fall back to unminified HTML
        }
        
        // FIXED: Only cache if HTML is valid and not empty
        if (isValidHTML(minifiedHtml)) {
          save(r.url, r.device, minifiedHtml, status);
        } else {
          console.warn("Invalid/empty HTML detected for", r.url, "- not caching, will retry");
          // Remove any existing invalid cache entry
          removeFromCache(r.url, r.device);
          // Requeue for retry (but not as urgent to avoid infinite loops)
          push(r.url, r.device, false);
        }
      } else {
        // Non-200 status (except 404) - don't cache, but also remove any existing cache
        console.warn("Non-200 status", status, "for", r.url, "- removing from cache");
        removeFromCache(r.url, r.device);
        // Could implement retry logic here with exponential backoff
      }
    } catch (e) {
      console.error("Render error", r, e);
      // On render error, remove any existing cache entry and potentially requeue
      removeFromCache(r.url, r.device);
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
    const ins = db.query("INSERT OR IGNORE INTO queue (url, device, enqueued_at, priority) VALUES (?,?,?,0)");
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
      if (pathname === '/admin/flush') { 
        db.exec('DELETE FROM pages; DELETE FROM errors;'); 
        return new Response('cache cleared'); 
      }
      // NEW: Admin endpoint to clean empty cache entries
      if (pathname === '/admin/clean-empty') {
        const emptyEntries = db.query("SELECT url, device FROM pages WHERE html IS NULL OR html = '' OR LENGTH(TRIM(html)) < 50").all();
        for (const entry of emptyEntries) {
          removeFromCache(entry.url, entry.device);
          // Optionally requeue for retry
          push(entry.url, entry.device, false);
        }
        return new Response(`Cleaned ${emptyEntries.length} empty cache entries`);
      }
      return new Response('Not Found', { status: 404 });
    }

    /* Render endpoint */
    if (pathname === '/render') {
      const target = searchParams.get('url'); if (!target) return new Response('Missing url', { status: 400 });
      const ua      = (req.headers.get('user-agent') || '') + ' prerender';
      const device  = deviceFromUA(ua);
      const urlKey  = norm(target);

      if (!allowed(urlKey.split('/')[0])) return new Response('Domain not allowed', { status: 403 });

      const err = db.query('SELECT status FROM errors WHERE url = ? AND device = ?').get(urlKey, device);
      if (err) return new Response('Not found', { status: err.status, headers: { 'X-Prerender-Cache': '404' }});

      // 1️⃣ Check cache first
      let snap = get(urlKey, device);
      if (snap) {
        // FIXED: Check if cached HTML is valid, if not, treat as cache miss
        if (!isValidHTML(snap.html)) {
          console.warn("Invalid HTML in cache for", urlKey, "- removing and retrying");
          removeFromCache(urlKey, device);
          snap = null; // Treat as cache miss
        } else {
          // Valid cache hit
          if (Date.now() - snap.fetched_at > REFRESH_MS) push(urlKey, device); // soft refresh in background
          return new Response(snap.html, {
            status: snap.status || 200,
            headers: {
              'Content-Type': 'text/html; charset=utf-8',
              'X-Prerender-Cache': 'HIT'
            }
          });
        }
      }

      // 2️⃣ Not cached (or invalid cache) → enqueue with high priority
      push(urlKey, device, true);

      // 3️⃣ Give worker a window to process (max = PAGE_TIMEOUT + 3 s)
      const DEADLINE = Date.now() + PAGE_TIMEOUT + 3_000;
      while (Date.now() < DEADLINE) {
        await Bun.sleep(250);
        snap = get(urlKey, device);
        if (snap && isValidHTML(snap.html)) break;
      }

      if (snap && isValidHTML(snap.html)) {
        return new Response(snap.html, {
          status: snap.status || 200,
          headers: {
            'Content-Type': 'text/html; charset=utf-8',
            'X-Prerender-Cache': 'MISS-WAIT'
          }
        });
      }

      // 4️⃣ Still not done → tell client to retry soon
      return new Response('Rendering in progress', {
        status: 202,
        headers: {
          'Retry-After': '15',
          'X-Prerender-Cache': 'QUEUED'
        }
      });
    }

    return new Response('Not Found', { status: 404 });
  }
});

/*──────────── Boot ───────────*/
for (const { domain } of db.query('SELECT domain FROM domains WHERE active = 1').all()) robots(domain);

console.log(`🚀 Prerender server running on port ${PORT}`);