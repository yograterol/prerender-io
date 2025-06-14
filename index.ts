/*
  Lightweight Prerender.io‑style server — v1.3 (Multi-worker + device‑aware + JS‑free)
  Bun + Puppeteer · Port 4000
  ──────────────────────────────────────────────────────────────────
  Snapshot‑only output: fully rendered DOM + inlined same‑origin CSS.
  ALL JavaScript is stripped **after** the page finishes rendering.
  Asset loading is allowed for any sub‑domain of the requested domain
  (e.g. static.example.com, img.cdn.example.com …).

  v1.3 changes (2025‑06‑12):
  • Multiple concurrent workers with atomic job claiming
  • Worker crash recovery with timeout mechanism
  • Separate browser instances per worker to avoid conflicts
  • Worker ID tracking and heartbeat system
*/

import { Database } from "bun:sqlite";
import puppeteer from "puppeteer";
import { minify } from "html-minifier-terser";
import * as zlib from "zlib";
import { XMLParser } from "fast-xml-parser";

/*──────────────────────────── Config ───────────────────────────*/
const PORT          = 4000;
const REFRESH_MS    = 1000 * 60 * 60 * 24 * 8;   // 8 days
const PURGE_MS      = 1000 * 60 * 60 * 24 * 60;  // 60 days
const PAGE_TIMEOUT  = 25_000;
const WORKER_COUNT  = 5;  // Number of concurrent workers
const CLAIM_TIMEOUT = 60_000;  // Job claim timeout (1 minute)

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
  claimed_by TEXT NULL,                  -- worker ID that claimed this job
  claimed_at INTEGER NULL,               -- when job was claimed
  PRIMARY KEY (url, device)
);

CREATE TABLE IF NOT EXISTS errors (
  url TEXT,
  device TEXT,
  status INTEGER NOT NULL,
  first_hit INTEGER NOT NULL,
  PRIMARY KEY (url, device)
);

CREATE TABLE IF NOT EXISTS workers (
  worker_id TEXT PRIMARY KEY,
  last_heartbeat INTEGER NOT NULL,
  started_at INTEGER NOT NULL
);

/*──────── Indexes ────────*/
CREATE INDEX IF NOT EXISTS idx_pages_url          ON pages(url);
CREATE INDEX IF NOT EXISTS idx_pages_fetched_at   ON pages(fetched_at);
CREATE INDEX IF NOT EXISTS idx_queue_priority     ON queue(priority, enqueued_at);
CREATE INDEX IF NOT EXISTS idx_queue_claimed      ON queue(claimed_by, claimed_at);
CREATE INDEX IF NOT EXISTS idx_errors_first_hit   ON errors(first_hit);
CREATE INDEX IF NOT EXISTS idx_domains_active     ON domains(active);
CREATE INDEX IF NOT EXISTS idx_workers_heartbeat  ON workers(last_heartbeat);
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
  db.query("INSERT OR REPLACE INTO queue (url, device, enqueued_at, priority, claimed_by, claimed_at) VALUES (?,?,?,?,NULL,NULL)")
    .run(url, dev, Date.now(), urgent ? 1 : 0);
}

// Helper to check if HTML content is valid (not empty/null)
function isValidHTML(html) {
  if (!html || typeof html !== 'string') return false;
  const trimmed = html.trim();
  if (!trimmed) return false;
  // Basic check for minimal HTML structure
  return trimmed.length > 50 && (trimmed.includes('<html') || trimmed.includes('<body') || trimmed.includes('<!DOCTYPE'));
}

// Remove invalid cache entry
function removeFromCache(url, dev) {
  db.query("DELETE FROM pages WHERE url = ? AND device = ?").run(url, dev);
}

// Worker management functions
function registerWorker(workerId) {
  const now = Date.now();
  db.query("INSERT OR REPLACE INTO workers (worker_id, last_heartbeat, started_at) VALUES (?,?,?)")
    .run(workerId, now, now);
}

function updateWorkerHeartbeat(workerId) {
  db.query("UPDATE workers SET last_heartbeat = ? WHERE worker_id = ?")
    .run(Date.now(), workerId);
}

// Atomically claim a job from the queue
function claimJob(workerId) {
  const now = Date.now();
  const timeoutThreshold = now - CLAIM_TIMEOUT;
  
  // First, release any stale claims
  db.query("UPDATE queue SET claimed_by = NULL, claimed_at = NULL WHERE claimed_at < ?")
    .run(timeoutThreshold);
  
  // Try to claim the highest priority unclaimed job
  const job = db.query(`
    SELECT url, device FROM queue 
    WHERE claimed_by IS NULL 
    ORDER BY priority DESC, enqueued_at ASC 
    LIMIT 1
  `).get();
  
  if (!job) return null;
  
  // Atomically claim the job
  const result = db.query(`
    UPDATE queue 
    SET claimed_by = ?, claimed_at = ? 
    WHERE url = ? AND device = ? AND claimed_by IS NULL
  `).run(workerId, now, job.url, job.device);
  
  // If we successfully claimed it, return the job
  if (result.changes > 0) {
    return job;
  }
  
  return null; // Someone else claimed it first
}

// Remove job from queue after completion
function completeJob(workerId, url, device) {
  db.query("DELETE FROM queue WHERE url = ? AND device = ? AND claimed_by = ?")
    .run(url, device, workerId);
}

/* robots.txt sitemap extractor */
const extractMaps = txt => txt.split(/\r?\n/).map(l => l.trim()).filter(l => /^sitemap:/i.test(l)).map(l => l.split(/\s+/)[1]).filter(Boolean);

/*──────────────────────── Puppeteer rendering ────────────────*/
function rootDomain(h) {
  return h.split('.').slice(-2).join('.');
}

async function createBrowser() {
  return await puppeteer.launch({ 
    headless: "new", 
    args: ["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"] 
  });
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

async function render(browser, full, dev, ua) {
  const page = await browser.newPage();
  const host = new URL(full).host;
  const root = rootDomain(host);

  await page.setUserAgent(ua || DEFAULT_UA[dev]);
  await page.setViewport(VIEWPORT[dev]);

  const FONT_OK = [host, `fonts.gstatic.com`, `cdnjs.cloudflare.com`,]; // Removed  `cdn.materialdesignicons.com`

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
async function createWorker(workerId) {
  registerWorker(workerId);
  const browser = await createBrowser();
  
  console.log(`🔧 Worker ${workerId} started with dedicated browser`);

  // Heartbeat interval
  const heartbeatInterval = setInterval(() => {
    updateWorkerHeartbeat(workerId);
  }, 10000); // Update every 10 seconds

  async function workerLoop() {
    while (true) {
      try {
        const job = claimJob(workerId);
        if (!job) {
          await Bun.sleep(1000);
          continue;
        }

        console.log(`🔧 Worker ${workerId} processing: ${job.url} (${job.device})`);

        if (!allowed(job.url.split('/')[0])) {
          completeJob(workerId, job.url, job.device);
          continue;
        }

        const snap = get(job.url, job.device);
        if (snap && Date.now() - snap.fetched_at < PURGE_MS) {
          completeJob(workerId, job.url, job.device);
          continue;
        }

        try {
          const { html, status } = await render(browser, `https://${job.url}`, job.device, DEFAULT_UA[job.device]);
          
          // Handle different scenarios
          if (status === 404) {
            // Store 404 errors in errors table
            db.query("INSERT OR IGNORE INTO errors (url, device, status, first_hit) VALUES (?,?,?,?)").run(job.url, job.device, status, Date.now());
          } else if (status === 200 && html) {
            // Try to minify the HTML
            let minifiedHtml;
            try {
              minifiedHtml = await minify(html, MINIFY_OPTS);
            } catch (minifyError) {
              console.warn(`⚠️  Worker ${workerId}: Minification failed for ${job.url}`, minifyError);
              minifiedHtml = html; // Fall back to unminified HTML
            }
            
            // Only cache if HTML is valid and not empty
            if (isValidHTML(minifiedHtml)) {
              save(job.url, job.device, minifiedHtml, status);
              console.log(`✅ Worker ${workerId}: Cached ${job.url} (${job.device})`);
            } else {
              console.warn(`⚠️  Worker ${workerId}: Invalid/empty HTML for ${job.url} - not caching, will retry`);
              // Remove any existing invalid cache entry
              removeFromCache(job.url, job.device);
              // Requeue for retry (but not as urgent to avoid infinite loops)
              push(job.url, job.device, false);
            }
          } else {
            // Non-200 status (except 404) - don't cache, but also remove any existing cache
            console.warn(`⚠️  Worker ${workerId}: Non-200 status ${status} for ${job.url} - removing from cache`);
            removeFromCache(job.url, job.device);
          }
        } catch (e) {
          console.error(`❌ Worker ${workerId}: Render error for ${job.url}`, e);
          // On render error, remove any existing cache entry
          removeFromCache(job.url, job.device);
        }

        completeJob(workerId, job.url, job.device);
        
      } catch (e) {
        console.error(`❌ Worker ${workerId}: Unexpected error`, e);
        await Bun.sleep(5000); // Wait before retrying
      }
    }
  }

  // Handle cleanup on process exit
  process.on('SIGINT', async () => {
    clearInterval(heartbeatInterval);
    await browser.close();
    console.log(`🔧 Worker ${workerId} shut down gracefully`);
  });

  return workerLoop();
}

// Start multiple workers
async function startWorkers() {
  const workers = [];
  for (let i = 1; i <= WORKER_COUNT; i++) {
    const workerId = `worker-${i}-${Date.now()}`;
    workers.push(createWorker(workerId));
  }
  await Promise.all(workers);
}

/*────────────── robots.txt & sitemap ingestion ───────────────*/
async function robots(host) {
  try { const res = await fetch(`https://${host}/robots.txt`); if (!res.ok) return; for (const sm of extractMaps(await res.text())) await sitemap(sm); } catch {}
}
async function sitemap(url) {
  try {
    console.log(`📋 Processing sitemap: ${url}`);
    const res = await fetch(url); 
    if (!res.ok) return 0;
    
    let buf = Buffer.from(await res.arrayBuffer());
    if (url.endsWith('.gz')) buf = zlib.gunzipSync(buf);
    const xml = new XMLParser({ ignoreAttributes: false }).parse(buf.toString());
    
    let urlsToProcess = [];
    
    if (xml.urlset?.url) {
      urlsToProcess = xml.urlset.url.map(u => norm(u.loc));
    }
    
    if (urlsToProcess.length === 0) {
      if (xml.sitemapindex?.sitemap) {
        let totalProcessed = 0;
        for (const s of xml.sitemapindex.sitemap) {
          totalProcessed += await sitemapSuperEfficient(s.loc) || 0;
        }
        return totalProcessed;
      }
      return 0;
    }
    
    // Super efficient: Use a single query to insert only new URLs
    const devices = ['desktop', 'mobile'];
    const insertNewUrls = db.query(`
      INSERT INTO queue (url, device, enqueued_at, priority, claimed_by, claimed_at) 
      SELECT ?, ?, ?, 0, NULL, NULL
      WHERE NOT EXISTS (
        SELECT 1 FROM queue WHERE url = ? AND device = ?
      ) AND NOT EXISTS (
        SELECT 1 FROM pages WHERE url = ? AND device = ?
      )
    `);
    
    let insertCount = 0;
    const now = Date.now();
    
    for (const url of urlsToProcess) {
      for (const device of devices) {
        const result = insertNewUrls.run(url, device, now, url, device, url, device);
        insertCount += result.changes;
      }
    }
    
    // Handle nested sitemaps
    if (xml.sitemapindex?.sitemap) {
      for (const s of xml.sitemapindex.sitemap) {
        await sitemapSuperEfficient(s.loc);
      }
    }
    
    console.log(`📋 Sitemap ${url} processed: ${insertCount} new URLs queued`);
    return insertCount;
    
  } catch (e) {
    console.error(`❌ Error processing sitemap ${url}:`, e);
    return 0;
  }
}

/*──────────────────────── HTTP server ─────────────────────────*/
Bun.serve({
  idleTimeout: 35,
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
      if (pathname === '/admin/workers') {
        const workers = db.query('SELECT worker_id, last_heartbeat, started_at FROM workers ORDER BY last_heartbeat DESC').all();
        const now = Date.now();
        return Response.json(workers.map(w => ({
          ...w,
          alive: (now - w.last_heartbeat) < 30000, // Consider alive if heartbeat within 30s
          uptime: now - w.started_at
        })));
      }
      if (pathname === '/admin/queue') {
        const queue = db.query('SELECT url, device, priority, claimed_by, claimed_at, enqueued_at FROM queue ORDER BY priority DESC, enqueued_at ASC LIMIT 50').all();
        return Response.json(queue);
      }
      if (pathname === '/admin/clean-empty') {
        const emptyEntries = db.query("SELECT url, device FROM pages WHERE html IS NULL OR html = '' OR LENGTH(TRIM(html)) < 50").all();
        for (const entry of emptyEntries) {
          removeFromCache(entry.url, entry.device);
          push(entry.url, entry.device, false);
        }
        return new Response(`Cleaned ${emptyEntries.length} empty cache entries`);
      }
      return new Response('Not Found', { status: 404 });
    }

    /* Render endpoint */
    if (pathname === '/render') {
      const target = searchParams.get('url'); if (!target) return new Response('Missing url', { status: 400 });
      const originalUA = req.headers.get('user-agent') || '';
      const ua      = originalUA + ' prerender';
      const device  = deviceFromUA(ua);
      const urlKey  = norm(target);

      // Log the user agent information
      console.log(`[RENDER] ${new Date().toISOString()} | URL: ${target} | User-Agent: "${originalUA}" | Device: ${device}`);

      if (!allowed(urlKey.split('/')[0])) return new Response('Domain not allowed', { status: 403 });

      const err = db.query('SELECT status FROM errors WHERE url = ? AND device = ?').get(urlKey, device);
      if (err) return new Response('Not found', { status: err.status, headers: { 'X-Prerender-Cache': '404' }});

      // 1️⃣ Check cache first
      let snap = get(urlKey, device);
      if (snap) {
        // Check if cached HTML is valid, if not, treat as cache miss
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

      // 3️⃣ Give workers a window to process (max = PAGE_TIMEOUT + 10 s)
      const DEADLINE = Date.now() + PAGE_TIMEOUT + 10_000;
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

      // 4️⃣ If still not ready after extended wait, return a generic error
      console.warn(`[RENDER] Timeout waiting for ${urlKey} (${device}) - rendering took too long`);
      return new Response('Service temporarily unavailable', {
        status: 503,
        headers: {
          'X-Prerender-Cache': 'TIMEOUT'
        }
      });
    }

    return new Response('Not Found', { status: 404 });
  }
});

/*──────────── Boot ───────────*/
// Clean up any stale worker registrations on startup
db.exec('DELETE FROM workers WHERE last_heartbeat < ?', Date.now() - 60000);

// Load domains and start robots crawling
for (const { domain } of db.query('SELECT domain FROM domains WHERE active = 1').all()) robots(domain);

console.log(`🚀 Prerender server running on port ${PORT} with ${WORKER_COUNT} workers`);

// Start the workers
startWorkers().catch(console.error);