/**
 * TTL Companion - Background Service Worker
 *
 * Responsibilities:
 *   1) Relay messages between popup and the active tab's content script.
 *   2) Run the local-queue retry drainer: every captured item is persisted
 *      to chrome.storage.local by the content script BEFORE upload. If the
 *      upload failed (server down, offline, etc.), this drainer retries it
 *      on a recurring alarm so no data is ever lost.
 */

const RETRY_ALARM_NAME = "ttl_retry_drain";
const RETRY_PERIOD_MINUTES = 0.5;   // every 30s
const MAX_ATTEMPTS_PER_RUN = 20;    // don't hammer the server

/**
 * Expected content script version. Must match CONTENT_SCRIPT_VERSION in
 * content.js. If a tab reports anything else back to our "status" ping,
 * we force-reinject the fresh file over the top so we never silently
 * run against a stale injection.
 */
const EXPECTED_CONTENT_VERSION = "1.4.0.2";

async function ensureContentScript(tabId) {
  let needsInject = false;
  try {
    const resp = await chrome.tabs.sendMessage(tabId, { action: "status" });
    const ver = (resp && resp.contentScriptVersion) || "unknown";
    if (ver !== EXPECTED_CONTENT_VERSION) {
      console.log(`[TTL BG] stale content.js in tab ${tabId} (v${ver}) — reinjecting ${EXPECTED_CONTENT_VERSION}`);
      needsInject = true;
    }
  } catch (e) {
    needsInject = true;
  }
  if (!needsInject) return true;
  try {
    await chrome.scripting.executeScript({
      target: { tabId },
      files: ["content.js"],
    });
    await new Promise((r) => setTimeout(r, 300));
    return true;
  } catch (injectError) {
    console.error("[TTL BG] Failed to inject:", injectError);
    return false;
  }
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  // Content script asking us to retry the queue soon
  if (msg && msg.action === "queue_retry_soon") {
    // Fire an immediate drain, alarm keeps periodic retries going
    drainQueue().catch((e) => console.error("[TTL BG] drain err:", e));
    sendResponse({ ok: true });
    return true;
  }

  if (msg.target === "content") {
    (async () => {
      try {
        const tabId = msg.tabId;
        if (!tabId) {
          sendResponse({ error: "No tab ID provided" });
          return;
        }

        const injected = await ensureContentScript(tabId);
        if (!injected) {
          sendResponse({ error: "Could not inject content script. Try refreshing the page." });
          return;
        }

        const response = await chrome.tabs.sendMessage(tabId, msg);
        if (msg.action === "start") {
          chrome.storage.local.set({
            isRecording: true,
            showId: msg.showId,
            tabId,
            dashboardUrl: msg.dashboardUrl,
            apiKey: msg.apiKey,
            captureMode: msg.captureMode || "auction",
          });
        } else if (msg.action === "stop") {
          chrome.storage.local.set({ isRecording: false, showId: null, tabId: null });
        }
        sendResponse(response);
      } catch (e) {
        sendResponse({ error: e.message });
      }
    })();
    return true;
  }
});

// ─── Local Queue Retry Drainer ────────────────────────────────────

/**
 * Scan chrome.storage.local for pending captures (cap_*, uploaded=false)
 * and try to upload each one. Successes are marked uploaded; failures
 * stay queued for the next drain. Nothing is ever deleted by this function.
 */
async function drainQueue() {
  const all = await chrome.storage.local.get(null);
  const pending = [];
  for (const [key, val] of Object.entries(all)) {
    if (key.startsWith("cap_") && val && val.uploaded === false) {
      pending.push([key, val]);
    }
  }
  if (pending.length === 0) {
    // Nothing to retry, but the store may still hold uploaded entries
    // from previous sessions. Sweep them out.
    await purgeUploadedCaptures();
    return;
  }

  console.log(`[TTL BG] Draining ${pending.length} pending capture(s)`);
  // Oldest first, and cap how many we hit per run
  pending.sort((a, b) => (a[1].timestamp || "").localeCompare(b[1].timestamp || ""));
  const batch = pending.slice(0, MAX_ATTEMPTS_PER_RUN);

  for (const [id, cap] of batch) {
    try {
      const headers = { "Content-Type": "application/json" };
      if (cap.apiKey) headers["X-API-Key"] = cap.apiKey;
      const resp = await fetch(`${cap.dashboardUrl}/api/extension-capture`, {
        method: "POST",
        headers,
        body: JSON.stringify({
          show_id: cap.showId,
          item_title: cap.itemTitle,
          image_base64: cap.imageBase64 || "",
          timestamp: cap.timestamp,
        }),
      });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const result = await resp.json();
      if (!result || result.success === false) {
        throw new Error(result && result.error ? result.error : "success=false");
      }
      cap.uploaded = true;
      cap.uploadedAt = new Date().toISOString();
      cap.uploadError = null;
      const obj = {}; obj[id] = cap;
      await chrome.storage.local.set(obj);
      console.log(`[TTL BG] Retried upload OK: ${cap.itemTitle}`);
    } catch (e) {
      cap.uploadError = String(e.message || e).slice(0, 500);
      cap.lastAttemptAt = new Date().toISOString();
      cap.attempts = (cap.attempts || 0) + 1;
      const obj = {}; obj[id] = cap;
      await chrome.storage.local.set(obj);
      // No console.error to avoid log spam when server is offline
    }
  }

  // After every drain pass, sweep any entries the server has confirmed
  // (uploaded=true) so chrome.storage.local doesn't accumulate stale
  // 3-5 MB PNG payloads across long sessions.
  await purgeUploadedCaptures();
}

/**
 * Remove every cap_* entry from chrome.storage.local whose `uploaded`
 * flag is true. The server already has those rows, so the local copy
 * is no longer needed. Pending (uploaded=false) entries stay so the
 * drainer can keep retrying them.
 */
async function purgeUploadedCaptures() {
  try {
    const all = await chrome.storage.local.get(null);
    const toRemove = [];
    for (const [key, val] of Object.entries(all)) {
      if (key.startsWith("cap_") && val && val.uploaded === true) {
        toRemove.push(key);
      }
    }
    if (toRemove.length) {
      await chrome.storage.local.remove(toRemove);
      console.log(`[TTL BG] Purged ${toRemove.length} uploaded captures`);
    }
    const { ttl_error_log } = await chrome.storage.local.get("ttl_error_log");
    if (Array.isArray(ttl_error_log) && ttl_error_log.length > 100) {
      await chrome.storage.local.set({ ttl_error_log: ttl_error_log.slice(-100) });
    }
  } catch (e) {
    console.warn("[TTL BG] purge failed:", e);
  }
}

// ─── Background-driven chat scraper ─────────────────────────────
//
// Content script injection has been unreliable across reloads. This
// scraper runs INSIDE background.js on an alarm — chrome.scripting
// executes the DOM walk inside each TikTok tab and the result comes
// back here, so the sold POST fires from background. We no longer
// depend on whichever content.js version is cached in the tab.

const SOLD_SCRAPER_ALARM = "ttl_sold_scraper";
const SOLD_SCRAPER_PERIOD = 0.1; // minutes = ~6s
const _bgSeenSoldSigs = new Set();

async function _bgScrapeOnePage(tabId) {
  const result = await chrome.scripting.executeScript({
    target: { tabId },
    world: "MAIN",
    func: () => {
      const out = { pinCard: null, chat: [] };

      // PRIMARY: the auction-pin-card itself. When an item sells, the
      // card flips to "Bids: $X.XX  <buyer> won!  Last round ended".
      // This is the authoritative source — it carries the item's full
      // title (with "#N" prefix) and the exact clearing price.
      const card = document.querySelector(".auction-pin-card");
      if (card) {
        const titleEl = card.querySelector("[class*='text-body-m-medium']");
        const title = titleEl ? (titleEl.textContent || "").trim() : "";
        const cardText = (card.innerText || "").replace(/\s+/g, " ").trim();
        const after = cardText.indexOf(title) === 0 ? cardText.slice(title.length).trim() : cardText;
        const wonMatch = after.match(/\bwon!?\b/i);
        if (title && wonMatch) {
          const priceMatch = after.match(/\$([\d.,]+)/);
          const price = priceMatch ? parseFloat(priceMatch[1].replace(/,/g, "")) : null;
          const sliceStart = priceMatch ? priceMatch.index + priceMatch[0].length : 0;
          const buyer = after.slice(sliceStart, wonMatch.index).trim() || "";
          if (price != null) out.pinCard = { item_title: title, buyer, sold_price: price };
        }
      }

      // FALLBACK: chat "<buyer> won auction item <title>|$<price>" lines.
      // Chat titles lack the "#N" prefix; server normalizes both forms.
      const SOLD_RE = /^(.+?)\s+won auction item\s+(.+?)\s*\|\s*\$([\d.,]+)\s*$/;
      const candidates = [
        ...document.querySelectorAll("span.text-neutral-text1"),
        ...document.querySelectorAll(".leading-none.h-fit.flex-1"),
      ];
      const seen = new Set();
      for (const el of candidates) {
        const t = (el.innerText || "").trim();
        if (!t || t.indexOf("won auction item") === -1) continue;
        if (seen.has(t)) continue;
        seen.add(t);
        const m = t.match(SOLD_RE);
        if (!m) continue;
        const price = parseFloat(m[3].replace(/,/g, ""));
        if (!isFinite(price)) continue;
        out.chat.push({ buyer: m[1].trim(), item_title: m[2].trim(), sold_price: price });
      }
      return out;
    },
  });
  return (result && result[0] && result[0].result) || { pinCard: null, chat: [] };
}

async function _bgPostSold(dashboardUrl, apiKey, showId, entry, source) {
  const headers = { "Content-Type": "application/json" };
  if (apiKey) headers["X-API-Key"] = apiKey;
  const resp = await fetch(`${dashboardUrl}/api/extension-sold`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      show_id: showId,
      item_title: entry.item_title,
      buyer: entry.buyer,
      sold_price: entry.sold_price,
    }),
  });
  console.log(`[TTL BG] sold (${source}) ${entry.item_title} • ${entry.buyer} • $${entry.sold_price} → HTTP ${resp.status}`);
  return resp;
}

async function _bgRunSoldScraper() {
  const { isRecording, showId, dashboardUrl, apiKey } =
    await chrome.storage.local.get(["isRecording", "showId", "dashboardUrl", "apiKey"]);
  if (!isRecording || !showId || !dashboardUrl) return;
  const tabs = await chrome.tabs.query({ url: "https://shop.tiktok.com/*" });
  if (!tabs.length) return;

  for (const t of tabs) {
    if (!t.id) continue;
    let scraped;
    try { scraped = await _bgScrapeOnePage(t.id); }
    catch (e) { continue; }

    // PRIMARY: pin card "won!" state. Fires once per item per sale.
    if (scraped.pinCard) {
      const p = scraped.pinCard;
      const sig = `${showId}|${p.item_title}|${p.buyer}|${p.sold_price}`;
      if (!_bgSeenSoldSigs.has(sig)) {
        _bgSeenSoldSigs.add(sig);
        try {
          const resp = await _bgPostSold(dashboardUrl, apiKey, showId, p, "pin");
          if (!resp.ok && resp.status === 404) _bgSeenSoldSigs.delete(sig);
        } catch (err) { _bgSeenSoldSigs.delete(sig); }
      }
    }

    // FALLBACK: chat lines. Catches anything the pin card missed (e.g.
    // the extension woke up after the card had already advanced).
    for (const e of scraped.chat) {
      const sig = `${showId}|${e.item_title}|${e.buyer}|${e.sold_price}`;
      if (_bgSeenSoldSigs.has(sig)) continue;
      _bgSeenSoldSigs.add(sig);
      try {
        const resp = await _bgPostSold(dashboardUrl, apiKey, showId, e, "chat");
        if (!resp.ok && resp.status === 404) _bgSeenSoldSigs.delete(sig);
      } catch (err) { _bgSeenSoldSigs.delete(sig); }
    }
  }
}

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === RETRY_ALARM_NAME) {
    drainQueue().catch((e) => console.error("[TTL BG] drain err:", e));
  } else if (alarm.name === SOLD_SCRAPER_ALARM) {
    _bgRunSoldScraper().catch((e) => console.error("[TTL BG] scraper err:", e));
  }
});

function _ensureAlarms() {
  chrome.alarms.create(RETRY_ALARM_NAME, { periodInMinutes: RETRY_PERIOD_MINUTES });
  chrome.alarms.create(SOLD_SCRAPER_ALARM, { periodInMinutes: SOLD_SCRAPER_PERIOD });
}

chrome.runtime.onInstalled.addListener(() => {
  _ensureAlarms();
  chrome.tabs.query({ url: "https://shop.tiktok.com/*" }, (tabs) => {
    for (const t of tabs || []) {
      if (!t.id) continue;
      chrome.scripting.executeScript({
        target: { tabId: t.id },
        files: ["content.js"],
      }).catch((e) => console.warn("[TTL BG] re-inject failed on tab", t.id, e));
    }
  });
});
chrome.runtime.onStartup.addListener(_ensureAlarms);
chrome.alarms.get(RETRY_ALARM_NAME, (existing) => { if (!existing) _ensureAlarms(); });
