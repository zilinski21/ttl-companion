/**
 * TTL Companion - Content Script
 * Runs on shop.tiktok.com/streamer/live/* pages.
 * Monitors the video overlay for auction items and timers.
 * When a timer starts on a new item → captures video frame + sends to dashboard.
 */

(function () {
  "use strict";

  // ─── State ───────────────────────────────────────────────────
  let isRecording = false;
  let currentShowId = null;
  let currentItemTitle = null;
  let lastTimerValue = null;
  let itemCaptured = false;      // has current item been captured already?
  let pollInterval = null;
  let scheduledReloadInterval = null;
  const POLL_MS = 200;           // check every 200ms (same as WN companion)
  const SCHEDULED_RELOAD_MS = 5 * 60 * 1000; // refresh tab every 5 min to dodge stream freezes
  // Local-backup purge tuning. Captures contain a full-resolution PNG
  // base64 (~3-5 MB each) — letting them pile up makes chrome.storage.local
  // huge and the extension sluggish. After every PURGE_EVERY successful
  // uploads we sweep the store and drop anything already confirmed
  // uploaded server-side, plus trim the error log.
  let _uploadsSincePurge = 0;
  const PURGE_EVERY = 10;
  let DASHBOARD_URL = "http://localhost:8081";
  let API_KEY = "";
  // "auction" (default): capture + log the moment a new item appears on screen.
  // "set_price":         buffer the frame on item appearance, only log once the
  //                      chat confirms "won auction item" for that title. Avoids
  //                      logging unsold set-price items that just time out.
  let CAPTURE_MODE = "auction";
  // title -> { base64, timestamp } captured when the item first appeared.
  // Only used in set_price mode; drained when a matching sold event fires.
  const _pendingByTitle = new Map();
  const PENDING_MAX_AGE_MS = 60 * 60 * 1000; // 1h — items that never sell expire.

  // ─── DOM Helpers ─────────────────────────────────────────────

  /**
   * Find the active auction item title from the .auction-pin-card widget.
   * This widget appears at the bottom of the video during an active auction.
   */
  function findItemTitle() {
    const card = document.querySelector(".auction-pin-card");
    if (!card) return null;

    // The item name is in a .sc-dtBdUo or any leaf div with the item title text
    // Try the most specific selector first
    const titleEl =
      card.querySelector(".sc-dtBdUo") ||
      card.querySelector("[class*='text-body-m-medium']") ||
      card.querySelector("[class*='truncate']");
    if (titleEl) {
      const text = (titleEl.textContent || "").trim();
      if (text && text.length > 2 && text.length < 300) return text;
    }

    // Fallback: walk leaf nodes inside the card looking for "#N ItemName" pattern
    const allEls = card.querySelectorAll("*");
    for (const el of allEls) {
      if (el.children.length > 0) continue;
      const text = (el.textContent || "").trim();
      if (!text) continue;
      // Skip known non-item texts
      if (
        text.startsWith("Bids:") ||
        text.includes("won!") ||
        text.includes("Last round") ||
        text.match(/^\d+s$/) ||
        text.match(/^\d+ bids?$/)
      )
        continue;
      if (text.length > 3 && text.length < 200) {
        return text;
      }
    }
    return null;
  }

  /**
   * Find the auction timer value.
   * TikTok uses seconds format ("10s", "5s") or mm:ss.
   */
  function findTimer() {
    // Strategy 1: Video overlay timer
    const videoContainer =
      document.querySelector(".styles-module__video--zb3DZ") ||
      document.querySelector("video")?.closest("[class*='video']");

    if (videoContainer) {
      const allEls = videoContainer.querySelectorAll("*");
      for (const el of allEls) {
        if (el.children.length > 0) continue;
        const text = (el.textContent || "").trim();
        if (/^\d+s$/.test(text) || /^\d{1,2}$/.test(text) || /^\d+:\d{2}$/.test(text)) {
          return text;
        }
      }
    }

    // Strategy 2: Timer/countdown elements anywhere on page
    const timerSelectors = [
      "[class*='countdown']",
      "[class*='timer']",
      "[class*='Timer']",
      "[class*='Countdown']",
      "[data-testid*='timer']",
      "[data-testid*='countdown']",
    ];
    for (const sel of timerSelectors) {
      try {
        const els = document.querySelectorAll(sel);
        for (const el of els) {
          const text = (el.textContent || "").trim();
          if (/\d+s?$/.test(text) || /\d+:\d{2}/.test(text)) {
            return text;
          }
        }
      } catch (_) {}
    }

    // Strategy 3: Search for countdown-like text near auction items
    const spans = document.querySelectorAll("span, div");
    for (const el of spans) {
      if (el.children.length > 0) continue;
      const text = (el.textContent || "").trim();
      if (/^\d+s$/.test(text) && parseInt(text) <= 60) {
        // Make sure it's not the static "10s" duration label
        const parent = el.parentElement;
        const siblings = parent ? parent.textContent.trim() : "";
        if (siblings.includes("Starting price") || siblings.includes("Quantity")) {
          continue;
        }
        return text;
      }
      if (/^\d+:\d{2}$/.test(text)) {
        return text;
      }
    }

    return null;
  }

  /**
   * Find the main live video element (largest visible <video>).
   */
  function findMainVideo() {
    const videos = document.querySelectorAll("video");
    let mainVideo = null;
    let largestArea = 0;

    for (const v of videos) {
      const rect = v.getBoundingClientRect();
      const style = window.getComputedStyle(v);
      if (
        rect.width > 200 &&
        rect.height > 200 &&
        style.display !== "none" &&
        style.visibility !== "hidden" &&
        style.opacity !== "0"
      ) {
        const area = rect.width * rect.height;
        if (area > largestArea) {
          largestArea = area;
          mainVideo = v;
        }
      }
    }
    return mainVideo;
  }

  /**
   * Draw the current video frame to a canvas and return
   * { base64, hash, videoTime } where hash is a short perceptual
   * fingerprint computed from a 16x16 downscale. Two different
   * live frames will (essentially always) produce different hashes;
   * a frozen/buffered stream yields the same hash on repeat capture.
   */
  function captureFrameSnapshot() {
    const mainVideo = findMainVideo();
    if (!mainVideo) return null;
    try {
      const w = mainVideo.videoWidth || mainVideo.clientWidth;
      const h = mainVideo.videoHeight || mainVideo.clientHeight;
      if (!w || !h) return null;

      // Full-res canvas for the PNG we upload
      const canvas = document.createElement("canvas");
      canvas.width = w;
      canvas.height = h;
      const ctx = canvas.getContext("2d");
      ctx.drawImage(mainVideo, 0, 0, w, h);
      const dataUrl = canvas.toDataURL("image/png");
      const base64 = dataUrl.split(",")[1];

      // Tiny downscaled canvas for a cheap fingerprint
      const HS = 16;
      const small = document.createElement("canvas");
      small.width = HS;
      small.height = HS;
      const sctx = small.getContext("2d");
      sctx.drawImage(mainVideo, 0, 0, HS, HS);
      const pixels = sctx.getImageData(0, 0, HS, HS).data;
      // Fold pixels into a compact hex string (skip alpha to reduce noise).
      // Simultaneously accumulate luminance so we can reject black frames
      // (TikTok occasionally returns an all-black video frame during scene
      // changes — we never want to save those as the item screenshot).
      let hash = "";
      let lumaSum = 0;
      let lumaCount = 0;
      for (let i = 0; i < pixels.length; i += 4) {
        hash += ((pixels[i] >> 4) & 0xf).toString(16);
        hash += ((pixels[i + 1] >> 4) & 0xf).toString(16);
        hash += ((pixels[i + 2] >> 4) & 0xf).toString(16);
        lumaSum += 0.2126 * pixels[i] + 0.7152 * pixels[i + 1] + 0.0722 * pixels[i + 2];
        lumaCount += 1;
      }
      const avgLuma = lumaCount ? lumaSum / lumaCount : 0;

      return {
        base64,
        hash,
        avgLuma,
        videoTime: mainVideo.currentTime || 0,
        readyState: mainVideo.readyState || 0,
        paused: !!mainVideo.paused,
      };
    } catch (e) {
      console.error("[TTL] captureFrameSnapshot failed:", e);
      return null;
    }
  }

  // Last good capture — used to detect duplicate frames caused by
  // a frozen/buffered video stream.
  let lastCaptureHash = null;
  let lastCaptureVideoTime = null;
  // Count of consecutive items whose capture was still stale after
  // in-item retries + nudges. Escalates to a full tab reload.
  let consecutiveStaleItems = 0;
  // Guard so we don't reload more than once every 60s (protects against
  // flapping if something else is wrong).
  let lastReloadAttemptAt = 0;
  const MIN_RELOAD_INTERVAL_MS = 60 * 1000;
  const STALE_ITEM_RELOAD_THRESHOLD = 3; // reload after N stale items in a row

  /**
   * Nudge the video element to force it past a buffer / freeze:
   *   - seek forward fractionally
   *   - pause + play
   * Returns a promise that resolves when the nudge has (hopefully) taken.
   */
  async function nudgeVideo() {
    const v = findMainVideo();
    if (!v) return;
    try {
      const t = v.currentTime;
      // Tiny seek can unstick HLS/DASH buffer gaps on live streams.
      try { v.currentTime = t + 0.1; } catch (_) {}
      if (v.paused) {
        try { await v.play(); } catch (_) {}
      } else {
        try { v.pause(); } catch (_) {}
        await new Promise((r) => setTimeout(r, 60));
        try { await v.play(); } catch (_) {}
      }
    } catch (e) {
      console.warn("[TTL] nudgeVideo failed:", e);
    }
  }

  /**
   * Last-resort: reload the whole tab. The content script auto-resumes
   * recording on re-injection because isRecording/showId/dashboardUrl/
   * apiKey live in chrome.storage.local, so the stream continues.
   *
   * Rate-limited to once per MIN_RELOAD_INTERVAL_MS to avoid reload storms.
   */
  function reloadTabIfFeedFrozen(reason) {
    const now = Date.now();
    if (now - lastReloadAttemptAt < MIN_RELOAD_INTERVAL_MS) return false;
    lastReloadAttemptAt = now;
    _logError("tab_reload", reason);
    console.warn(`[TTL] FEED FROZEN — reloading tab: ${reason}`);
    // Give the error log a moment to persist before navigating.
    setTimeout(() => {
      try { window.location.reload(); } catch (_) {}
    }, 120);
    return true;
  }

  /**
   * Capture a *fresh* frame for a new item. If the first snapshot
   * is identical to the previous item's snapshot (indicating a
   * freeze / buffering / stale frame), retry up to `maxTries` times
   * with increasing delays until we get a visually distinct frame
   * or the video's currentTime advances. If retries fail, nudge the
   * video element. If several consecutive items come back stale,
   * reload the tab as a final escalation.
   *
   * Always returns a base64 even on give-up, so the capture entry
   * still lands in the local store.
   */
  async function captureFreshFrame(maxTries = 5, baseDelayMs = 250) {
    let snap = captureFrameSnapshot();
    if (!snap) return null;

    // First item in session — nothing to compare against, accept it.
    if (lastCaptureHash === null) {
      lastCaptureHash = snap.hash;
      lastCaptureVideoTime = snap.videoTime;
      consecutiveStaleItems = 0;
      return snap.base64;
    }

    // A frame is "bad" if it looks identical to the last item (stale)
    // or is mostly black (scene transition / loading). Both retry the
    // same way.
    const BLACK_LUMA_THRESHOLD = 12; // 0-255 scale; <12 is near-black
    const looksStale = (s) =>
      (s.hash === lastCaptureHash &&
        (s.videoTime === lastCaptureVideoTime || s.paused || s.readyState < 2)) ||
      s.avgLuma < BLACK_LUMA_THRESHOLD;

    let nudged = false;
    for (let i = 0; i < maxTries; i++) {
      if (!looksStale(snap)) {
        lastCaptureHash = snap.hash;
        lastCaptureVideoTime = snap.videoTime;
        consecutiveStaleItems = 0;
        return snap.base64;
      }
      // Halfway through retries, try to actively unfreeze the video.
      if (!nudged && i >= Math.floor(maxTries / 2)) {
        console.warn("[TTL] Frame still stale — nudging video element.");
        await nudgeVideo();
        nudged = true;
      }
      const delay = baseDelayMs + i * 150;
      console.warn(
        `[TTL] Possibly stale frame (hash=${snap.hash.slice(0, 8)}…, videoTime=${snap.videoTime.toFixed(2)}, paused=${snap.paused}, readyState=${snap.readyState}). Retrying in ${delay}ms (${i + 1}/${maxTries}).`
      );
      await new Promise((r) => setTimeout(r, delay));
      const next = captureFrameSnapshot();
      if (next) snap = next;
    }

    // Still stale — keep the frame for this item, bump the counter,
    // and if we've seen enough consecutive stale items reload the tab.
    consecutiveStaleItems += 1;
    console.warn(
      `[TTL] Returning stale frame after ${maxTries} retries — video may be frozen (consecutiveStaleItems=${consecutiveStaleItems}).`
    );
    _logError(
      "stale_frame",
      `hash=${snap.hash.slice(0, 12)} videoTime=${snap.videoTime} paused=${snap.paused} consecutive=${consecutiveStaleItems}`
    );

    if (consecutiveStaleItems >= STALE_ITEM_RELOAD_THRESHOLD) {
      reloadTabIfFeedFrozen(
        `${consecutiveStaleItems} consecutive stale-frame items`
      );
    }

    lastCaptureHash = snap.hash;
    lastCaptureVideoTime = snap.videoTime;
    return snap.base64;
  }

  /**
   * Back-compat shim: synchronous wrapper around captureFrameSnapshot
   * for anything that still wants a simple "current frame now".
   */
  function captureVideoFrame() {
    const snap = captureFrameSnapshot();
    return snap ? snap.base64 : null;
  }

  // ─── Local-First Persistence ─────────────────────────────────
  //
  // Every capture is written to chrome.storage.local (key `cap_<ts>_<rand>`)
  // BEFORE attempting to upload. If upload fails, the entry stays with
  // `uploaded: false` and a retry alarm (in background.js) drains it later.
  // This guarantees no capture is ever lost, even if Render is down or the
  // employee's network drops. Failures also land in `ttl_error_log` for
  // forensic visibility.

  function _genCaptureId() {
    return `cap_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  }

  async function _logError(context, details, extra) {
    // 1) Always persist locally — never loses data even if the server is down.
    try {
      const { ttl_error_log } = await chrome.storage.local.get("ttl_error_log");
      const log = Array.isArray(ttl_error_log) ? ttl_error_log : [];
      log.push({ ts: new Date().toISOString(), context, details: String(details).slice(0, 500) });
      // Keep only the last 500 errors
      const trimmed = log.slice(-500);
      await chrome.storage.local.set({ ttl_error_log: trimmed });
    } catch (_) { /* never throw from logger */ }

    // 2) Best-effort phone-home to /api/extension-error so server-side
    // debugging during a live show doesn't require browser devtools. This
    // fetch never throws and never blocks the caller.
    try {
      if (!DASHBOARD_URL) return;
      const headers = { "Content-Type": "application/json" };
      if (API_KEY) headers["X-API-Key"] = API_KEY;
      // Fire and forget — keepalive lets the request survive page nav/reload.
      fetch(`${DASHBOARD_URL}/api/extension-error`, {
        method: "POST",
        headers,
        keepalive: true,
        body: JSON.stringify({
          context,
          details: String(details).slice(0, 2000),
          timestamp: new Date().toISOString(),
          dashboard_url: DASHBOARD_URL,
          show_id: (extra && extra.show_id) || currentShowId || null,
          item_title: (extra && extra.item_title) || "",
          user_agent: navigator.userAgent,
        }),
      }).catch(() => {});
    } catch (_) { /* never throw from logger */ }
  }

  async function _saveLocal(capture) {
    // capture is the full record; the key is capture.id
    const obj = {};
    obj[capture.id] = capture;
    await chrome.storage.local.set(obj);
  }

  async function _markUploaded(id) {
    const existing = (await chrome.storage.local.get(id))[id];
    if (!existing) return;
    existing.uploaded = true;
    existing.uploadedAt = new Date().toISOString();
    existing.uploadError = null;
    const obj = {}; obj[id] = existing;
    await chrome.storage.local.set(obj);
  }

  async function _markUploadError(id, err) {
    const existing = (await chrome.storage.local.get(id))[id];
    if (!existing) return;
    existing.uploaded = false;
    existing.uploadError = String(err).slice(0, 500);
    existing.lastAttemptAt = new Date().toISOString();
    existing.attempts = (existing.attempts || 0) + 1;
    const obj = {}; obj[id] = existing;
    await chrome.storage.local.set(obj);
  }

  /**
   * Sweep chrome.storage.local for any cap_* entries whose `uploaded`
   * flag is true (server confirmed receipt) and remove them. Also trims
   * the error log to its most recent 100 entries. Pending captures
   * (uploaded=false) are left alone so the background drainer can keep
   * retrying them.
   */
  async function _purgeUploadedLocalData() {
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
        console.log(`[TTL] Purged ${toRemove.length} uploaded captures from local storage`);
      }
      const { ttl_error_log } = await chrome.storage.local.get("ttl_error_log");
      if (Array.isArray(ttl_error_log) && ttl_error_log.length > 100) {
        await chrome.storage.local.set({ ttl_error_log: ttl_error_log.slice(-100) });
      }
    } catch (e) {
      console.warn("[TTL] purge failed:", e);
    }
  }
  // Expose for background.js so it can purge after a retry-drain too.
  window.__TTL_purgeUploadedLocalData = _purgeUploadedLocalData;

  /**
   * Attempt to POST a single stored capture to the dashboard.
   * Returns true on success, false on failure.
   */
  async function uploadCapture(capture) {
    const captureHeaders = { "Content-Type": "application/json" };
    if (capture.apiKey) captureHeaders["X-API-Key"] = capture.apiKey;
    const resp = await fetch(`${capture.dashboardUrl}/api/extension-capture`, {
      method: "POST",
      headers: captureHeaders,
      body: JSON.stringify({
        show_id: capture.showId,
        item_title: capture.itemTitle,
        image_base64: capture.imageBase64 || "",
        timestamp: capture.timestamp,
      }),
    });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    const result = await resp.json();
    if (!result || result.success === false) {
      throw new Error(result && result.error ? result.error : "server returned success=false");
    }
    return true;
  }

  /**
   * Primary capture entry point.
   *   1) Resolve the latest show_id (best-effort).
   *   2) Persist capture to chrome.storage.local FIRST (local is source of truth).
   *   3) Try uploading; on failure, mark the entry as pending and log the error.
   *
   * Nothing about step 2 depends on the network, so captures are never lost.
   */
  async function sendCapture(itemTitle, imageBase64) {
    // Resolve the latest show (best-effort, non-blocking for local save).
    let showId = currentShowId;
    try {
      const headers = API_KEY ? { "X-API-Key": API_KEY } : {};
      const showsResp = await fetch(`${DASHBOARD_URL}/api/shows`, { headers });
      if (showsResp.ok) {
        const shows = await showsResp.json();
        if (shows && shows.length > 0) {
          showId = shows[0].id;
          if (showId !== currentShowId) {
            console.log(`[TTL] Show updated: ${currentShowId} -> ${showId}`);
            currentShowId = showId;
          }
        }
      }
    } catch (_) { /* fall back to cached currentShowId */ }

    const capture = {
      id: _genCaptureId(),
      showId,
      itemTitle,
      imageBase64: imageBase64 || "",
      timestamp: new Date().toISOString(),
      dashboardUrl: DASHBOARD_URL,
      apiKey: API_KEY || "",
      uploaded: false,
      uploadError: null,
      attempts: 0,
    };

    // STEP 1 — save locally first. Must not be skipped for any reason.
    try {
      await _saveLocal(capture);
      console.log(`[TTL] Saved locally: ${itemTitle} (${capture.id})`);
    } catch (e) {
      // Last-resort: if even local storage fails, log to the error log itself
      // (which also lives in chrome.storage.local — if that's broken the
      // browser profile has bigger problems).
      console.error("[TTL] CRITICAL: could not save locally:", e);
      await _logError("local_save_failed", `${itemTitle}: ${e}`);
      return null;
    }

    // STEP 2 — try uploading. Failure is non-fatal because data is already local.
    try {
      await uploadCapture(capture);
      await _markUploaded(capture.id);
      console.log(`[TTL] Uploaded: ${itemTitle}`);
      // Periodic local-storage cleanup. Counter bumps on every successful
      // upload; once it crosses PURGE_EVERY we sweep out everything the
      // server has already confirmed so chrome.storage.local stays small.
      _uploadsSincePurge += 1;
      if (_uploadsSincePurge >= PURGE_EVERY) {
        _uploadsSincePurge = 0;
        _purgeUploadedLocalData();
      }
      return { success: true };
    } catch (e) {
      console.warn(`[TTL] Upload failed for ${itemTitle}, will retry:`, e.message);
      await _markUploadError(capture.id, e.message);
      await _logError("upload_failed", `${itemTitle}: ${e.message}`);
      // Ask background service worker to schedule a retry drain soon
      try { chrome.runtime.sendMessage({ action: "queue_retry_soon" }); } catch (_) {}
      return { success: false, error: e.message, savedLocally: true };
    }
  }

  // Expose for background.js retry drainer
  window.__TTL_uploadCapture = uploadCapture;

  // ─── Sold-chat scraper ───────────────────────────────────────
  //
  // The TikTok Live dashboard's chat panel contains a "Top customers"
  // section with lines like:
  //   <buyer> won auction item  <item_name>|$<price>
  // We scan leaf text nodes for that pattern on a timer, dedupe on
  // (item_title + buyer + price), and POST each new win to
  // /api/extension-sold so the dashboard fills in buyer + sold_price.
  //
  // Chat item titles lack the "#" prefix that the dashboard uses;
  // the server normalizes both variants.

  /**
   * Drop buffered pending-frame entries older than PENDING_MAX_AGE_MS so
   * items that never sell don't leak memory across a long session.
   */
  function _prunePendingBuffer() {
    const cutoff = Date.now() - PENDING_MAX_AGE_MS;
    for (const [title, entry] of _pendingByTitle) {
      if (entry.ts < cutoff) _pendingByTitle.delete(title);
    }
  }

  /**
   * The sold-chat row carries the item name WITHOUT the "#N " prefix that
   * the on-screen auction card uses. Try exact match first, then look for
   * a buffered title that ends with the sold title (prefix-stripped match).
   * Returns {title, base64} and removes the entry from the buffer.
   */
  function _popBufferedForSoldTitle(soldTitle) {
    if (_pendingByTitle.has(soldTitle)) {
      const v = _pendingByTitle.get(soldTitle);
      _pendingByTitle.delete(soldTitle);
      return { title: soldTitle, base64: v.base64 };
    }
    const needle = soldTitle.toLowerCase();
    for (const [bufTitle, v] of _pendingByTitle) {
      const low = bufTitle.toLowerCase();
      if (low === needle || low.endsWith(" " + needle) || low.endsWith(needle)) {
        _pendingByTitle.delete(bufTitle);
        return { title: bufTitle, base64: v.base64 };
      }
    }
    return null;
  }

  // ─── Pin-card sold watcher (set-price mode) ──────────────────
  //
  // For set-price items, the chat does NOT emit a "won auction item" line.
  // Instead the on-screen auction-pin-card itself transitions from
  //   "Bids: $X.XX  No bids  0 bids"           (active, no bids yet)
  // through
  //   "Bids: $X.XX  <buyer> won!  Last round ended"   (SOLD)
  // or to
  //   "Bids: $X.XX  No bids       Last round ended"   (unsold — skip)
  //
  // When set-price mode is on, we detect the "won!" transition per-item,
  // flush the buffered screenshot, and POST a synthetic sold event with
  // the buyer + price parsed from the card. Items that fall through to
  // the unsold terminal state silently drop their buffered frame — nothing
  // ever reaches the dashboard for them, which is the whole point.

  // Set of "title|buyer|price" sigs already handled so we don't double-fire
  // when the sold state lingers on screen for several polling ticks.
  const _seenPinSoldSigs = new Set();
  let _pinSoldInterval = null;

  /**
   * Parse the current .auction-pin-card into a structured state.
   * Returns null if the card is absent, else:
   *   { title, price: Number|null, buyer: string|null,
   *     status: "active" | "sold" | "unsold" | "unknown" }
   */
  function _parsePinCard() {
    const card = document.querySelector(".auction-pin-card");
    if (!card) return null;

    // Title: first .text-body-m-medium descendant we find.
    const titleEl = card.querySelector("[class*='text-body-m-medium']");
    const title = titleEl ? (titleEl.textContent || "").trim() : "";

    // Parse everything from the full card text to survive TikTok shuffling
    // class names between states. We subtract the title from the front so
    // its chars can't trip up the price/buyer regex.
    const cardText = (card.innerText || "").replace(/\s+/g, " ").trim();
    const afterTitle = title && cardText.indexOf(title) === 0
      ? cardText.slice(title.length).trim()
      : cardText;

    const priceMatch = afterTitle.match(/\$([\d.,]+)/);
    const price = priceMatch ? parseFloat(priceMatch[1].replace(/,/g, "")) : null;
    const wonMatch = afterTitle.match(/\bwon!?\b/i);

    let status = "unknown";
    let buyer = null;
    if (wonMatch) {
      status = "sold";
      // Buyer = text between the "$X.XX" and the "won!" marker.
      // e.g. "Bids: $50.00 T won!" → "T"
      const sliceStart = priceMatch ? priceMatch.index + priceMatch[0].length : 0;
      const section = afterTitle.slice(sliceStart, wonMatch.index).trim();
      if (section) buyer = section;
    } else if (/\bNo bids\b/i.test(afterTitle) && /Last round ended/i.test(afterTitle)) {
      status = "unsold";
    } else {
      status = "active";
    }

    return { title, price, buyer, status, _rowText: afterTitle };
  }

  /**
   * Polling loop for the pin-card sold state. Only active in set_price mode.
   * Fires sendCapture + /api/extension-sold for the first "sold" tick of
   * each unique (title|buyer|price) signature.
   */
  // Track the last parsed state per title so we only log on transitions
  // (otherwise 500ms polling floods the console).
  const _pinLastStatusByTitle = new Map();

  async function _flushPinSold() {
    if (!isRecording) return;
    if (CAPTURE_MODE !== "set_price") return;
    const parsed = _parsePinCard();
    if (!parsed || !parsed.title) return;

    // Log every state transition for the active title so we can diagnose
    // missed sales. "active -> sold" means the detector worked;
    // "active -> active" forever means we never saw the won! transition.
    const prev = _pinLastStatusByTitle.get(parsed.title);
    if (prev !== parsed.status) {
      _pinLastStatusByTitle.set(parsed.title, parsed.status);
      console.log(
        `[TTL] (set_price) pin-card: "${parsed.title}" ${prev || "-"} -> ${parsed.status}` +
        (parsed.buyer ? ` buyer=${parsed.buyer}` : "") +
        (parsed.price != null ? ` price=$${parsed.price}` : "") +
        ` | rowText="${parsed._rowText}"`
      );
    }

    if (parsed.status !== "sold") return;

    const sig = `${parsed.title}|${parsed.buyer || ""}|${parsed.price ?? ""}`;
    if (_seenPinSoldSigs.has(sig)) return;
    _seenPinSoldSigs.add(sig);

    // Flush the screenshot buffered when this item first appeared. If none
    // exists OR the buffered frame is empty (eg the video wasn't ready at
    // buffer time), fall back to a fresh captureFreshFrame which retries
    // past stale/black frames. Either way we want a non-empty image so the
    // dashboard row isn't blank.
    const buffered = _popBufferedForSoldTitle(parsed.title);
    let frame = buffered?.base64 || "";
    const titleForCapture = buffered?.title || parsed.title;
    if (!frame) {
      console.log(`[TTL] (set_price) No buffered frame for "${parsed.title}" — capturing now.`);
      try { frame = (await captureFreshFrame()) || ""; } catch (_) {}
    }
    try {
      const r = await sendCapture(titleForCapture, frame);
      console.log(`[TTL] (set_price) sendCapture result:`, r);
    } catch (e) {
      console.warn(`[TTL] (set_price) sendCapture threw:`, e);
    }

    // Now the sold event. The server's /api/extension-sold matches on title
    // + show_id, so the capture above must land first.
    try {
      const headers = { "Content-Type": "application/json" };
      if (API_KEY) headers["X-API-Key"] = API_KEY;
      const resp = await fetch(`${DASHBOARD_URL}/api/extension-sold`, {
        method: "POST",
        headers,
        keepalive: true,
        body: JSON.stringify({
          show_id: currentShowId,
          item_title: parsed.title,
          buyer: parsed.buyer || "",
          sold_price: parsed.price,
        }),
      });
      const body = await resp.text().catch(() => "");
      console.log(
        `[TTL] (set_price) Sold via pin card: ${parsed.title} • ${parsed.buyer} • $${parsed.price} ` +
        `→ HTTP ${resp.status} ${body.slice(0, 200)}`
      );
      if (!resp.ok) _seenPinSoldSigs.delete(sig); // retry on next tick
    } catch (err) {
      console.warn(`[TTL] (set_price) sold POST failed:`, err);
      _seenPinSoldSigs.delete(sig);
    }
  }

  function _startPinSoldPoller() {
    if (_pinSoldInterval) return;
    _pinSoldInterval = setInterval(_flushPinSold, 500);
    _flushPinSold();
  }

  function _stopPinSoldPoller() {
    if (_pinSoldInterval) {
      clearInterval(_pinSoldInterval);
      _pinSoldInterval = null;
    }
    _seenPinSoldSigs.clear();
    _pinLastStatusByTitle.clear();
  }

  const SOLD_RE = /^(.+?)\s+won auction item\s+(.+?)\s*\|\s*\$([\d.,]+)\s*$/;
  const _seenSoldSigs = new Set();
  let _soldInterval = null;

  function _parseSoldEntries() {
    // The "won auction item" row is ONE span, but its text is split across
    // multiple text nodes (buyer name is its own DOM subtree because TikTok
    // wraps display names in separate spans for rendering). A raw text-node
    // walk only finds "won auction item" in isolation. Instead iterate
    // element containers whose aggregated innerText contains the marker.
    const out = [];
    const seen = new Set();
    // Fast paths: the observed row class on TikTok's auction chat. Fall
    // back to querying any leaf-ish element if those change.
    const candidates = [
      ...document.querySelectorAll("span.text-neutral-text1"),
      ...document.querySelectorAll(".leading-none.h-fit.flex-1"),
    ];
    // Final fallback: scan all spans, filter by content. Cheap on this page.
    if (candidates.length === 0) {
      candidates.push(...document.querySelectorAll("span, div"));
    }
    for (const el of candidates) {
      const t = (el.innerText || "").trim();
      if (!t || t.indexOf("won auction item") === -1) continue;
      if (seen.has(t)) continue;
      seen.add(t);
      const m = t.match(SOLD_RE);
      if (!m) continue;
      const priceNum = parseFloat(m[3].replace(/,/g, ""));
      if (!isFinite(priceNum)) continue;
      out.push({
        buyer: m[1].trim(),
        item_title: m[2].trim(),
        sold_price: priceNum,
      });
    }
    return out;
  }

  async function _flushSoldEntries() {
    if (!isRecording) return;
    if (!DASHBOARD_URL) return;
    // In set-price mode the pin-card poller is authoritative. The chat
    // "won auction item" line should never fire for set-price items, but
    // some streamers mix auction + set-price in the same session and we
    // don't want a mode-mismatched chat line to double-fire a sold event.
    if (CAPTURE_MODE === "set_price") return;
    const entries = _parseSoldEntries();
    if (!entries.length) return;

    for (const e of entries) {
      const sig = `${e.item_title}|${e.buyer}|${e.sold_price}`;
      if (_seenSoldSigs.has(sig)) continue;
      _seenSoldSigs.add(sig);
      try {
        const headers = { "Content-Type": "application/json" };
        if (API_KEY) headers["X-API-Key"] = API_KEY;
        const resp = await fetch(`${DASHBOARD_URL}/api/extension-sold`, {
          method: "POST",
          headers,
          keepalive: true,
          body: JSON.stringify({
            show_id: currentShowId,
            item_title: e.item_title,
            buyer: e.buyer,
            sold_price: e.sold_price,
          }),
        });
        if (resp.ok) {
          console.log(
            `[TTL] Sold: ${e.item_title} • ${e.buyer} • $${e.sold_price}`
          );
        } else if (resp.status === 404) {
          // Item not yet captured, or /api/extension-sold not deployed yet
          // on this server. Unmark so we can retry on a later poll.
          _seenSoldSigs.delete(sig);
        }
      } catch (err) {
        // Network failure — unmark so we retry on next poll.
        _seenSoldSigs.delete(sig);
      }
    }
  }

  function _startSoldPoller() {
    if (_soldInterval) return;
    _soldInterval = setInterval(_flushSoldEntries, 3000);
    // Run one pass immediately too.
    _flushSoldEntries();
  }

  function _stopSoldPoller() {
    if (_soldInterval) {
      clearInterval(_soldInterval);
      _soldInterval = null;
    }
    _seenSoldSigs.clear();
  }

  // ─── Main Polling Loop ───────────────────────────────────────

  function poll() {
    if (!isRecording) return;

    const newItemTitle = findItemTitle();
    const currentTimer = findTimer();

    // Item changed - capture immediately and reset state
    if (newItemTitle && newItemTitle !== currentItemTitle) {
      console.log(`[TTL] Item changed: "${currentItemTitle}" -> "${newItemTitle}"`);
      currentItemTitle = newItemTitle;
      itemCaptured = false;
      lastTimerValue = null;

      // Capture the new item. captureFreshFrame retries if the video
      // looks frozen / we got the same frame as the previous item, so
      // duplicate screenshots across consecutive items are suppressed.
      const titleAtCapture = currentItemTitle; // snapshot in case item changes mid-await
      if (CAPTURE_MODE === "set_price") {
        // Buffer frame; defer sendCapture until the sold-chat scraper
        // confirms this title actually sold.
        console.log(`[TTL] (set_price) Buffering frame for: "${titleAtCapture}"`);
        captureFreshFrame().then((imageBase64) => {
          _pendingByTitle.set(titleAtCapture, {
            base64: imageBase64 || "",
            ts: Date.now(),
          });
          _prunePendingBuffer();
        });
      } else {
        console.log(`[TTL] Capturing new item: "${titleAtCapture}"`);
        captureFreshFrame().then((imageBase64) => {
          sendCapture(titleAtCapture, imageBase64);
        });
      }
      itemCaptured = true;
    }

    // Update last timer (still tracked for debugging)
    if (currentTimer) {
      lastTimerValue = currentTimer;
    }
  }

  // ─── Start / Stop ────────────────────────────────────────────

  function startRecording(showId, dashboardUrl, apiKey, captureMode) {
    if (isRecording) return;
    if (dashboardUrl) DASHBOARD_URL = dashboardUrl;
    if (apiKey) API_KEY = apiKey;
    CAPTURE_MODE = captureMode === "set_price" ? "set_price" : "auction";
    _pendingByTitle.clear();
    currentShowId = showId;
    isRecording = true;
    itemCaptured = false;
    currentItemTitle = findItemTitle();
    lastTimerValue = null;
    pollInterval = setInterval(poll, POLL_MS);
    _startSoldPoller();
    if (CAPTURE_MODE === "set_price") _startPinSoldPoller();
    console.log(`[TTL] capture mode: ${CAPTURE_MODE}`);
    // Periodic preventative tab reload — TikTok's HLS stream slowly
    // drifts into a frozen/black state over long sessions; refreshing
    // the tab every 5 minutes keeps the video pipeline fresh. Recording
    // state lives in chrome.storage.local so the session survives the
    // reload (background.js re-injects content.js on the new page).
    if (scheduledReloadInterval) clearInterval(scheduledReloadInterval);
    scheduledReloadInterval = setInterval(() => {
      console.log("[TTL] 5-minute scheduled tab reload (anti-freeze).");
      _logError("scheduled_reload", "5min preventative refresh");
      setTimeout(() => { try { window.location.reload(); } catch (_) {} }, 120);
    }, SCHEDULED_RELOAD_MS);
    console.log(`[TTL] Recording started for show ${showId}`);
  }

  function stopRecording() {
    isRecording = false;
    currentShowId = null;
    if (pollInterval) {
      clearInterval(pollInterval);
      pollInterval = null;
    }
    if (scheduledReloadInterval) {
      clearInterval(scheduledReloadInterval);
      scheduledReloadInterval = null;
    }
    _stopSoldPoller();
    _stopPinSoldPoller();
    console.log("[TTL] Recording stopped");
  }

  // ─── Message Handling (from popup / background) ──────────────

  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    switch (msg.action) {
      case "start":
        startRecording(msg.showId, msg.dashboardUrl, msg.apiKey, msg.captureMode);
        sendResponse({ success: true, status: "recording" });
        break;
      case "stop":
        stopRecording();
        sendResponse({ success: true, status: "stopped" });
        break;
      case "status":
        sendResponse({
          isRecording,
          currentShowId,
          currentItemTitle,
          lastTimerValue,
          itemCaptured,
          captureMode: CAPTURE_MODE,
          pendingBufferedCount: _pendingByTitle.size,
        });
        break;
      default:
        sendResponse({ error: "Unknown action" });
    }
    return true; // keep channel open for async
  });

  // ─── Init ────────────────────────────────────────────────────
  console.log("[TTL] TTL Companion content script loaded on", window.location.href);

  // Check if we should auto-resume recording
  chrome.storage.local.get(["isRecording", "showId", "dashboardUrl", "apiKey", "captureMode"], (data) => {
    if (data.dashboardUrl) DASHBOARD_URL = data.dashboardUrl;
    if (data.apiKey) API_KEY = data.apiKey;
    if (data.isRecording && data.showId) {
      console.log("[TTL] Auto-resuming recording for show", data.showId);
      startRecording(data.showId, data.dashboardUrl, data.apiKey, data.captureMode);
    }
  });
})();
