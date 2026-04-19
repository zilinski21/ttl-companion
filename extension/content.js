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
  const POLL_MS = 200;           // check every 200ms (same as WN companion)
  let DASHBOARD_URL = "http://localhost:8081";
  let API_KEY = "";

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
      // Fold pixels into a compact hex string (skip alpha to reduce noise)
      let hash = "";
      for (let i = 0; i < pixels.length; i += 4) {
        hash += ((pixels[i] >> 4) & 0xf).toString(16);
        hash += ((pixels[i + 1] >> 4) & 0xf).toString(16);
        hash += ((pixels[i + 2] >> 4) & 0xf).toString(16);
      }

      return {
        base64,
        hash,
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

    const looksStale = (s) =>
      s.hash === lastCaptureHash &&
      (s.videoTime === lastCaptureVideoTime || s.paused || s.readyState < 2);

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

  async function _logError(context, details) {
    try {
      const { ttl_error_log } = await chrome.storage.local.get("ttl_error_log");
      const log = Array.isArray(ttl_error_log) ? ttl_error_log : [];
      log.push({ ts: new Date().toISOString(), context, details: String(details).slice(0, 500) });
      // Keep only the last 500 errors
      const trimmed = log.slice(-500);
      await chrome.storage.local.set({ ttl_error_log: trimmed });
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
      console.log(`[TTL] Capturing new item: "${currentItemTitle}"`);
      const titleAtCapture = currentItemTitle; // snapshot in case item changes mid-await
      captureFreshFrame().then((imageBase64) => {
        sendCapture(titleAtCapture, imageBase64);
      });
      itemCaptured = true;
    }

    // Update last timer (still tracked for debugging)
    if (currentTimer) {
      lastTimerValue = currentTimer;
    }
  }

  // ─── Start / Stop ────────────────────────────────────────────

  function startRecording(showId, dashboardUrl, apiKey) {
    if (isRecording) return;
    if (dashboardUrl) DASHBOARD_URL = dashboardUrl;
    if (apiKey) API_KEY = apiKey;
    currentShowId = showId;
    isRecording = true;
    itemCaptured = false;
    currentItemTitle = findItemTitle();
    lastTimerValue = null;
    pollInterval = setInterval(poll, POLL_MS);
    console.log(`[TTL] Recording started for show ${showId}`);
  }

  function stopRecording() {
    isRecording = false;
    currentShowId = null;
    if (pollInterval) {
      clearInterval(pollInterval);
      pollInterval = null;
    }
    console.log("[TTL] Recording stopped");
  }

  // ─── Message Handling (from popup / background) ──────────────

  chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
    switch (msg.action) {
      case "start":
        startRecording(msg.showId, msg.dashboardUrl, msg.apiKey);
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
  chrome.storage.local.get(["isRecording", "showId", "dashboardUrl", "apiKey"], (data) => {
    if (data.dashboardUrl) DASHBOARD_URL = data.dashboardUrl;
    if (data.apiKey) API_KEY = data.apiKey;
    if (data.isRecording && data.showId) {
      console.log("[TTL] Auto-resuming recording for show", data.showId);
      startRecording(data.showId, data.dashboardUrl, data.apiKey);
    }
  });
})();
