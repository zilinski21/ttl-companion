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
 * Ensure the content script is injected into the tab.
 */
async function ensureContentScript(tabId) {
  try {
    await chrome.tabs.sendMessage(tabId, { action: "status" });
    return true;
  } catch (e) {
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
}

chrome.runtime.onMessage.addListener((msg, sender, sendResponse) => {
  // Content script asking us to retry the queue soon
  if (msg && msg.action === "queue_retry_soon") {
    // Fire an immediate drain, alarm keeps periodic retries going
    drainQueue().catch((e) => console.error("[TTL BG] drain err:", e));
    sendResponse({ ok: true });
    return true;
  }

  // Content script asking us to screenshot the visible tab. Used as a
  // fallback when canvas drawImage() on the <video> returns a black frame
  // (happens with protected/GPU-layer video — the canvas sees nothing
  // even though the pixels are clearly on screen).
  if (msg && msg.action === "capture_visible_tab") {
    const tabId = sender && sender.tab && sender.tab.id;
    const windowId = sender && sender.tab && sender.tab.windowId;
    if (!windowId) {
      sendResponse({ error: "no windowId" });
      return true;
    }
    chrome.tabs.captureVisibleTab(windowId, { format: "png" }, (dataUrl) => {
      if (chrome.runtime.lastError || !dataUrl) {
        sendResponse({ error: (chrome.runtime.lastError && chrome.runtime.lastError.message) || "no dataUrl" });
        return;
      }
      sendResponse({ dataUrl });
    });
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
  if (pending.length === 0) return;

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
}

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === RETRY_ALARM_NAME) {
    drainQueue().catch((e) => console.error("[TTL BG] drain err:", e));
  }
});

// Set up the recurring alarm (idempotent — create() replaces any existing one)
chrome.runtime.onInstalled.addListener(() => {
  chrome.alarms.create(RETRY_ALARM_NAME, { periodInMinutes: RETRY_PERIOD_MINUTES });
});
chrome.runtime.onStartup.addListener(() => {
  chrome.alarms.create(RETRY_ALARM_NAME, { periodInMinutes: RETRY_PERIOD_MINUTES });
});
// Also ensure the alarm exists whenever the worker wakes up
chrome.alarms.get(RETRY_ALARM_NAME, (existing) => {
  if (!existing) {
    chrome.alarms.create(RETRY_ALARM_NAME, { periodInMinutes: RETRY_PERIOD_MINUTES });
  }
});
