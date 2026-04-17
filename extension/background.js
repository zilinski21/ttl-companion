/**
 * TTL Companion - Background Service Worker
 * Relays messages between popup and the active tab's content script.
 */

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
