/**
 * TTL Companion - Popup UI Logic
 * Configurable dashboard URL + API key for remote use.
 */

const statusEl = document.getElementById("status");
const showSelect = document.getElementById("show-select");
const btnRecord = document.getElementById("btn-record");
const btnStop = document.getElementById("btn-stop");
const controlsIdle = document.getElementById("controls-idle");
const controlsRecording = document.getElementById("controls-recording");
const settingsSection = document.getElementById("settings-section");
const currentItemEl = document.getElementById("current-item");
const currentTimerEl = document.getElementById("current-timer");
const serverUrlInput = document.getElementById("server-url");
const apiKeyInput = document.getElementById("api-key");
const btnSaveSettings = document.getElementById("btn-save-settings");
const settingsToggle = document.getElementById("settings-toggle");
const dashboardLink = document.getElementById("dashboard-link");

let activeTabId = null;
let dashboardUrl = "";
let apiKey = "";

// ─── Get Active Tab ──────────────────────────────────────────

async function getActiveTab() {
  const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
  return tab;
}

// ─── Settings ────────────────────────────────────────────────

btnSaveSettings.addEventListener("click", async () => {
  const url = serverUrlInput.value.trim().replace(/\/+$/, ""); // remove trailing slash
  const key = apiKeyInput.value.trim();

  if (!url) {
    statusEl.className = "status error";
    statusEl.textContent = "Enter a dashboard URL";
    return;
  }

  // Test connection
  statusEl.className = "status setup";
  statusEl.textContent = "Connecting...";

  try {
    const headers = key ? { "X-API-Key": key } : {};
    const resp = await fetch(`${url}/api/shows`, { headers });
    const shows = await resp.json();

    // Save settings
    dashboardUrl = url;
    apiKey = key;
    await chrome.storage.sync.set({ dashboardUrl: url, apiKey: key });

    // Also tell content script and background about the new URL
    chrome.storage.local.set({ dashboardUrl: url, apiKey: key });

    statusEl.className = "status idle";
    statusEl.textContent = "Connected";
    dashboardLink.href = url;

    settingsSection.classList.add("hidden");
    loadShows();
  } catch (e) {
    statusEl.className = "status error";
    statusEl.textContent = "Cannot connect to " + url;
  }
});

settingsToggle.addEventListener("click", () => {
  settingsSection.classList.toggle("hidden");
  if (!settingsSection.classList.contains("hidden")) {
    serverUrlInput.value = dashboardUrl;
    apiKeyInput.value = apiKey;
  }
});

// ─── Load Shows ──────────────────────────────────────────────

async function loadShows() {
  try {
    const headers = apiKey ? { "X-API-Key": apiKey } : {};
    const resp = await fetch(`${dashboardUrl}/api/shows`, { headers });
    const shows = await resp.json();
    showSelect.innerHTML = "";

    if (!shows || shows.length === 0) {
      showSelect.innerHTML = '<option value="">No shows - create one in dashboard</option>';
      return;
    }

    shows.forEach((show) => {
      const opt = document.createElement("option");
      opt.value = show.id;
      opt.textContent = `${show.name} (${show.date})`;
      showSelect.appendChild(opt);
    });

    controlsIdle.classList.remove("hidden");
    btnRecord.disabled = false;
  } catch (e) {
    showSelect.innerHTML = '<option value="">Cannot load shows</option>';
    statusEl.className = "status error";
    statusEl.textContent = "Cannot connect to dashboard";
  }
}

// ─── Send Message to Content Script ──────────────────────────

function sendToContent(action, extra = {}) {
  return new Promise((resolve) => {
    chrome.runtime.sendMessage(
      { target: "content", action, tabId: activeTabId, ...extra },
      (response) => {
        if (chrome.runtime.lastError) {
          resolve({ error: chrome.runtime.lastError.message });
        } else {
          resolve(response || { error: "No response" });
        }
      }
    );
  });
}

// ─── Start Recording ─────────────────────────────────────────

btnRecord.addEventListener("click", async () => {
  const showId = parseInt(showSelect.value);
  if (!showId) return;

  const tab = await getActiveTab();
  if (!tab || !tab.url || !tab.url.includes("shop.tiktok.com")) {
    statusEl.className = "status error";
    statusEl.textContent = "Open this on your TikTok Shop streamer tab";
    return;
  }
  activeTabId = tab.id;

  const response = await sendToContent("start", { showId, dashboardUrl, apiKey });

  if (response.error) {
    statusEl.className = "status error";
    statusEl.textContent = response.error;
    return;
  }

  // Notify dashboard
  const headers = { "Content-Type": "application/json" };
  if (apiKey) headers["X-API-Key"] = apiKey;
  fetch(`${dashboardUrl}/api/extension-start`, {
    method: "POST",
    headers,
    body: JSON.stringify({ show_id: showId }),
  }).catch(() => {});

  showRecordingUI();
});

// ─── Stop Recording ──────────────────────────────────────────

btnStop.addEventListener("click", async () => {
  await sendToContent("stop");

  const headers = { "Content-Type": "application/json" };
  if (apiKey) headers["X-API-Key"] = apiKey;
  fetch(`${dashboardUrl}/api/extension-stop`, {
    method: "POST",
    headers,
  }).catch(() => {});

  showIdleUI();
});

// ─── UI State ────────────────────────────────────────────────

function showRecordingUI() {
  settingsSection.classList.add("hidden");
  controlsIdle.classList.add("hidden");
  controlsRecording.classList.remove("hidden");
  statusEl.className = "status recording";
  statusEl.textContent = "Recording...";
  updateRecordingInfo();
}

function showIdleUI() {
  controlsIdle.classList.remove("hidden");
  controlsRecording.classList.add("hidden");
  statusEl.className = "status idle";
  statusEl.textContent = "Not recording";
  currentItemEl.textContent = "--";
  currentTimerEl.textContent = "--";
  loadShows();
}

async function updateRecordingInfo() {
  const response = await sendToContent("status");
  if (response && response.isRecording) {
    currentItemEl.textContent = response.currentItemTitle || "--";
    currentTimerEl.textContent = response.lastTimerValue || "--";
    setTimeout(updateRecordingInfo, 500);
  }
}

// ─── Init ────────────────────────────────────────────────────

(async () => {
  const tab = await getActiveTab();
  activeTabId = tab?.id;

  // Load saved settings
  const saved = await chrome.storage.sync.get(["dashboardUrl", "apiKey"]);
  dashboardUrl = saved.dashboardUrl || "http://localhost:8081";
  apiKey = saved.apiKey || "";
  serverUrlInput.value = dashboardUrl;
  apiKeyInput.value = apiKey;
  dashboardLink.href = dashboardUrl;

  // Check if already recording
  chrome.storage.local.get(["isRecording", "showId", "tabId"], (data) => {
    if (data.isRecording) {
      activeTabId = data.tabId || activeTabId;
      showRecordingUI();
    } else if (dashboardUrl && dashboardUrl !== "http://localhost:8081" && !apiKey) {
      // Has URL but no key - show settings
      statusEl.className = "status setup";
      statusEl.textContent = "Enter API key to connect";
    } else {
      // Try loading shows
      settingsSection.classList.add("hidden");
      loadShows();
    }
  });
})();
