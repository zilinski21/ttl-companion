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
   * Capture the video frame as a base64 PNG using canvas.
   */
  function captureVideoFrame() {
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

    if (!mainVideo) return null;

    try {
      const canvas = document.createElement("canvas");
      canvas.width = mainVideo.videoWidth || mainVideo.clientWidth;
      canvas.height = mainVideo.videoHeight || mainVideo.clientHeight;
      const ctx = canvas.getContext("2d");
      ctx.drawImage(mainVideo, 0, 0, canvas.width, canvas.height);
      // Return base64 without the data:image/png;base64, prefix
      const dataUrl = canvas.toDataURL("image/png");
      return dataUrl.split(",")[1];
    } catch (e) {
      console.error("[TTL] Video frame capture failed:", e);
      return null;
    }
  }

  /**
   * Send captured data to the dashboard API.
   * Auto-resolves show_id to the latest show if not set.
   */
  async function sendCapture(itemTitle, imageBase64) {
    try {
      // ALWAYS resolve to the latest show from dashboard
      let showId = currentShowId;
      try {
        const headers = API_KEY ? { "X-API-Key": API_KEY } : {};
        const showsResp = await fetch(`${DASHBOARD_URL}/api/shows`, { headers });
        const shows = await showsResp.json();
        if (shows && shows.length > 0) {
          showId = shows[0].id; // Most recent show
          if (showId !== currentShowId) {
            console.log(`[TTL] Show updated: ${currentShowId} -> ${showId}`);
            currentShowId = showId;
          }
        }
      } catch (_) {}

      const captureHeaders = { "Content-Type": "application/json" };
      if (API_KEY) captureHeaders["X-API-Key"] = API_KEY;
      const resp = await fetch(`${DASHBOARD_URL}/api/extension-capture`, {
        method: "POST",
        headers: captureHeaders,
        body: JSON.stringify({
          show_id: showId,
          item_title: itemTitle,
          image_base64: imageBase64 || "",
          timestamp: new Date().toISOString(),
        }),
      });
      const result = await resp.json();
      if (result.success) {
        console.log(`[TTL] Captured: ${itemTitle}`);
      } else {
        console.error("[TTL] Capture failed:", result.error);
      }
      return result;
    } catch (e) {
      console.error("[TTL] Failed to send capture:", e);
      return null;
    }
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

      // Capture the new item immediately
      console.log(`[TTL] Capturing new item: "${currentItemTitle}"`);
      const imageBase64 = captureVideoFrame();
      sendCapture(currentItemTitle, imageBase64);
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
