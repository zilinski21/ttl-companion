#!/usr/bin/env python3
"""
TTL Monitor: TikTok Live Stream Monitor
Monitors a TikTok Shop live stream via CDP (Chrome DevTools Protocol).
Captures video frames when auction items go live (timer starts).
Connects to existing Chrome - does NOT launch a new browser.

Requires Chrome started with: --remote-debugging-port=9222
"""

import asyncio
import csv
import os
import re
import sys
import time
from pathlib import Path
from datetime import datetime
from typing import Optional

try:
    from playwright.async_api import async_playwright, Page
except ImportError:
    print("Error: Missing playwright library.")
    print("Install with: pip install playwright")
    print("Then run: playwright install chromium")
    sys.exit(1)


def get_captures_dir() -> Path:
    env_value = os.environ.get("TT_CAPTURES_DIR")
    if env_value:
        return Path(os.path.expanduser(env_value)).resolve()
    return (Path.home() / "Downloads" / "TT recorder live" / "captures").resolve()


class TikTokMonitor:
    """Monitor TikTok Shop live stream for auction items via CDP."""

    def __init__(
        self,
        cdp_url: str = "http://localhost:9222",
        debug: bool = False,
        show_id: int = None,
        show_name: str = None,
        show_date: str = None,
    ):
        self.cdp_url = cdp_url
        self.debug = debug
        self.show_id = show_id
        self.show_name = show_name
        self.show_date = show_date

        # Create show-specific folder structure
        self.captures_dir = get_captures_dir()
        self.captures_dir.mkdir(parents=True, exist_ok=True)

        if show_id and show_name and show_date:
            safe_name = re.sub(r'[<>:"/\\|?*]', "", show_name).strip()
            safe_date = re.sub(r'[<>:"/\\|?*]', "", show_date).strip()
            self.show_dir = self.captures_dir / f"{safe_name}_{safe_date}"
            self.show_dir.mkdir(exist_ok=True)
            self.log_file = self.show_dir / "log.csv"
            self.screenshot_dir = self.show_dir
        else:
            self.show_dir = self.captures_dir
            self.screenshot_dir = self.captures_dir
            if show_id:
                self.log_file = self.captures_dir / f"log_{show_id}.csv"
            else:
                self.log_file = self.captures_dir / "log.csv"

        self.page: Optional[Page] = None
        self.current_item_title: Optional[str] = None
        self.last_timer_value: Optional[str] = None
        self.current_item_logged: bool = False
        self.item_screenshot_taken: bool = False
        self.capture_counter: int = 0

        # Initialize CSV file with headers if it doesn't exist
        if not self.log_file.exists():
            with open(self.log_file, "w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(
                    [
                        "timestamp",
                        "item_title",
                        "pinned_text",
                        "filename",
                        "sold_price",
                        "sold_timestamp",
                        "viewers",
                    ]
                )

    def sanitize_filename(self, text: str) -> str:
        """Sanitize text for use in filename."""
        sanitized = re.sub(r'[<>:"/\\|?*]', "", text)
        sanitized = sanitized.replace(" ", "_")
        if len(sanitized) > 100:
            sanitized = sanitized[:100]
        return sanitized

    async def find_tiktok_tab(self, browser) -> Optional[Page]:
        """Find the TikTok Shop streamer live event tab."""
        for context in browser.contexts:
            for page in context.pages:
                url = page.url
                if "shop.tiktok.com/streamer/live/event/dashboard" in url and "session_id" in url:
                    return page
        # Fallback: any TikTok streamer live page
        for context in browser.contexts:
            for page in context.pages:
                if "shop.tiktok.com/streamer/live" in page.url:
                    return page
        return None

    async def find_item_title(self) -> Optional[str]:
        """
        Find the auction item title on the video overlay.
        During a live auction, the item name appears as an overlay on the video feed.
        Also checks the left panel auction list as a fallback.
        """
        if not self.page:
            return None

        try:
            # Strategy 1: Look for item title in the video overlay area
            # The auction overlay appears on top of the video during live
            video_container = await self.page.query_selector('.styles-module__video--zb3DZ')
            if video_container:
                # Check for overlay text elements that contain auction item info
                overlay_text = await video_container.evaluate(
                    """
                    (container) => {
                        // Look for text overlays on the video that look like auction items
                        // Pattern: "#N ItemName" or product name text
                        const allEls = container.querySelectorAll('*');
                        for (const el of allEls) {
                            if (el.children.length > 0) continue;
                            const text = (el.textContent || '').trim();
                            // Match auction item pattern: "#1 Something" or item with price
                            if (text && text.length > 3 && text.length < 200) {
                                // Skip known non-item texts
                                if (text.includes('Script') || text.includes('Add script') ||
                                    text.includes('go LIVE') || text.includes('LIVE Manager') ||
                                    text.includes('on-screen')) continue;
                                // Check for auction item patterns
                                if (/^#\\d+/.test(text) || text.includes('$')) {
                                    return text;
                                }
                            }
                        }
                        return null;
                    }
                """
                )
                if overlay_text:
                    return overlay_text.strip()

            # Strategy 2: Look for any overlay element with auction-like content
            # TikTok may use dynamic class names, so search broadly
            overlay_items = await self.page.evaluate(
                """
                () => {
                    // Search for auction overlay elements on the video
                    const videoArea = document.querySelector('.styles-module__video--zb3DZ') ||
                                     document.querySelector('video')?.parentElement?.parentElement;
                    if (!videoArea) return null;

                    // Find all positioned elements (overlays)
                    const overlays = videoArea.querySelectorAll('[style*="absolute"], .absolute, [class*="absolute"]');
                    for (const overlay of overlays) {
                        const text = (overlay.textContent || '').trim();
                        // Skip known UI elements
                        if (text.includes('Script') || text.includes('go LIVE') ||
                            text.includes('Add script')) continue;
                        // Look for auction item text
                        if (text && text.length > 3 && text.length < 300) {
                            // Extract the item name if it contains auction patterns
                            const match = text.match(/#\\d+\\s+.+/);
                            if (match) return match[0].substring(0, 200);
                        }
                    }
                    return null;
                }
            """
            )
            if overlay_items:
                return overlay_items.strip()

            # Strategy 3: Check the left panel auction list as fallback
            # The auction item name appears in the left panel with class hkmbTh or similar
            selectors = [
                ".hkmbTh",
                ".sc-dtBdUo",
                "[class*='auction'] [class*='name']",
                "[class*='auction'] [class*='title']",
            ]
            for selector in selectors:
                try:
                    element = await self.page.query_selector(selector)
                    if element:
                        text = await element.inner_text()
                        if text and text.strip():
                            return text.strip()
                except Exception:
                    continue

            # Strategy 4: Broad search for elements matching "#N ITEM_NAME" pattern
            item_text = await self.page.evaluate(
                """
                () => {
                    const walker = document.createTreeWalker(
                        document.body,
                        NodeFilter.SHOW_TEXT,
                        null,
                        false
                    );
                    let node;
                    while (node = walker.nextNode()) {
                        const text = (node.textContent || '').trim();
                        if (/^#\\d+\\s+\\w/.test(text) && text.length > 5 && text.length < 200) {
                            // Verify it's not in a chat/comment area
                            let el = node.parentElement;
                            let inChat = false;
                            for (let i = 0; i < 5 && el; i++) {
                                const cls = (el.className || '').toLowerCase();
                                if (cls.includes('chat') || cls.includes('comment')) {
                                    inChat = true;
                                    break;
                                }
                                el = el.parentElement;
                            }
                            if (!inChat) return text;
                        }
                    }
                    return null;
                }
            """
            )
            if item_text:
                return item_text.strip()

        except Exception as e:
            if self.debug:
                print(f"Error finding item title: {e}")

        return None

    async def find_timer(self) -> Optional[str]:
        """
        Find the auction timer. TikTok uses seconds format (e.g., "10s", "5s", "3", "0").
        The timer appears as an overlay on the video or in the auction panel.
        Returns timer text or None.
        """
        if not self.page:
            return None

        try:
            # Strategy 1: Look for timer in video overlay
            video_container = await self.page.query_selector('.styles-module__video--zb3DZ')
            if video_container:
                timer_text = await video_container.evaluate(
                    """
                    (container) => {
                        const allEls = container.querySelectorAll('*');
                        for (const el of allEls) {
                            if (el.children.length > 0) continue;
                            const text = (el.textContent || '').trim();
                            // Match timer patterns: "10s", "5s", "0s", "10", "5", "0:10", "0:05"
                            if (/^\\d+s$/.test(text) || /^\\d{1,2}$/.test(text) || /^\\d+:\\d{2}$/.test(text)) {
                                return text;
                            }
                        }
                        return null;
                    }
                """
                )
                if timer_text:
                    return timer_text.strip()

            # Strategy 2: Look for timer in the auction panel (left side)
            timer_text = await self.page.evaluate(
                """
                () => {
                    // Look for elements containing timer-like text near auction items
                    const allEls = document.querySelectorAll('span, div');
                    for (const el of allEls) {
                        if (el.children.length > 0) continue;
                        const text = (el.textContent || '').trim();
                        // Match countdown patterns
                        if (/^\\d+s$/.test(text) && parseInt(text) <= 60) {
                            // Verify it's not the static "10s" duration label
                            // The static label has a clock icon sibling
                            const parent = el.parentElement;
                            const siblings = parent ? parent.textContent.trim() : '';
                            // If parent has other text like "Starting price", it's the config, not countdown
                            if (siblings.includes('Starting price') || siblings.includes('Quantity')) {
                                continue;
                            }
                            return text;
                        }
                        // Also check for mm:ss format
                        if (/^\\d+:\\d{2}$/.test(text)) {
                            return text;
                        }
                    }
                    return null;
                }
            """
            )
            if timer_text:
                return timer_text.strip()

            # Strategy 3: Check for active countdown elements
            # During live auctions, TikTok may add specific countdown classes
            countdown_selectors = [
                "[class*='countdown']",
                "[class*='timer']",
                "[class*='Timer']",
                "[class*='Countdown']",
                "[data-testid*='timer']",
                "[data-testid*='countdown']",
            ]
            for selector in countdown_selectors:
                try:
                    elements = await self.page.query_selector_all(selector)
                    for element in elements:
                        text = await element.inner_text()
                        if text and text.strip():
                            cleaned = text.strip()
                            if re.search(r'\d+s?$', cleaned) or re.search(r'\d+:\d{2}', cleaned):
                                return cleaned
                except Exception:
                    continue

        except Exception as e:
            if self.debug:
                print(f"Error finding timer: {e}")

        return None

    async def take_screenshot(self, item_title: str) -> str:
        """Take a screenshot of just the video element and return the filename."""
        if not self.page:
            raise RuntimeError("Page not available")

        date_str = datetime.now().strftime("%m-%d")
        sanitized_title = self.sanitize_filename(item_title)
        self.capture_counter += 1
        filename = f"{sanitized_title} {date_str}_{self.capture_counter:03d}.png"
        filepath = self.screenshot_dir / filename

        try:
            # Wait briefly for stability
            await asyncio.sleep(0.3)

            # Find the video element
            video = None
            videos = await self.page.query_selector_all("video")
            if videos:
                largest_area = 0
                for v in videos:
                    try:
                        box = await v.bounding_box()
                        if not box:
                            continue
                        is_visible = await v.evaluate(
                            """
                            el => {
                                const rect = el.getBoundingClientRect();
                                const style = window.getComputedStyle(el);
                                return rect.width > 0 && rect.height > 0 &&
                                       style.display !== 'none' &&
                                       style.visibility !== 'hidden' &&
                                       style.opacity !== '0' &&
                                       rect.width > 200 &&
                                       rect.height > 200;
                            }
                        """
                        )
                        if is_visible and box:
                            area = box["width"] * box["height"]
                            if area > largest_area:
                                largest_area = area
                                video = v
                    except Exception:
                        continue

            if not video:
                raise RuntimeError("Could not find video element on page")

            box = await video.bounding_box()
            if not box:
                raise RuntimeError("Video element has no bounding box")

            # Capture video frame via canvas (same approach as WN companion)
            image_data = await video.evaluate(
                """
                async (video) => {
                    const canvas = document.createElement('canvas');
                    canvas.width = video.videoWidth || video.clientWidth;
                    canvas.height = video.videoHeight || video.clientHeight;
                    const ctx = canvas.getContext('2d');
                    ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
                    return canvas.toDataURL('image/png');
                }
            """
            )

            import base64

            if image_data.startswith("data:image"):
                header, encoded = image_data.split(",", 1)
                image_bytes = base64.b64decode(encoded)
                with open(filepath, "wb") as f:
                    f.write(image_bytes)

                video_width = await video.evaluate("v => v.videoWidth || v.clientWidth")
                video_height = await video.evaluate("v => v.videoHeight || v.clientHeight")
                print(f"  ✓ Video frame captured ({video_width}x{video_height}px)")
            else:
                raise RuntimeError("Failed to get video frame data")

        except Exception as e:
            print(f"  ❌ Error capturing video frame: {e}")
            # Fallback: try any video
            try:
                videos = await self.page.query_selector_all("video")
                if videos:
                    fallback_video = videos[0]
                    image_data = await fallback_video.evaluate(
                        """
                        async (video) => {
                            const canvas = document.createElement('canvas');
                            canvas.width = video.videoWidth || video.clientWidth;
                            canvas.height = video.videoHeight || video.clientHeight;
                            const ctx = canvas.getContext('2d');
                            ctx.drawImage(video, 0, 0, canvas.width, canvas.height);
                            return canvas.toDataURL('image/png');
                        }
                    """
                    )
                    import base64
                    if image_data.startswith("data:image"):
                        header, encoded = image_data.split(",", 1)
                        image_bytes = base64.b64decode(encoded)
                        with open(filepath, "wb") as f:
                            f.write(image_bytes)
                        print(f"  ✓ Fallback: Video frame captured")
                        return filename
            except Exception as fallback_error:
                print(f"  ❌ Fallback also failed: {fallback_error}")
            raise

        return filename

    async def log_to_csv(
        self,
        item_title: str,
        pinned_text: str,
        filename: str,
        sold_price: Optional[str] = None,
        sold_timestamp: Optional[str] = None,
        viewers: Optional[str] = None,
    ):
        """Append a row to the CSV log."""
        timestamp = datetime.now().isoformat()
        with open(self.log_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    timestamp,
                    item_title,
                    pinned_text or "",
                    filename,
                    sold_price or "",
                    sold_timestamp or "",
                    viewers or "",
                ]
            )

    async def monitor(self):
        """Main monitoring loop. Connects to existing Chrome via CDP."""
        async with async_playwright() as p:
            print("🔗 Connecting to Chrome via CDP...")
            print(f"   CDP URL: {self.cdp_url}")
            print("   (Make sure Chrome is running with --remote-debugging-port=9222)")

            try:
                browser = await p.chromium.connect_over_cdp(self.cdp_url)
                print("✓ Connected to Chrome")
            except Exception as e:
                print(f"❌ Failed to connect to Chrome via CDP: {e}")
                print("\nTo fix this:")
                print("1. Close Chrome completely")
                print("2. Relaunch Chrome with:")
                print('   /Applications/Google\\ Chrome.app/Contents/MacOS/Google\\ Chrome --remote-debugging-port=9222')
                print("3. Open your TikTok Shop streamer dashboard")
                print("4. Try recording again")
                return

            # Find the TikTok Shop tab
            print("🔍 Looking for TikTok Shop streamer tab...")
            self.page = await self.find_tiktok_tab(browser)

            if not self.page:
                print("❌ Could not find TikTok Shop streamer tab")
                print("   Make sure you have shop.tiktok.com/streamer/live/event/dashboard open")
                await browser.close()
                return

            print(f"✓ Found TikTok tab: {self.page.url[:80]}...")

            # Wait for video element
            print("⏳ Waiting for video element...")
            try:
                await self.page.wait_for_selector("video", state="visible", timeout=30000)
                print("✓ Video element found")
            except Exception:
                print("⚠️  Video element not found yet - continuing anyway")

            await asyncio.sleep(2)

            # Initial detection
            print("🔍 Detecting initial state...")
            self.current_item_title = await self.find_item_title()

            if self.current_item_title:
                print(f"✓ Current item: '{self.current_item_title}'")
            else:
                print("⚠️  No item detected yet (will keep checking)")

            # Monitoring loop
            print("\n🔍 Monitoring for auction items...")
            print("Press Ctrl+C to stop\n")

            try:
                while True:
                    try:
                        # Check if tab is still valid
                        try:
                            _ = self.page.url
                        except Exception:
                            print("⚠️  Tab disconnected, trying to reconnect...")
                            self.page = await self.find_tiktok_tab(browser)
                            if not self.page:
                                print("❌ Could not reconnect to TikTok tab")
                                break
                            print("✓ Reconnected")

                        # Check current values
                        new_item_title = await self.find_item_title()
                        current_timer = await self.find_timer()
                    except Exception as check_error:
                        if self.debug:
                            print(f"⚠️  Error checking page: {check_error}")
                        await asyncio.sleep(3)
                        continue

                    if self.debug:
                        print(
                            f"[DEBUG] Item: '{new_item_title or 'N/A'}' | "
                            f"Timer: '{current_timer or 'N/A'}'"
                        )

                    # Check if item changed
                    if new_item_title and new_item_title != self.current_item_title:
                        print(f"\n🔄 ITEM CHANGED!")
                        print(f"  Old: '{self.current_item_title}'")
                        print(f"  New: '{new_item_title}'")

                        self.current_item_title = new_item_title
                        self.current_item_logged = False
                        self.last_timer_value = None
                        self.item_screenshot_taken = False
                        self.capture_counter = 0

                    # Detect timer start - this is when we capture
                    timer_just_started = False
                    if current_timer:
                        # Timer is present - check if it just appeared
                        timer_val = re.sub(r'[^0-9]', '', current_timer)
                        if timer_val:
                            timer_num = int(timer_val)
                            # Timer just started if:
                            # - We had no timer before
                            # - Or previous timer was 0
                            if not self.last_timer_value or self.last_timer_value in ["0", "0s", "0:00"]:
                                if timer_num > 0:
                                    timer_just_started = True

                    # Take screenshot when timer starts on a new item
                    if (
                        timer_just_started
                        and not self.item_screenshot_taken
                        and not self.current_item_logged
                        and self.current_item_title
                    ):
                        print(f"\n⏱️  TIMER STARTED! ({current_timer})")
                        print(f"  Item: '{self.current_item_title}'")
                        print("  Taking video frame capture...")
                        try:
                            filename = await self.take_screenshot(self.current_item_title)
                            print(f"  ✓ Saved: {filename}")
                            self.item_screenshot_taken = True
                        except Exception as screenshot_error:
                            print(f"  ❌ Video frame capture failed: {screenshot_error}")
                            filename = None

                        # Log to CSV
                        await self.log_to_csv(
                            self.current_item_title,
                            "",  # pinned_text - not used for TikTok
                            filename or "",
                            None,
                        )
                        print(f"  ✓ Logged to {self.log_file.name}")
                        self.current_item_logged = True

                    # If timer is running but we missed the start, still capture
                    if (
                        current_timer
                        and not self.current_item_logged
                        and self.current_item_title
                    ):
                        timer_val = re.sub(r'[^0-9]', '', current_timer)
                        if timer_val and int(timer_val) > 0:
                            print(f"\n⏱️  TIMER RUNNING! ({current_timer})")
                            print(f"  Item: '{self.current_item_title}'")
                            print("  Taking video frame capture...")
                            try:
                                filename = await self.take_screenshot(self.current_item_title)
                                print(f"  ✓ Saved: {filename}")
                            except Exception as screenshot_error:
                                print(f"  ❌ Video frame capture failed: {screenshot_error}")
                                filename = None

                            await self.log_to_csv(
                                self.current_item_title,
                                "",
                                filename or "",
                                None,
                            )
                            print(f"  ✓ Logged to {self.log_file.name}")
                            self.current_item_logged = True

                    # Update last timer value
                    if current_timer:
                        self.last_timer_value = current_timer

                    # Fast polling for short timers
                    await asyncio.sleep(0.2)

            except KeyboardInterrupt:
                print("\n\n✓ Monitoring stopped by user")
            except Exception as e:
                print(f"\n\n❌ Fatal error during monitoring: {e}")
                import traceback
                traceback.print_exc()
            finally:
                # Disconnect from Chrome - do NOT close the browser!
                try:
                    print("\n🔄 Disconnecting from Chrome (browser stays open)...")
                    await browser.close()
                    print("✓ Disconnected")
                except Exception as e:
                    print(f"⚠️  Error disconnecting: {e}")


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Monitor TikTok Shop live stream for auction items"
    )
    parser.add_argument(
        "--cdp-url",
        default="http://localhost:9222",
        help="Chrome DevTools Protocol URL (default: http://localhost:9222)",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Print debug info every polling cycle",
    )
    parser.add_argument(
        "--show-id",
        type=int,
        default=None,
        help="Show ID for writing to show-specific CSV file",
    )
    parser.add_argument(
        "--show-name",
        type=str,
        default=None,
        help="Show name for creating show-specific folder",
    )
    parser.add_argument(
        "--show-date",
        type=str,
        default=None,
        help="Show date for creating show-specific folder",
    )

    args = parser.parse_args()

    monitor = TikTokMonitor(
        cdp_url=args.cdp_url,
        debug=args.debug,
        show_id=args.show_id,
        show_name=args.show_name,
        show_date=args.show_date,
    )
    await monitor.monitor()


if __name__ == "__main__":
    asyncio.run(main())
