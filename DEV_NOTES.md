# TTL Companion — Dev Notes

Living reference for whoever (or whatever) picks this up next.

## Rollback

The commit that was live on `auctionops.onrender.com` right before the
Set Price mode rollout is tagged:

```
live-pre-setprice-2026-10-12  →  efdd03e
```

Fastest rollback: Render dashboard → Deploys → Rollback to the deploy
built from `efdd03e`.

Git rollback (new commit that undoes the change):

```
git revert 16e74e3 63f00be --no-edit && git push origin main
```

Hard rollback (rewrites history, destructive):

```
git reset --hard live-pre-setprice-2026-10-12 && git push --force origin main
```

## Capture modes

Picked in the extension popup, honored per-session.

### Auction (unchanged, original behavior)
`poll()` fires `captureFreshFrame()` the moment the pin-card title
changes. Every item that appears gets a screenshot in the dashboard
regardless of whether it sells. Chat scraper fills in buyer + sold
price when a `won auction item … | $X` line appears.

### Set price (new, v1.4.1.3+)
On item appearance → do nothing. On pin-card `<buyer> won!` →
`captureFreshFrame()` and upload with the sold event. Items that never
sell never hit the dashboard. The video is settled on the right item by
the time the sold confirmation appears, so pictures match the item.

## Sold-event scraper lives in `background.js`

Content scripts stay cached in open tabs across extension reloads.
Chrome only re-reads them on tab navigation, which caused multiple
sessions where "reload" didn't actually propagate new code. Sold
detection + POST was moved into the service worker, which Chrome *does*
re-read on every reload. See `_bgRunSoldScraper` in `background.js`.

Scraper priority:
1. `.auction-pin-card` — carries the full `#N` title and clearing price
   (`Bids: $X.XX  <buyer> won!`). This is the authoritative source.
2. Chat `<buyer> won auction item <title>|$<price>` lines — fallback
   when the pin card has already advanced past the sold state.

Alarm period is `SOLD_SCRAPER_PERIOD = 0.1` min (~6s). `_bgSeenSoldSigs`
dedupes within the session; 404s leave the sig unmarked so a retry
happens next tick (handles the race where sold fires before capture).

## Content-script version check

`background.js` has `EXPECTED_CONTENT_VERSION`. On every popup → background
message, `ensureContentScript` sends a `status` ping; if the response
version doesn't match, it force re-injects `content.js`. Also, on
`chrome.runtime.onInstalled` the background re-injects into every open
`https://shop.tiktok.com/*` tab. Bump both `manifest.json` version AND
`CONTENT_SCRIPT_VERSION` AND `EXPECTED_CONTENT_VERSION` together when
touching `content.js`.

## `/api/items` must read from the DB, not CSV

The CSV at `<captures>/<show>_<date>/log.csv` only ever has what the
extension initially captured. Sold prices and buyers land in the DB
via `/api/extension-sold` and never make it into the CSV. The endpoint
is forced to the DB path (see `dashboard.py` where it comments
"ALWAYS prefer the DB"). If you ever revert that, local-dev dashboards
will silently show blank sold prices even though the data is in the DB.

## Server runs under launchd

Not just `python3 dashboard.py` in a shell. The service is
`com.ttl.dashboard`. Restart with:

```
launchctl kickstart -k gui/$(id -u)/com.ttl.dashboard
```

Logs go to `/Users/davidzilinski/ttl-data/captures/.dashboard.stderr.log`
(and `.stdout.log`). HTTP access log is in stderr too — grep for
`POST /api/extension` to see what the extension is sending.

## Local database

Live DB file: `/Users/davidzilinski/ttl-data/captures/dashboard.db`
(NOT `/Users/davidzilinski/ttl-companion/ttl_dashboard.db` — that file
exists but is empty / stale).

Quick peek at an item: `/api/debug/items-peek?show_id=<id>`.
Phonehome log: `/api/debug/extension-errors?limit=50`.
Full health: `/api/debug/health`.

## Extension install location trap

Chrome registers the extension's folder path on first Load Unpacked.
Reload only re-reads from that path. If the path is a stale copy
(e.g. `~/Downloads/extension/` from an old zip), bumping versions in
`~/ttl-companion/extension/` does nothing. Verify with:

```
python3 -c "
import json, os
p = os.path.expanduser('~/Library/Application Support/Google/Chrome/Default/Secure Preferences')
with open(p) as f: d = json.load(f)
e = d['extensions']['settings'].get('hncedhpgmjgfeollkjfjkienfdjmjpei', {})
print('loaded from:', e.get('path'))
"
```

Expected path: `/Users/davidzilinski/ttl-companion/extension`

## requirements.txt vs requirements-render.txt

`requirements.txt` has heavy dev-only extras (`torch`, `open_clip_torch`,
`numpy`) for local AI image matching. Render uses `requirements-render.txt`
which omits them to fit the starter-plan memory limit. Keep them split.

## Known operational quirks

- Items that race (sold event arrives before capture) → sold POST
  returns 404 the first time, then `_bgSeenSoldSigs` releases the sig
  and the next ~6s tick retries. Normal, no action needed.
- `_setPriceSeenSold` is per-session in content.js. If the user
  restarts recording mid-stream, previously-detected sold items in the
  current chat view will be re-detected and will fire again (server
  update is idempotent, no harm).
- "Mode" shown in the recording popup header is live from the content
  script's `status` response — if it says `AUCTION` when you expected
  `SET PRICE`, chrome.storage.sync has the old mode cached from an
  earlier session; pick again in the dropdown + Start Recording.
