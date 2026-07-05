"""
╔══════════════════════════════════════════════════════════════════════╗
║          anihq.cc Stream Extractor  —  v11.0                        ║
╠══════════════════════════════════════════════════════════════════════╣
║  FOLDER LAYOUT                                                       ║
║                                                                      ║
║  input/                                                              ║
║    ├── watch_urls.txt          ← Source 1: local URLs, one per line  ║
║    └── remote_url_source.json  ← Source 2: cached remote GitHub JSON ║
║                                   (auto-fetched & saved here;        ║
║                                    re-fetched if older than 1 hour)  ║
║                                                                      ║
║  output/                                                             ║
║    ├── stream_data.json            ← extracted stream records        ║
║    ├── stream_data_2.json …        ← auto-split at 700 KB            ║
║    ├── stream_data_readable.txt    ← human-readable mirror           ║
║    ├── stream_data_readable_2.txt… ← auto-split at 700 KB            ║
║    ├── processed_urls.txt          ← skip-list: successfully done    ║
║    └── failed_urls.txt             ← skip-list: all proxies failed   ║
╚══════════════════════════════════════════════════════════════════════╝

REMOTE SOURCE FORMAT  (auto-detected, any of these work):
  • {"returncode":0, "stdout":"url1\\nurl2\\n…"}  ← GitHub Actions wrapper
  • ["url1", "url2", …]                           ← plain URL array
  • [{"url":"url1"}, {"url":"url2"}, …]           ← object array
  • plain text, one URL per line                  ← fallback
"""

import re
import base64
import sys
import json
import time
import random
from pathlib import Path

# ── Dependency bootstrap ──────────────────────────────────────────────
try:
    from curl_cffi import requests as crequests
    from bs4 import BeautifulSoup
except ImportError:
    import subprocess
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", "curl_cffi", "beautifulsoup4",
         "--break-system-packages", "-q"]
    )
    from curl_cffi import requests as crequests
    from bs4 import BeautifulSoup


# ════════════════════════════════════════════════════════════════════════
#  PATHS & CONFIG
# ════════════════════════════════════════════════════════════════════════
BASE_DIR   = Path(__file__).parent
INPUT_DIR  = BASE_DIR / "input"
OUTPUT_DIR = BASE_DIR / "output"

INPUT_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

# ── Input files ──────────────────────────────────────────────────────
# Source 1 — local, hand-curated list
INPUT_LOCAL_URLS    = INPUT_DIR / "watch_urls.txt"

# Source 2 — remote GitHub JSON, cached here in input/
INPUT_REMOTE_CACHE  = INPUT_DIR / "remote_url_source.json"

# URL of the remote source
REMOTE_SOURCE_URL   = (
    "https://raw.githubusercontent.com/srtfile/anihq_cc"
    "/refs/heads/main/anihq2.json"
)

# How long before the cached remote file is considered stale (seconds)
REMOTE_CACHE_TTL    = 3600          # 1 hour

# ── Output base names (files go inside output/) ──────────────────────
OUT_STREAM_JSON  = "stream_data"           # stream_data.json, _2, _3 …
OUT_STREAM_TXT   = "stream_data_readable"  # stream_data_readable.txt, _2 …
OUT_PROCESSED    = "processed_urls"        # processed_urls.txt, _2 …
OUT_FAILED       = "failed_urls"           # failed_urls.txt, _2 …

# ── Tuning ────────────────────────────────────────────────────────────
BATCH_SIZE    = 500
REQUEST_DELAY = 1.8           # polite pause between successful grabs
MAX_FILE_SIZE = 700 * 1024    # 700 KB — auto-split threshold

# ── Proxy pool ────────────────────────────────────────────────────────
PROXIES = [
    "http://ygxmhkcc:n3batopqanpg@31.59.20.176:6754",
    "http://ygxmhkcc:n3batopqanpg@31.56.127.193:7684",
    "http://ygxmhkcc:n3batopqanpg@45.38.107.97:6014",
    "http://ygxmhkcc:n3batopqanpg@38.154.203.95:5863",
    "http://ygxmhkcc:n3batopqanpg@198.105.121.200:6462",
    "http://ygxmhkcc:n3batopqanpg@64.137.96.74:6641",
    "http://ygxmhkcc:n3batopqanpg@198.23.243.226:6361",
    "http://ygxmhkcc:n3batopqanpg@38.154.185.97:6370",
    "http://ygxmhkcc:n3batopqanpg@142.111.67.146:5611",
    "http://ygxmhkcc:n3batopqanpg@191.96.254.138:6185",
]


# ════════════════════════════════════════════════════════════════════════
#  SESSION
# ════════════════════════════════════════════════════════════════════════
_SESSION = None

def get_session() -> crequests.Session:
    global _SESSION
    if _SESSION is None:
        _SESSION = crequests.Session(impersonate="chrome120")
        _SESSION.headers.update({"Accept-Language": "en-US,en;q=0.9"})
    return _SESSION


# ════════════════════════════════════════════════════════════════════════
#  FILE HELPERS
# ════════════════════════════════════════════════════════════════════════

def append_line(filepath: Path, text: str):
    """Append one line to a file, creating it if needed."""
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(text.strip() + "\n")


def next_output_path(base_name: str, ext: str) -> Path:
    """
    Return the current active output path for *base_name* + *ext*.
    If the existing file has reached MAX_FILE_SIZE it is considered full
    and the next numbered slot is returned instead.

      base_name.ext  →  base_name_2.ext  →  base_name_3.ext …
    """
    counter = 1
    while True:
        suffix = "" if counter == 1 else f"_{counter}"
        path   = OUTPUT_DIR / f"{base_name}{suffix}{ext}"
        if not path.exists() or path.stat().st_size < MAX_FILE_SIZE:
            return path
        counter += 1


def load_all_skip_urls() -> set:
    """
    Collect every URL that has already been processed or permanently failed
    by scanning all split files of processed_urls and failed_urls.
    Returns a combined set used to skip re-processing.
    """
    combined: set = set()
    for base in (OUT_PROCESSED, OUT_FAILED):
        counter = 1
        while True:
            suffix = "" if counter == 1 else f"_{counter}"
            p = OUTPUT_DIR / f"{base}{suffix}.txt"
            if not p.exists():
                break
            for line in p.read_text(encoding="utf-8").splitlines():
                # failed_urls lines: "url  # reason" — strip the comment
                url_part = line.split("#")[0].strip()
                if url_part:
                    combined.add(url_part)
            counter += 1
    return combined


# ════════════════════════════════════════════════════════════════════════
#  INPUT SOURCE 2 — REMOTE URL CACHE  (input/remote_url_source.json)
# ════════════════════════════════════════════════════════════════════════

def _parse_urls_from_raw(raw: str) -> list:
    """
    Extract URLs from *raw* text regardless of the remote file's format:

      Format A — GitHub Actions shell-output wrapper:
          {"returncode": 0, "stdout": "url1\\nurl2\\n…", "stderr": ""}

      Format B — plain JSON array of strings:
          ["url1", "url2", …]

      Format C — JSON array of objects with a "url" key:
          [{"url": "url1"}, {"url": "url2"}, …]

      Format D — plain text, one URL per line (fallback)
    """
    urls = []
    text = raw.strip()

    if text and text[0] in ("{", "["):
        try:
            data = json.loads(text)

            # Format A ─────────────────────────────────────────────────
            if isinstance(data, dict):
                blob = data.get("stdout", "")
                if blob:
                    for line in blob.splitlines():
                        line = line.strip()
                        if line.startswith("http"):
                            urls.append(line)
                    if urls:
                        return urls
                # generic fallback: any string value that is a URL
                for v in data.values():
                    if isinstance(v, str) and v.startswith("http"):
                        urls.append(v)
                return urls

            # Format B & C ─────────────────────────────────────────────
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, str) and item.strip():
                        urls.append(item.strip())
                    elif isinstance(item, dict) and "url" in item:
                        urls.append(item["url"])
                return urls

        except json.JSONDecodeError:
            pass  # fall through to plain-text parser

    # Format D ─────────────────────────────────────────────────────────
    for line in raw.splitlines():
        line = line.strip()
        if line.startswith("http"):
            urls.append(line)

    return urls


def _is_cache_fresh() -> bool:
    """True if input/remote_url_source.json exists and is younger than REMOTE_CACHE_TTL."""
    if not INPUT_REMOTE_CACHE.exists():
        return False
    age = time.time() - INPUT_REMOTE_CACHE.stat().st_mtime
    return age < REMOTE_CACHE_TTL


def _fetch_and_save_remote() -> bool:
    """
    Download the remote source JSON and save it to input/remote_url_source.json.
    Returns True on success, False if all retries failed.
    """
    print("   🌐 Downloading remote source → saving to input/remote_url_source.json …")
    session     = get_session()
    max_retries = 5

    for attempt in range(1, max_retries + 1):
        try:
            r = session.get(
                REMOTE_SOURCE_URL,
                timeout=120,
                headers={
                    "Accept":        "application/json, text/plain, */*",
                    "Cache-Control": "no-cache",
                },
            )
            r.raise_for_status()

            raw = r.text
            if not raw or not raw.strip():
                raise ValueError("Empty response body")

            # Quick sanity check — make sure we can actually parse URLs from it
            sample = _parse_urls_from_raw(raw)
            if not sample:
                raise ValueError("Downloaded content parsed to zero URLs")

            # Save raw bytes exactly as received so no data is lost
            INPUT_REMOTE_CACHE.write_text(raw, encoding="utf-8")
            print(f"   ✅ Saved {INPUT_REMOTE_CACHE.stat().st_size / 1024:.1f} KB  "
                  f"({len(sample):,} URLs detected)")
            return True

        except Exception as exc:
            wait = 8 * attempt
            print(f"   Attempt {attempt}/{max_retries} failed: {exc}")
            if attempt < max_retries:
                print(f"   Retrying in {wait}s …")
                time.sleep(wait)

    return False


def load_remote_input_urls() -> list:
    """
    Main entry point for Source 2.

    1. If input/remote_url_source.json is fresh  → read from disk (no network)
    2. If missing or stale                       → download and save it first
    3. Parse URLs from the cached file
    4. If download failed and no cache exists    → return empty list gracefully
    """
    print("📥 Input Source 2 : remote_url_source.json")

    if _is_cache_fresh():
        age_min = (time.time() - INPUT_REMOTE_CACHE.stat().st_mtime) / 60
        print(f"   ✔ Cache is fresh ({age_min:.0f} min old) — reading from disk")
    else:
        if INPUT_REMOTE_CACHE.exists():
            age_min = (time.time() - INPUT_REMOTE_CACHE.stat().st_mtime) / 60
            print(f"   ⏰ Cache is stale ({age_min:.0f} min old) — re-fetching …")
        else:
            print("   📭 No cache found — fetching for the first time …")

        ok = _fetch_and_save_remote()
        if not ok:
            if INPUT_REMOTE_CACHE.exists():
                print("   ⚠️  Download failed — falling back to existing stale cache")
            else:
                print("   ⚠️  Download failed and no cache exists — skipping Source 2")
                return []

    # Parse URLs from the on-disk cache
    raw  = INPUT_REMOTE_CACHE.read_text(encoding="utf-8")
    urls = _parse_urls_from_raw(raw)
    print(f"   📄 {len(urls):,} URLs loaded from remote_url_source.json")
    return urls


# ════════════════════════════════════════════════════════════════════════
#  PARSING HELPERS
# ════════════════════════════════════════════════════════════════════════

def decode_embed_id(raw: str):
    """Base64-decode a 'label:url' data-embed-id attribute value."""
    try:
        parts = raw.split(":", 1)
        label = base64.b64decode(parts[0] + "==").decode("utf-8", errors="ignore").strip()
        url   = (
            base64.b64decode(parts[1] + "==").decode("utf-8", errors="ignore").strip()
            if len(parts) > 1 else ""
        )
        return label, url
    except Exception:
        return "?", ""


def parse_anime_meta(url: str) -> dict:
    """Extract anime name, episode number, and dub/sub language from a watch URL."""
    slug = url.rstrip("/").split("/watch/")[-1]
    ep_m = re.search(r"-episode-(\d+)", slug)
    ep   = ep_m.group(1) if ep_m else "?"
    lang = "Dub" if "dubbed" in slug.lower() else "Sub"
    name = (
        re.split(r"-episode-", slug, flags=re.IGNORECASE)[0]
        .replace("-", " ").title()
    )
    return {"ep": ep, "lang": lang, "name": name}


def resolve_cdn_url(voe_url: str, proxy: str) -> str:
    """Follow a voe embed URL to its CDN /e/ endpoint."""
    try:
        r = get_session().get(
            voe_url,
            timeout=20,
            headers={"Referer": "https://anihq.cc/"},
            proxies={"http": proxy, "https": proxy},
        )
        m = re.search(
            r"window\.location\.href\s*=\s*['\"]([^'\"]+/e/[a-z0-9]+)['\"]",
            r.text,
        )
        if m:
            cdn = m.group(1)
            return ("https:" + cdn) if cdn.startswith("//") else cdn
    except Exception as exc:
        print(f"      [WARN] CDN resolve: {exc}")
    return ""


def extract_m3u8_urls(html: str) -> list:
    """Pull every unique .m3u8 URL from a page's HTML source."""
    found = re.findall(r'https?://[^\s\'"<>]+\.m3u8[^\s\'"<>]*', html)
    for key in ("file", "src", "hls", "source", "url"):
        found += re.findall(
            r'["\']?' + key + r'["\']?\s*[:=]\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            html, re.IGNORECASE,
        )
    seen, out = set(), []
    for u in found:
        u = u.rstrip('.,;:!?)]}\\"\'')
        if u and u not in seen:
            seen.add(u)
            out.append(u)
    return out


# ════════════════════════════════════════════════════════════════════════
#  CORE EXTRACTION
# ════════════════════════════════════════════════════════════════════════

def scrape_episode(page_url: str, serial: int, proxy: str) -> dict:
    """
    Fetch one anihq watch page and return a record with all stream / m3u8 URLs.
    """
    meta = parse_anime_meta(page_url)
    print(f"  [{serial}] {meta['name']}  |  Ep {meta['ep']}  |  {meta['lang']}")
    print(f"      URL : {page_url}")

    record = {
        "serial":      serial,
        "url":         page_url,
        "ep":          meta["ep"],
        "language":    meta["lang"],
        "anime_name":  meta["name"],
        "stream_urls": [],
        "m3u8_urls":   [],
    }

    time.sleep(random.uniform(1.2, 2.5))

    r = get_session().get(
        page_url,
        timeout=25,
        proxies={"http": proxy, "https": proxy},
    )
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    for btn in soup.find_all(attrs={"data-embed-id": True}):
        label, voe_url = decode_embed_id(btn.get("data-embed-id", ""))
        if not voe_url:
            continue
        if voe_url not in record["stream_urls"]:
            record["stream_urls"].append(voe_url)
        cdn_url = resolve_cdn_url(voe_url, proxy)
        if cdn_url and cdn_url not in record["stream_urls"]:
            record["stream_urls"].append(cdn_url)
        print(f"      [{label}]  voe → {voe_url}")
        if cdn_url:
            print(f"               cdn → {cdn_url}")

    record["m3u8_urls"] = extract_m3u8_urls(r.text)
    return record


def format_readable(rec: dict) -> str:
    """Convert a record dict to a clean human-readable text block."""
    lines = [
        f"Serial No      : {rec['serial']}",
        f"Anime / Movie  : {rec['anime_name']}",
        f"Episode        : {rec['ep']}   |   Language: {rec['language']}",
        f"Watch URL      : {rec['url']}",
    ]
    for i, u in enumerate(rec["stream_urls"], 1):
        lines.append(f"Stream URL {i:>2}  : {u}")
    for i, u in enumerate(rec["m3u8_urls"], 1):
        lines.append(f"M3U8 URL   {i:>2}  : {u}")
    lines.append("─" * 80)
    return "\n".join(lines)


def pick_proxy(idx: int) -> str:
    return PROXIES[idx % len(PROXIES)]


# ════════════════════════════════════════════════════════════════════════
#  MAIN
# ════════════════════════════════════════════════════════════════════════

def main():
    print()
    print("╔══════════════════════════════════════════════════╗")
    print("║   anihq.cc Stream Extractor  v11.0               ║")
    print("╠══════════════════════════════════════════════════╣")
    print(f"║   input/   → {str(INPUT_DIR):<36}║")
    print(f"║   output/  → {str(OUTPUT_DIR):<36}║")
    print("╚══════════════════════════════════════════════════╝")
    print()

    # ── 1. Load both input sources ───────────────────────────────────
    print("─" * 54)

    # Source 1 — local watch_urls.txt
    local_urls: list = []
    print(f"📄 Input Source 1 : {INPUT_LOCAL_URLS.name}")
    if INPUT_LOCAL_URLS.exists():
        local_urls = [
            l.strip()
            for l in INPUT_LOCAL_URLS.read_text(encoding="utf-8").splitlines()
            if l.strip()
        ]
        print(f"   📄 {len(local_urls):,} URLs loaded from {INPUT_LOCAL_URLS.name}")
    else:
        print(f"   ℹ️  File not found — place your URLs in {INPUT_LOCAL_URLS}")

    print()

    # Source 2 — remote_url_source.json  (cached in input/)
    remote_urls = load_remote_input_urls()

    print()
    print("─" * 54)

    # ── 2. Merge, deduplicate, filter valid watch-page URLs ──────────
    all_urls   = local_urls + remote_urls
    valid_urls = list(dict.fromkeys(
        u for u in all_urls
        if u.startswith("http") and "/watch/" in u
    ))

    # ── 3. Skip already-done URLs ────────────────────────────────────
    skip_set = load_all_skip_urls()
    pending  = [u for u in valid_urls if u not in skip_set]

    print(f"  Source 1 (watch_urls.txt)        : {len(local_urls):>7,} URLs")
    print(f"  Source 2 (remote_url_source.json): {len(remote_urls):>7,} URLs")
    print(f"  Total unique valid /watch/ URLs  : {len(valid_urls):>7,}")
    print(f"  Already processed (skip)         : {len(skip_set):>7,}")
    print(f"  Pending                          : {len(pending):>7,}")
    print(f"  This batch                       : {min(BATCH_SIZE, len(pending)):>7,}")
    print("─" * 54)
    print()

    if not pending:
        print("✅ All URLs already processed — nothing to do.")
        return

    batch = pending[:BATCH_SIZE]

    # ── 4. Resolve active output file paths ──────────────────────────
    json_out = next_output_path(OUT_STREAM_JSON, ".json")
    txt_out  = next_output_path(OUT_STREAM_TXT,  ".txt")
    proc_out = next_output_path(OUT_PROCESSED,   ".txt")
    fail_out = next_output_path(OUT_FAILED,      ".txt")

    print("  Output files this run:")
    print(f"    stream_data   → output/{json_out.name}")
    print(f"    readable      → output/{txt_out.name}")
    print(f"    processed log → output/{proc_out.name}")
    print(f"    failed log    → output/{fail_out.name}")
    print()

    # Load existing JSON records so we append rather than overwrite
    existing: list = []
    if json_out.exists():
        try:
            existing = json.loads(json_out.read_text(encoding="utf-8"))
        except Exception:
            existing = []

    serial      = max((r.get("serial", 0) for r in existing), default=0) + 1
    new_records = []
    new_texts   = []
    ok_count    = err_count = 0
    proxy_idx   = 0

    # ── 5. Process batch ─────────────────────────────────────────────
    for url in batch:
        success      = False
        attempts     = 0
        max_attempts = len(PROXIES) * 2

        while attempts < max_attempts and not success:
            proxy = pick_proxy(proxy_idx)
            host  = proxy.split("@")[1] if "@" in proxy else proxy
            print(f"  [PROXY] {host}")
            try:
                rec = scrape_episode(url, serial, proxy)
                new_records.append(rec)
                new_texts.append(format_readable(rec))
                append_line(proc_out, url)
                serial   += 1
                ok_count += 1
                success   = True
                print("  [OK] ✓\n")
            except Exception as exc:
                attempts  += 1
                proxy_idx += 1
                print(f"  [RETRY] {exc}")
                time.sleep(2)

        if not success:
            err_count += 1
            append_line(fail_out, f"{url}  # all proxies failed")
            print("  [FAILED] all proxies exhausted\n")
        else:
            # Re-check paths after every success — files may have crossed 700 KB
            json_out = next_output_path(OUT_STREAM_JSON, ".json")
            txt_out  = next_output_path(OUT_STREAM_TXT,  ".txt")
            proc_out = next_output_path(OUT_PROCESSED,   ".txt")
            fail_out = next_output_path(OUT_FAILED,      ".txt")
            time.sleep(REQUEST_DELAY)

    # ── 6. Persist results ───────────────────────────────────────────
    final_json = next_output_path(OUT_STREAM_JSON, ".json")
    base_data: list = []
    if final_json.exists():
        try:
            base_data = json.loads(final_json.read_text(encoding="utf-8"))
        except Exception:
            base_data = []
    with open(final_json, "w", encoding="utf-8") as f:
        json.dump(base_data + new_records, f, indent=2, ensure_ascii=False)

    if new_texts:
        final_txt = next_output_path(OUT_STREAM_TXT, ".txt")
        with open(final_txt, "a", encoding="utf-8") as f:
            f.write("\n\n".join(new_texts) + "\n\n")

    print()
    print("━" * 54)
    print(f"  Batch complete  |  ✅ OK: {ok_count}   ❌ Failed: {err_count}")
    print("━" * 54)
    print()


if __name__ == "__main__":
    main()
