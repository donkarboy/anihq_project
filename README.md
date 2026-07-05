# anihq.cc Stream Extractor

Batch extracts stream URLs from [anihq.cc](https://anihq.cc) watch pages.

## Folder layout

```
anihq_project/
├── main.py                          ← main extractor script
├── .github/workflows/
│   └── anihq_scraper.yml            ← GitHub Actions (runs every 20 min)
│
├── input/
│   ├── watch_urls.txt               ← Source 1: local watch-page URLs (one per line)
│   └── remote_url_source.json       ← Source 2: cached remote GitHub JSON
│                                       (auto-refreshed every hour at runtime)
│
└── output/                          ← all output files written here
    ├── stream_data.json             ← extracted stream records
    ├── stream_data_readable.txt     ← human-readable mirror
    ├── processed_urls.txt           ← skip-list: successfully processed
    └── failed_urls.txt              ← skip-list: all proxies failed
    (each file auto-splits at 700 KB → stream_data_2.json, _3.json …)
```

## How it works

1. Reads URLs from both input sources and merges them
2. Skips any URL already in `output/processed_urls.txt` or `output/failed_urls.txt`
3. Processes up to 500 URLs per run through rotating proxies
4. Saves stream / m3u8 URLs to `output/stream_data.json` + readable `.txt`
5. Logs successes to `processed_urls.txt`, failures to `failed_urls.txt`
6. All output files auto-split at 700 KB

## Run locally

```bash
pip install curl_cffi beautifulsoup4
python main.py
```

## Run on GitHub Actions

Push this repo to GitHub — the workflow triggers automatically every 20 minutes
and commits new results back to the repo.
