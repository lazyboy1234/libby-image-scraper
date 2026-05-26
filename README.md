# Libby image scraper

Pulls missing product images from public CDNs (upcitemdb / go-upc / Bing), running
on GitHub Actions for free rotating-IP coverage. Each Actions run gets a fresh IP
from GitHub's pool, so we never get sticky-blocked the way our home IP does.

DATABASE_URL is a repo secret — never committed.

