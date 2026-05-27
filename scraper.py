#!/usr/bin/env python3
"""GH-Actions image scraper. Self-contained — pulls missing-image barcodes from
the Cloud SQL DB, scrapes upcitemdb / go-upc / Bing, writes back image_url.

Why GH Actions: each run launches on a fresh runner with a different public IP
from GitHub's massive pool. This bypasses the source-side rate limits that our
home IP hits after a long sustained scrape. Public-repo runs are free + unlimited.

Reads DATABASE_URL from env (set via GitHub Actions repo secret).
Usage:  python scraper.py --batch 500 --workers 8
"""
from __future__ import annotations
import argparse, json, os, random, re, sys, time, urllib.parse
from concurrent.futures import ThreadPoolExecutor
from typing import Optional

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor


USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

GOOD_CDN = (
    "media-amazon", "ssl-images-amazon", "walmartimages", "samsclubimages",
    "scene7.com", "kroger.com", "albertsons-media", "go-upc.s3", "openfoodfacts",
    "target.scene7", "smartlabel.pepsico", "fritolay.com", "instacart.com",
)
BAD = (
    "barcode", "shutterstock", "alamy", "istockphoto", "gettyimages",
    "logo", "icon", "avatar", "default.png", "placeholder",
)


def ua() -> str:
    return random.choice(USER_AGENTS)


# ── Source 1: upcitemdb trial ──────────────────────────────────────────────
def upcitemdb(client: httpx.Client, barcode: str) -> Optional[str]:
    try:
        r = client.get(
            f"https://api.upcitemdb.com/prod/trial/lookup?upc={barcode}",
            headers={"User-Agent": ua()}, timeout=12,
        )
        if r.status_code != 200:
            return None
        items = (r.json() or {}).get("items") or []
        if not items:
            return None
        imgs = (items[0] or {}).get("images") or []
        for u in imgs:
            ul = (u or "").lower()
            if any(b in ul for b in BAD):
                continue
            if any(g in ul for g in GOOD_CDN):
                return u
        return imgs[0] if imgs else None
    except Exception:
        return None


# ── Source 2: go-upc.com (scrapes the public product page) ─────────────────
_IMG_RE = re.compile(r'<img[^>]+src="([^"]+)"[^>]*class="[^"]*product-image[^"]*"', re.I)
_OG_RE = re.compile(r'<meta[^>]+property="og:image"[^>]+content="([^"]+)"', re.I)


def go_upc(client: httpx.Client, barcode: str) -> Optional[str]:
    try:
        r = client.get(
            f"https://go-upc.com/search?q={barcode}",
            headers={"User-Agent": ua()}, timeout=12, follow_redirects=True,
        )
        if r.status_code != 200:
            return None
        m = _IMG_RE.search(r.text) or _OG_RE.search(r.text)
        if m:
            u = m.group(1)
            if not any(b in u.lower() for b in BAD):
                return u
        return None
    except Exception:
        return None


# ── Source 3: Bing image search (name-based) ───────────────────────────────
def bing_image(client: httpx.Client, query: str) -> Optional[str]:
    try:
        url = (
            "https://www.bing.com/images/search?q="
            + urllib.parse.quote(query[:80])
            + "&form=HDRSC2&first=1"
        )
        r = client.get(url, headers={"User-Agent": ua()}, timeout=12)
        if r.status_code != 200:
            return None
        # Bing embeds image URLs in JSON metadata inside m="...image: ..."
        matches = re.findall(r'mediaurl=([^"&]+)', r.text)
        candidates = [urllib.parse.unquote(m) for m in matches[:30]]
        candidates = [c for c in candidates if not any(b in c.lower() for b in BAD)]
        for c in candidates:
            if any(g in c.lower() for g in GOOD_CDN):
                return c
        return candidates[0] if candidates else None
    except Exception:
        return None


# ── Source 4: DuckDuckGo image search (free, no API key) ───────────────────
# DDG often surfaces images that Bing misses, especially for US store-brand
# products from minor retailers. Two-step protocol: first get a vqd token
# from the HTML page, then call the JSON image endpoint.
def ddg_image(client: httpx.Client, query: str) -> Optional[str]:
    try:
        # Step 1: scrape the vqd token from DDG's HTML response
        seed_url = "https://duckduckgo.com/?q=" + urllib.parse.quote(query[:80])
        seed = client.get(seed_url, headers={"User-Agent": ua()}, timeout=10)
        m = re.search(r"vqd=['\"]?([\d-]+)", seed.text) or re.search(r"vqd=([\d-]+)", seed.text)
        if not m:
            return None
        vqd = m.group(1)
        # Step 2: call the JSON endpoint
        api_url = (
            "https://duckduckgo.com/i.js?l=us-en&o=json&q="
            + urllib.parse.quote(query[:80])
            + f"&vqd={vqd}&f=,,,&p=1"
        )
        r = client.get(api_url, headers={"User-Agent": ua(), "Referer": "https://duckduckgo.com/"}, timeout=10)
        if r.status_code != 200:
            return None
        results = (r.json() or {}).get("results") or []
        candidates = [str((it or {}).get("image") or "") for it in results[:30] if it]
        candidates = [c for c in candidates if c and not any(b in c.lower() for b in BAD)]
        for c in candidates:
            if any(g in c.lower() for g in GOOD_CDN):
                return c
        return candidates[0] if candidates else None
    except Exception:
        return None


# ── Source 5: Manufacturer-site search via Bing (brand-only query) ──────────
# For private-label products where barcode lookups fail, the brand's own site
# often has a product image. Bing query "site:brand.com {name}" narrows the
# search to that domain — far higher hit rate for niche / regional brands.
def manufacturer_site_search(client: httpx.Client, name: str, brand: str | None) -> Optional[str]:
    if not brand or not name:
        return None
    try:
        # Strip the brand's legal suffix so the domain heuristic is cleaner.
        bclean = re.sub(r"(\sInc\.?|\sLLC|\sCo\.?|\sCorp\.?|\sLtd\.?)$", "", brand, flags=re.I).strip()
        if not bclean or len(bclean) < 2:
            return None
        # Site-scoped Bing query (works even when the brand has no www. presence).
        q = f'"{bclean}" {name[:50]} product'
        url = (
            "https://www.bing.com/images/search?q="
            + urllib.parse.quote(q[:120])
            + "&form=HDRSC2"
        )
        r = client.get(url, headers={"User-Agent": ua()}, timeout=12)
        if r.status_code != 200:
            return None
        matches = re.findall(r'mediaurl=([^"&]+)', r.text)
        candidates = [urllib.parse.unquote(m) for m in matches[:30]]
        candidates = [c for c in candidates if not any(b in c.lower() for b in BAD)]
        # Prefer images whose URL contains the brand's domain stem
        bstem = re.sub(r"[^a-z0-9]", "", bclean.lower())[:10]
        for c in candidates:
            if bstem and bstem in c.lower():
                return c
        for c in candidates:
            if any(g in c.lower() for g in GOOD_CDN):
                return c
        return candidates[0] if candidates else None
    except Exception:
        return None


def resolves_to_image(client: httpx.Client, url: str) -> bool:
    """Confirm the URL actually returns an image (never write a dead URL to DB)."""
    try:
        r = client.get(url, headers={"User-Agent": ua()}, timeout=10)
        return (
            r.status_code == 200
            and r.headers.get("content-type", "").startswith("image")
            and len(r.content) > 1500
        )
    except Exception:
        return False


# Trusted CDNs that almost always resolve — skip the extra validation GET.
TRUSTED = (
    "media-amazon", "ssl-images-amazon", "walmartimages", "scene7",
    "samsclubimages", "go-upc.s3", "kroger", "albertsons-media",
)


def find_image(client: httpx.Client, name: str, brand: str | None, barcode: str | None) -> Optional[str]:
    name = (name or "").strip()
    if not name or len(name) < 3:
        return None
    q = f"{brand} {name}".strip() if brand else name

    sources = [
        lambda: upcitemdb(client, barcode) if barcode else None,
        lambda: go_upc(client, barcode) if barcode else None,
        lambda: bing_image(client, q) if q else None,
        lambda: ddg_image(client, q) if q else None,
        lambda: manufacturer_site_search(client, name, brand) if brand else None,
    ]
    for fn in sources:
        try:
            u = fn()
        except Exception:
            u = None
        if not u:
            continue
        if any(t in u.lower() for t in TRUSTED) or resolves_to_image(client, u):
            return u
    return None


# ── DB helpers ──────────────────────────────────────────────────────────────
def claim_batch(conn, n: int) -> list[dict]:
    """Atomically claim N missing-image scored products. Uses an UPDATE ... RETURNING
    pattern to mark them with a sentinel (image_url = '__claimed_<ts>') so other
    concurrent GH runs don't pick the same rows. The sentinel is rolled back to
    NULL if we don't find an image, so the row becomes claimable again later.
    """
    sentinel = f"__claim_{int(time.time())}_{random.randint(1000,9999)}"
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(
            f"""WITH cte AS (
                  SELECT barcode FROM products
                  WHERE (image_url IS NULL OR image_url='')
                    AND name IS NOT NULL
                    AND barcode ~ '^[0-9]+$' AND length(barcode) >= 8
                    -- score filter dropped at 98pct coverage: remaining gap is
                    -- mostly unscored / private-label barcodes that still need
                    -- an image for the search page even without a score
                  ORDER BY base_analysis_at DESC NULLS LAST, last_recomputed DESC NULLS LAST
                  LIMIT %s
                  FOR UPDATE SKIP LOCKED
                )
                UPDATE products SET image_url = %s
                FROM cte WHERE products.barcode = cte.barcode
                RETURNING products.barcode, products.name, products.brand""",
            (n, sentinel),
        )
        rows = cur.fetchall()
    conn.commit()
    return [dict(r) for r in rows]


def write_result(conn, barcode: str, url: Optional[str]) -> None:
    """Either set the real URL, or clear the sentinel back to NULL."""
    with conn.cursor() as cur:
        if url:
            cur.execute(
                "UPDATE products SET image_url = %s WHERE barcode = %s",
                (url, barcode),
            )
        else:
            cur.execute(
                "UPDATE products SET image_url = NULL WHERE barcode = %s AND image_url LIKE '__claim_%%'",
                (barcode,),
            )
    conn.commit()


def worker(rec: dict, conn) -> bool:
    time.sleep(random.uniform(0.05, 0.3))  # light jitter
    with httpx.Client(follow_redirects=True) as client:
        url = find_image(client, rec["name"], rec.get("brand"), rec["barcode"])
    write_result(conn, rec["barcode"], url)
    return url is not None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--batch", type=int, default=300)
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print("ERROR: DATABASE_URL env var not set", file=sys.stderr)
        sys.exit(2)

    # Show egress IP so we can verify GH gives us a different one each run
    try:
        ip = httpx.get("https://api.ipify.org", timeout=5).text
    except Exception:
        ip = "?"
    print(f"egress IP: {ip}", flush=True)

    conn = psycopg2.connect(db_url, connect_timeout=15)
    try:
        recs = claim_batch(conn, args.batch)
        print(f"claimed batch: {len(recs)} products", flush=True)
        if not recs:
            print("nothing to do — catalog is full or all rows already claimed")
            return

        # Run workers; each opens its own DB cursor via the shared connection.
        # psycopg2 conn is single-threaded for queries → serialize writes via a lock.
        import threading
        write_lock = threading.Lock()

        def task(rec):
            try:
                time.sleep(random.uniform(0.05, 0.3))
                with httpx.Client(follow_redirects=True) as client:
                    url = find_image(client, rec["name"], rec.get("brand"), rec["barcode"])
                with write_lock:
                    write_result(conn, rec["barcode"], url)
                return url is not None
            except Exception as e:
                print(f"  err {rec['barcode']}: {e}", flush=True)
                with write_lock:
                    write_result(conn, rec["barcode"], None)
                return False

        t0 = time.time()
        hits = 0
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            for i, ok in enumerate(pool.map(task, recs), 1):
                if ok:
                    hits += 1
                if i % 50 == 0:
                    print(f"  {i}/{len(recs)}  found {hits} ({100*hits//i}%)  {time.time()-t0:.0f}s", flush=True)
        print(f"DONE: found {hits}/{len(recs)} ({100*hits//len(recs)}%) in {time.time()-t0:.0f}s")
    finally:
        # Failsafe: any rows still bearing our sentinel get cleared back to NULL.
        # (handles crashes mid-batch)
        with conn.cursor() as cur:
            cur.execute("UPDATE products SET image_url = NULL WHERE image_url LIKE '__claim_%'")
        conn.commit()
        conn.close()


if __name__ == "__main__":
    main()
