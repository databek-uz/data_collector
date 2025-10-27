import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse, io, os, gzip, warnings

from lxml import etree
from bs4 import BeautifulSoup
from readability import Document
from urllib.parse import urlparse
from datetime import datetime, timezone
from markdownify import markdownify as md

# SSL warninglarni to'xtatamiz
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

def iso_now(): return datetime.now(timezone.utc).isoformat()
def domain_of(url): return urlparse(url).netloc
def is_sitemap_link(url): return url.lower().endswith((".xml", ".xml.gz"))

def http_get(url, as_bytes=False, timeout=30):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "SitemapMini/1.0"}, verify=True)
    except requests.exceptions.SSLError:
        print(f"[SSL] verify failed, retrying insecure: {url}")
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "SitemapMini/1.0"}, verify=False)
    return r.status_code, (r.content if as_bytes else r.text)

def parse_locs(xml_bytes):
    if xml_bytes[:2] == b"\x1f\x8b":
        xml_bytes = gzip.decompress(xml_bytes)
    root = etree.fromstring(xml_bytes)
    return [el.text.strip() for el in root.xpath("//*[local-name()='loc']") if (el.text or "").strip()]

def detect_lang_from_url(url: str) -> str:
    try:
        path = urlparse(url).path or ""
        first_seg = path.lstrip("/").split("/", 1)[0].lower()
        if first_seg in ("en", "ru"):
            return first_seg
    except Exception:
        pass
    return "uz"

def collect_urls(sitemap_url, seen=None, limit=None):
    if seen is None: seen = set()
    if sitemap_url in seen: return []
    seen.add(sitemap_url)

    print(f"[SITEMAP] fetch: {sitemap_url}")
    st, blob = http_get(sitemap_url, as_bytes=True)
    if st != 200 or not blob:
        print(f"[SITEMAP] failed ({st}): {sitemap_url}")
        return []

    locs = parse_locs(blob)
    print(f"[SITEMAP] found <loc>: {len(locs)} from {sitemap_url}")

    urls, child_maps = [], []
    for loc in locs:
        (child_maps if is_sitemap_link(loc) else urls).append(loc)

    out = urls[: (limit or len(urls))]
    for sm in child_maps:
        if limit and len(out) >= limit: break
        more = collect_urls(sm, seen, None if not limit else (limit - len(out)))
        out.extend(more)

    seen_u, ordered = set(), []
    for u in out:
        if u not in seen_u:
            seen_u.add(u); ordered.append(u)

    if limit: ordered = ordered[:limit]
    print(f"[SITEMAP] accumulated URLs: {len(ordered)}")
    return ordered

def extract_text_markdown(html):
    try:
        article_html = Document(html).summary(html_partial=True) or ""
        soup = BeautifulSoup(article_html, "lxml")
    except Exception:
        soup = BeautifulSoup(html, "lxml")
    for img in soup.find_all("img"): img.decompose()
    text = soup.get_text("\n").strip()
    markdown = md(str(soup), strip=["img"]).strip()
    return text, markdown

def append_parquet(df_new, out_path):
    print(f"[SAVE] target: {out_path}")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    if os.path.exists(out_path):
        print("[SAVE] merging with existing parquet…")
        df_old = pd.read_parquet(out_path)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new
    df_all = (df_all.assign(_t=pd.to_datetime(df_all["created_at"], errors="coerce"))
                    .sort_values("_t").drop(columns="_t")
                    .drop_duplicates(subset="url", keep="last"))
    table = pa.Table.from_pandas(df_all, preserve_index=False)
    tmp = f"{out_path}.tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, out_path)
    print(f"[SAVE] done. rows={len(df_all)}")

def run(sitemap_url, out_root, max_pages):
    print(f"[START] sitemap={sitemap_url}  max_pages={max_pages or '∞'}")
    urls = collect_urls(sitemap_url, limit=(None if max_pages == 0 else max_pages))
    if not urls:
        print("[END] no URLs found from sitemap(s).")
        return

    print(f"[URLS] total: {len(urls)}")
    src = domain_of(sitemap_url)
    out_path = os.path.join(out_root, f"{src.replace('.', '_')}.parquet")

    rows = []
    for i, u in enumerate(urls, 1):
        if i % 50 == 0 or i == 1 or i == len(urls):
            print(f"[FETCH] {i}/{len(urls)}: {u}")
        st, html = http_get(u, as_bytes=False, timeout=40)
        if st != 200 or not html:
            continue
        text, markdown = extract_text_markdown(html)
        if not text:
            continue
        rows.append({
            "url": u,
            "text": text,
            "markdown": markdown,
            "lang": detect_lang_from_url(u),
            "source": src,
            "created_at": iso_now(),
        })

    if not rows:
        print("[END] no content extracted.")
        return

    df = pd.DataFrame(rows)[["url","text","markdown","lang","source","created_at"]]
    for c in df.columns: df[c] = df[c].astype("string")
    append_parquet(df, out_path)
    print("[END] completed successfully.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sitemap", required=True)
    ap.add_argument("--out-root", default="data")
    ap.add_argument("--max-pages", type=int, default=0)
    args = ap.parse_args()
    run(args.sitemap, args.out_root, args.max_pages)
