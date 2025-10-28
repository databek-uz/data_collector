#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import argparse, io, os, gzip, warnings, shutil, sys, time, random, json, re
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from lxml import etree
from lxml import html as lxml_html
from bs4 import BeautifulSoup
from readability import Document
from urllib.parse import urlparse
from datetime import datetime, timezone
from markdownify import markdownify as md

# SSL warninglarni to'xtatamiz
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# =========================
# --- Yordamchi funksiyalar
# =========================

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def domain_of(url):
    return urlparse(url).netloc

def is_sitemap_link(url):
    return url.lower().endswith((".xml", ".xml.gz"))

# requests.Session ishlatamiz (tezroq va barqarorroq) + adapter-level retry
def build_session():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "SitemapMini/1.3 (+https://example.com)",
        "Accept": "*/*",
        "Connection": "keep-alive",
    })
    retry = Retry(
        total=3,               # umumiy urinishlar
        connect=3,             # ulanish xatolarida
        read=3,                # o‘qish xatolarida
        backoff_factor=0.4,    # 0.4, 0.8, 1.6 ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s

session = build_session()

def http_get(url, as_bytes=False, timeout=30, retries=4, backoff=0.5):
    """Tarmoqqa chidamli GET: ConnectionReset (10054), ReadTimeout, va boshqalarga qayta urinish.
    Muvaffaqiyatsiz bo'lsa (status=0) bo'sh javob qaytaradi, exception ko'tarmaydi.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            r = session.get(url, timeout=(10, timeout), verify=True)
            return r.status_code, (r.content if as_bytes else r.text)
        except requests.exceptions.SSLError:
            print(f"[SSL] verify failed, retrying insecure: {url}")
            try:
                r = session.get(url, timeout=(10, timeout), verify=False)
                return r.status_code, (r.content if as_bytes else r.text)
            except requests.exceptions.RequestException as e:
                last_exc = e
        except (requests.exceptions.ConnectionError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ChunkedEncodingError) as e:
            last_exc = e
            sleep = backoff * (2 ** attempt) + random.random() * 0.1
            print(f"[NET] {type(e).__name__} on {url} (attempt {attempt+1}/{retries}), sleep {sleep:.2f}s")
            time.sleep(sleep)
        except requests.exceptions.RequestException as e:
            last_exc = e
            break
    print(f"[NET] failed to fetch: {url}  err={repr(last_exc)}")
    return 0, (b"" if as_bytes else "")

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
    if seen is None:
        seen = set()
    if sitemap_url in seen:
        return []
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
        if limit and len(out) >= limit:
            break
        more = collect_urls(sm, seen, None if not limit else (limit - len(out)))
        out.extend(more)

    seen_u, ordered = set(), []
    for u in out:
        if u not in seen_u:
            seen_u.add(u)
            ordered.append(u)

    if limit:
        ordered = ordered[:limit]
    print(f"[SITEMAP] accumulated URLs: {len(ordered)}")
    return ordered

# =========================================
# --- Incremental saqlash va dublikatni oldini olish
# =========================================

def load_existing_urls(parquet_path) -> set:
    if not os.path.exists(parquet_path):
        return set()
    try:
        urls = pd.read_parquet(parquet_path, columns=["url"]).astype("string")["url"].dropna().tolist()
        return set(urls)
    except Exception as e:
        print(f"[WARN] cannot read existing parquet ({e}). Continue as empty.")
        return set()

def safe_write_parquet(table: pa.Table, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    tmp = f"{out_path}.tmp"
    bak = f"{out_path}.bak"

    if os.path.exists(out_path):
        try:
            shutil.copy2(out_path, bak)
        except Exception as e:
            print(f"[WARN] backup failed: {e}")

    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, out_path)

    try:
        if os.path.exists(bak):
            os.remove(bak)
    except Exception:
        pass

def append_parquet_incremental(df_new: pd.DataFrame, out_path: str):
    print(f"[SAVE] target: {out_path}")
    if os.path.exists(out_path):
        print("[SAVE] merging with existing parquet…")
        try:
            df_old = pd.read_parquet(out_path)
        except Exception as e:
            print(f"[WARN] failed to read existing parquet ({e}). Proceed with new only.")
            df_old = pd.DataFrame(columns=df_new.columns)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new

    df_all = (
        df_all.assign(_t=pd.to_datetime(df_all["crawled_at"], errors="coerce"))
        .sort_values("_t")
        .drop(columns="_t")
        .drop_duplicates(subset="url", keep="last")
    )

    table = pa.Table.from_pandas(df_all, preserve_index=False)
    safe_write_parquet(table, out_path)
    print(f"[SAVE] done. rows={len(df_all)}")

# ============================
# --- Kuchaytirilgan CONTEXT extractor
# ============================

BLOCK_KILL = [
    "script","style","header","footer","nav","aside","noscript","iframe","svg","form","template","canvas",
]
CANDIDATE_XPATH = "|".join([
    "//article",
    "//*[@role='main']",
    "//main",
    "//section",
    "//div",
])

# Ixtiyoriy: ayrim domenlar uchun maxsus selektorlar (kerak bo‘lsa to‘ldiring)
DOMAIN_SELECTORS = {
    "kun.uz": [".news-inner__left"],
    "daryo.uz": [".news-page-session__grid_alias"]
}

def clean_dom(doc):
    # Keraksiz bloklarni o‘chirib tashlash
    for tag in BLOCK_KILL:
        for el in doc.findall(f".//{tag}"):
            parent = el.getparent()
            if parent is not None:
                parent.remove(el)
    # Kommentlarni tozalash
    lxml_html.etree.strip_tags(doc, lxml_html.etree.Comment)
    return doc

def text_stats(el):
    try:
        text = el.text_content()
    except TypeError:
        text = lxml_html.tostring(el, method="text", encoding="unicode")

    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    text = text.strip()

    words = len(re.findall(r"\w{2,}", text))
    links = len(el.findall(".//a"))
    density = words / (1 + links)
    return text, words, links, density


def pick_best_candidate(doc):
    best = ("", 0, 0, 0.0, None)  # text, words, links, density, node
    for el in doc.xpath(CANDIDATE_XPATH):
        t, w, l, d = text_stats(el)
        if w < 60:
            continue
        if l > 0 and (w / (l + 1)) < 20:
            continue
        score = d
        if score > best[3]:
            best = (t, w, l, d, el)
    return best

def meta_article_body(tree):
    # JSON-LD Article/NewsArticle qidiramiz
    for node in tree.xpath("//script[@type='application/ld+json']/text()"):
        try:
            data = json.loads(node)
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            # @graph bo‘lishi ham mumkin
            objs = [obj]
            if "@graph" in obj and isinstance(obj["@graph"], list):
                objs.extend([x for x in obj["@graph"] if isinstance(x, dict)])
            for o in objs:
                typ = o.get("@type")
                tset = set()
                if isinstance(typ, list):
                    tset = set([str(t).lower() for t in typ if isinstance(t, str)])
                elif isinstance(typ, str):
                    tset = {typ.lower()}
                if {"article","newsarticle"}.intersection(tset):
                    body = o.get("articleBody") or o.get("text")
                    if body and len(str(body).strip()) > 100:
                        return str(body).strip()
    # OpenGraph / meta fallback
    meta = {}
    for m in tree.xpath("//meta[@property or @name]"):
        k = (m.get("property") or m.get("name") or "").lower()
        v = m.get("content") or ""
        if k and v:
            meta[k] = v
    for key in ["article:content", "og:article:content", "twitter:description", "description"]:
        if key in meta and len(meta[key].strip()) > 100:
            return meta[key].strip()
    return ""

def html_to_markdown(fragment_html: str) -> str:
    try:
        return md(
            fragment_html,
            strip=["img", "video", "source", "svg"],
            heading_style="ATX",
            bullets="*",
        ).strip()
    except Exception:
        return BeautifulSoup(fragment_html, "lxml").get_text("\n").strip()

def try_domain_selectors(tree, domain):
    """DOMAIN_SELECTORS dagi CSS selektorlarni XPath ga aylantirib, fragmentlarni to‘playdi."""
    frags = []
    print(DOMAIN_SELECTORS[domain])
    if not domain or domain not in DOMAIN_SELECTORS:
        return frags
    try:
        from cssselect import GenericTranslator
        for css in DOMAIN_SELECTORS[domain]:
            try:
                xpath = GenericTranslator().css_to_xpath(css)
                for el in tree.xpath(xpath):
                    frags.append(lxml_html.tostring(el, encoding="unicode"))
            except Exception:
                continue
    except Exception:
        # cssselect o‘rnatilmagan yoki xatolik — e’tiborsiz qoldiramiz
        pass
    print(frags)
    return frags

def extract_main_content(html: str, base_url: str = "", allow_domain_rules: bool = True):
    """Kuchaytirilgan extractor: Readability + JSON-LD/meta + zichlik + ixtiyoriy domain qoidalari.
    Natija: (text, markdown)
    """
    candidates = []

    # 1) Readability kandidati
    try:
        rd_html = Document(html).summary(html_partial=True) or ""
        if rd_html:
            candidates.append(("readability", rd_html))
    except Exception:
        pass

    # 2) DOM ni quramiz va tozalaymiz
    try:
        tree = lxml_html.fromstring(html)
    except Exception:
        soup_tmp = BeautifulSoup(html, "lxml")
        tree = lxml_html.fromstring(str(soup_tmp))
    tree = clean_dom(tree)

    domain = domain_of(base_url) if base_url else ""

    # 3) Domain-spetsifik selektorlar (ixtiyoriy)
    if allow_domain_rules:
        for frag in try_domain_selectors(tree, domain):
            candidates.append(("domain", frag))

    # 4) JSON-LD / meta’dan articleBody olish
    body_meta = meta_article_body(tree)
    if body_meta:
        candidates.append(("jsonld/meta", f"<div>{body_meta}</div>"))

    # 5) Heuristik skoring bilan eng zich blok
    t, w, l, d, node = pick_best_candidate(tree)
    if node is not None and len(t) > 100:
        candidates.append(("density", lxml_html.tostring(node, encoding="unicode")))

    # 6) Kandidatlarni baholash va eng yaxshisini tanlash
    best_html = ""
    best_score = -1
    for _, frag_html in candidates:
        plain = BeautifulSoup(frag_html, "lxml").get_text("\n").strip()
        words = len(re.findall(r"\w{2,}", plain))
        lines = plain.count("\n") + 1
        score = words + min(lines, 200)
        if score > best_score:
            best_score = score
            best_html = frag_html

    # 7) Agar topilmasa — butun body
    if not best_html:
        body = tree.find("body")
        best_html = lxml_html.tostring(body or tree, encoding="unicode")

    # 8) Yakuniy tozalash va Markdown
    soup = BeautifulSoup(best_html, "lxml")
    for tag in soup.find_all(["img","video","source","svg","noscript"]):
        tag.decompose()
    text = soup.get_text("\n").strip()
    markdown = html_to_markdown(str(soup))

    return text, markdown

# =========================
# --- Asosiy ish oqimi
# =========================

def run(sitemap_url, out_root, max_pages, skip_existing=True, checkpoint_every=200):
    """
    skip_existing=True -> parquetda bo'lgan URL'larni qayta yuklamaslik.
    checkpoint_every -> har N ta sahifada oraliq saqlash (agar jarayon to'xtab qolsa, yo'qolishni kamaytiradi).
    """
    print(f"[START] sitemap={sitemap_url}  max_pages={max_pages or '∞'}  skip_existing={skip_existing}")
    urls = collect_urls(sitemap_url, limit=(None if max_pages == 0 else max_pages))
    if not urls:
        print("[END] no URLs found from sitemap(s).")
        return

    print(f"[URLS] total (raw): {len(urls)}")
    src = domain_of(sitemap_url)
    safe_src = src.replace(".", "_").replace(":", "_")
    out_path = os.path.join(out_root, f"{safe_src}.parquet")

    # Mavjud URL'larni yuklab, ro'yxatdan chetlatamiz
    existing = load_existing_urls(out_path) if skip_existing else set()
    if existing:
        before = len(urls)
        urls = [u for u in urls if u not in existing]
        print(f"[URLS] filtered by existing: {before} -> {len(urls)} (skipped {before - len(urls)})")

    if not urls:
        print("[END] nothing new to fetch.")
        return

    rows = []
    fetched = 0

    for i, u in enumerate(urls, 1):
        if i % 50 == 0 or i == 1 or i == len(urls):
            print(f"[FETCH] {i}/{len(urls)}: {u}")

        st, html = http_get(u, as_bytes=False, timeout=40)
        if st != 200 or not html:
            continue

        # YANGI, kuchaytirilgan extractor
        text, markdown = extract_main_content(html, base_url=u)
        if not text or len(text.strip()) < 40:
            # Readability-ga zaxira qayta urinish (ba'zan foyda beradi)
            try:
                alt = Document(html).summary(html_partial=True) or ""
                if alt:
                    soup_alt = BeautifulSoup(alt, "lxml")
                    text_alt = soup_alt.get_text("\n").strip()
                    if len(text_alt) > len(text):
                        text = text_alt
                        markdown = md(str(soup_alt), strip=["img","video","source","svg"]).strip()
            except Exception:
                pass

        if not text:
            continue

        rows.append({
            "url": u,
            "text": str(text),
            "markdown": str(markdown),
            "lang": detect_lang_from_url(u),
            "source": src,
            "crawled_at": iso_now(),
        })
        fetched += 1

        # Checkpoint: har N ta qator to'lganda parquetga qo'shib boramiz
        if checkpoint_every and (fetched % checkpoint_every == 0):
            print(f"[CKPT] flushing {len(rows)} rows…")
            df_ckpt = pd.DataFrame(rows)[["url","text","markdown","lang","source","crawled_at"]]
            for c in df_ckpt.columns:
                df_ckpt[c] = df_ckpt[c].astype("string")
            append_parquet_incremental(df_ckpt, out_path)
            rows = []  # buferni bo'shatamiz

    # Yakuniy flush
    if rows:
        df = pd.DataFrame(rows)[["url","text","markdown","lang","source","crawled_at"]]
        for c in df.columns:
            df[c] = df[c].astype("string")
        append_parquet_incremental(df, out_path)

    print("[END] completed successfully.")

# =========================
# --- CLI
# =========================

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--sitemap", required=True, help="Sitemap.xml yoki indeks URL")
    ap.add_argument("--out-root", default="data", help="Parquetlar yoziladigan papka")
    ap.add_argument("--max-pages", type=int, default=0, help="0 => cheksiz (sitemapdagi hamma URL)")
    ap.add_argument("--no-skip-existing", action="store_true",
                    help="Parquetdagi URL bo'lsa ham qayta yuklash (default: skip)")
    ap.add_argument("--checkpoint-every", type=int, default=200,
                    help="Har nechta sahifadan keyin oraliq saqlash")
    args = ap.parse_args()

    run(
        args.sitemap,
        args.out_root,
        args.max_pages,
        skip_existing=(not args.no_skip_existing),
        checkpoint_every=args.checkpoint_every,
    )
