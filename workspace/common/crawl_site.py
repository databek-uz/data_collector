#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Professional News Scraper with Advanced Features
- Multi-threaded scraping
- Comprehensive error handling
- Progress tracking with logging
- Rate limiting and retry logic
- Parquet output with incremental saves
- Resume capability
"""

import os
import re
import sys
import json
import gzip
import time
import random
import logging
import argparse
import warnings
from typing import List, Dict, Set, Optional, Tuple
from datetime import datetime, timezone
from urllib.parse import urlparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, asdict

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lxml import etree, html as lxml_html
from bs4 import BeautifulSoup
from readability import Document
from markdownify import markdownify as md
from tqdm import tqdm

# Disable SSL warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")
warnings.filterwarnings("ignore", category=UserWarning)

# ============================================================================
# Configuration & Data Classes
# ============================================================================

@dataclass
class ScraperConfig:
    """Scraper configuration"""
    sitemap_urls: List[str]
    output_dir: str = "data"
    max_pages: int = 0  # 0 = unlimited
    max_workers: int = 10
    checkpoint_every: int = 100
    skip_existing: bool = True
    rate_limit: float = 0.1  # seconds between requests per thread
    timeout: int = 30
    max_retries: int = 4
    user_agent: str = "NewsScraperPro/2.0 (+https://example.com)"
    verify_ssl: bool = True
    min_content_length: int = 100
    log_level: str = "INFO"


@dataclass
class Article:
    """Article data structure"""
    url: str
    title: str
    text: str
    markdown: str
    lang: str
    source: str
    crawled_at: str
    word_count: int
    status: str = "success"
    

# ============================================================================
# Logging Setup
# ============================================================================

def setup_logging(log_level: str = "INFO", log_file: Optional[str] = None):
    """Setup comprehensive logging"""
    log_format = "%(asctime)s [%(levelname)s] %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"
    
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        handlers.append(logging.FileHandler(log_file, encoding='utf-8'))
    
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format=log_format,
        datefmt=date_format,
        handlers=handlers,
        force=True
    )
    
    return logging.getLogger(__name__)


# ============================================================================
# HTTP Session Management
# ============================================================================

class HTTPSession:
    """Enhanced HTTP session with retry logic and rate limiting"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.session = self._build_session()
        self.last_request_time = {}
        self.logger = logging.getLogger(__name__)
        
    def _build_session(self) -> requests.Session:
        """Build session with retry strategy"""
        session = requests.Session()
        session.headers.update({
            "User-Agent": self.config.user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "uz,en-US,en;q=0.9,ru;q=0.8",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })
        
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False
        )
        
        adapter = HTTPAdapter(
            max_retries=retry_strategy,
            pool_connections=100,
            pool_maxsize=100,
            pool_block=False
        )
        
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        return session
    
    def _rate_limit(self, domain: str):
        """Apply rate limiting per domain"""
        now = time.time()
        last_time = self.last_request_time.get(domain, 0)
        time_since_last = now - last_time
        
        if time_since_last < self.config.rate_limit:
            time.sleep(self.config.rate_limit - time_since_last)
        
        self.last_request_time[domain] = time.time()
    
    def get(self, url: str, as_bytes: bool = False) -> Tuple[int, any]:
        """
        Make HTTP GET request with retry logic and rate limiting
        Returns: (status_code, content)
        """
        domain = urlparse(url).netloc
        self._rate_limit(domain)
        
        for attempt in range(self.config.max_retries):
            try:
                response = self.session.get(
                    url,
                    timeout=(10, self.config.timeout),
                    verify=self.config.verify_ssl
                )
                
                if response.status_code == 200:
                    return response.status_code, (response.content if as_bytes else response.text)
                elif response.status_code in [429, 503]:
                    # Rate limited or service unavailable
                    wait_time = (2 ** attempt) + random.random()
                    self.logger.warning(f"Rate limited on {url}, waiting {wait_time:.2f}s")
                    time.sleep(wait_time)
                    continue
                else:
                    return response.status_code, (b"" if as_bytes else "")
                    
            except requests.exceptions.SSLError:
                if self.config.verify_ssl:
                    self.logger.warning(f"SSL error on {url}, retrying without verification")
                    try:
                        response = self.session.get(url, timeout=(10, self.config.timeout), verify=False)
                        return response.status_code, (response.content if as_bytes else response.text)
                    except Exception as e:
                        self.logger.error(f"SSL retry failed: {e}")
                        
            except (requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ChunkedEncodingError) as e:
                wait_time = (2 ** attempt) * 0.5 + random.random() * 0.1
                self.logger.warning(f"{type(e).__name__} on {url} (attempt {attempt+1}/{self.config.max_retries}), waiting {wait_time:.2f}s")
                time.sleep(wait_time)
                
            except Exception as e:
                self.logger.error(f"Unexpected error fetching {url}: {e}")
                break
        
        return 0, (b"" if as_bytes else "")


# ============================================================================
# Sitemap Parser
# ============================================================================

class SitemapParser:
    """Parse XML sitemaps and collect URLs"""
    
    def __init__(self, http_session: HTTPSession):
        self.http = http_session
        self.logger = logging.getLogger(__name__)
        
    def discover_sitemap(self, base_url: str) -> Optional[str]:
        """Discover sitemap URL from robots.txt or common locations"""
        domain = urlparse(base_url).netloc
        scheme = urlparse(base_url).scheme or "https"
        
        # Try robots.txt first
        robots_url = f"{scheme}://{domain}/robots.txt"
        self.logger.info(f"Checking robots.txt: {robots_url}")
        
        status, content = self.http.get(robots_url)
        if status == 200 and content:
            for line in content.split('\n'):
                if line.lower().startswith('sitemap:'):
                    sitemap_url = line.split(':', 1)[1].strip()
                    self.logger.info(f"Found sitemap in robots.txt: {sitemap_url}")
                    return sitemap_url
        
        # Try common sitemap locations
        common_paths = [
            '/sitemap.xml',
            '/sitemap_index.xml',
            '/sitemap-index.xml',
            '/news-sitemap.xml',
            '/post-sitemap.xml',
            '/page-sitemap.xml',
            '/sitemaps/sitemap.xml',
        ]
        
        for path in common_paths:
            test_url = f"{scheme}://{domain}{path}"
            self.logger.debug(f"Trying: {test_url}")
            
            status, content = self.http.get(test_url, as_bytes=True)
            if status == 200 and content:
                # Check if it's valid XML
                try:
                    if content[:2] == b"\x1f\x8b":
                        content = gzip.decompress(content)
                    etree.fromstring(content)
                    self.logger.info(f"Found sitemap: {test_url}")
                    return test_url
                except:
                    continue
        
        self.logger.warning(f"Could not discover sitemap for {domain}")
        return None
        
    def parse_sitemap(self, sitemap_url: str, limit: Optional[int] = None) -> List[str]:
        """Parse sitemap recursively and return all article URLs"""
        # Auto-discover if needed
        if not sitemap_url.lower().endswith(('.xml', '.xml.gz')):
            self.logger.info(f"Input is not a sitemap URL, attempting auto-discovery...")
            discovered = self.discover_sitemap(sitemap_url)
            if discovered:
                sitemap_url = discovered
            else:
                self.logger.error(f"Could not find sitemap for: {sitemap_url}")
                return []
        
        self.logger.info(f"Parsing sitemap: {sitemap_url}")
        seen = set()
        urls = self._collect_urls(sitemap_url, seen, limit)
        
        # Remove duplicates while preserving order
        unique_urls = []
        seen_urls = set()
        for url in urls:
            if url not in seen_urls:
                seen_urls.add(url)
                unique_urls.append(url)
        
        self.logger.info(f"Found {len(unique_urls)} unique URLs from sitemap")
        return unique_urls
    
    def _collect_urls(self, sitemap_url: str, seen: Set[str], limit: Optional[int]) -> List[str]:
        """Recursively collect URLs from sitemap"""
        if sitemap_url in seen:
            return []
        seen.add(sitemap_url)
        
        status, content = self.http.get(sitemap_url, as_bytes=True)
        if status != 200 or not content:
            self.logger.warning(f"Failed to fetch sitemap ({status}): {sitemap_url}")
            return []
        
        try:
            # Decompress if gzipped
            if content[:2] == b"\x1f\x8b":
                content = gzip.decompress(content)
            
            # Check if content looks like HTML instead of XML
            content_lower = content[:500].lower()
            if b'<!doctype html' in content_lower or b'<html' in content_lower:
                self.logger.error(f"Received HTML instead of XML from: {sitemap_url}")
                self.logger.error("This might be a wrong URL. Try checking robots.txt or common sitemap locations.")
                return []
            
            # Parse XML
            try:
                root = etree.fromstring(content)
            except etree.XMLSyntaxError as e:
                self.logger.error(f"XML parsing error for {sitemap_url}: {e}")
                # Try to show first few lines for debugging
                try:
                    preview = content.decode('utf-8', errors='ignore')[:200]
                    self.logger.error(f"Content preview: {preview}")
                except:
                    pass
                return []
            
            locs = [el.text.strip() for el in root.xpath("//*[local-name()='loc']") 
                    if (el.text or "").strip()]
            
            self.logger.debug(f"Found {len(locs)} <loc> entries in {sitemap_url}")
            
            # Separate sitemap links from article URLs
            urls = []
            child_sitemaps = []
            
            for loc in locs:
                if self._is_sitemap_link(loc):
                    child_sitemaps.append(loc)
                else:
                    urls.append(loc)
            
            # Apply limit to current batch
            if limit:
                urls = urls[:limit]
            
            # Recursively process child sitemaps
            for child_sitemap in child_sitemaps:
                if limit and len(urls) >= limit:
                    break
                    
                remaining_limit = None if not limit else (limit - len(urls))
                child_urls = self._collect_urls(child_sitemap, seen, remaining_limit)
                urls.extend(child_urls)
                
                if limit and len(urls) >= limit:
                    urls = urls[:limit]
                    break
            
            return urls
            
        except Exception as e:
            self.logger.error(f"Error parsing sitemap {sitemap_url}: {e}")
            import traceback
            self.logger.debug(traceback.format_exc())
            return []
    
    @staticmethod
    def _is_sitemap_link(url: str) -> bool:
        """Check if URL is a sitemap"""
        return url.lower().endswith((".xml", ".xml.gz"))


# ============================================================================
# Content Extractor
# ============================================================================

class ContentExtractor:
    """Extract main content from HTML"""
    
    BLOCK_KILL = [
        "script", "style", "header", "footer", "nav", "aside",
        "noscript", "iframe", "svg", "form", "template", "canvas"
    ]
    
    DOMAIN_SELECTORS = {
        "kun.uz": [".news-inner__left", ".news-body"],
        "daryo.uz": [".news-page-session__grid_alias", ".post-content"],
        "gazeta.uz": [".article-content", ".news-text"],
        "uza.uz": [".article__text", ".news-content"],
        "aniq.uz": [".single-content", ".entry-content"],
    }
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def extract(self, html: str, url: str) -> Tuple[str, str, str]:
        """
        Extract content from HTML
        Returns: (title, text, markdown)
        """
        try:
            # Extract title
            title = self._extract_title(html)
            
            # Try multiple extraction methods
            candidates = []
            
            # Method 1: Readability
            try:
                doc = Document(html)
                rd_title = doc.title()
                rd_html = doc.summary(html_partial=True)
                if rd_html:
                    candidates.append(("readability", rd_html))
                if not title and rd_title:
                    title = rd_title
            except Exception as e:
                self.logger.debug(f"Readability failed: {e}")
            
            # Method 2: Parse DOM
            try:
                tree = lxml_html.fromstring(html)
            except:
                soup = BeautifulSoup(html, "lxml")
                tree = lxml_html.fromstring(str(soup))
            
            tree = self._clean_dom(tree)
            
            # Method 3: Domain-specific selectors
            domain = urlparse(url).netloc
            domain_fragments = self._try_domain_selectors(tree, domain)
            for frag in domain_fragments:
                candidates.append(("domain", frag))
            
            # Method 4: JSON-LD structured data
            jsonld_body = self._extract_jsonld_body(tree)
            if jsonld_body:
                candidates.append(("jsonld", f"<div>{jsonld_body}</div>"))
            
            # Method 5: Meta tags
            meta_body = self._extract_meta_body(tree)
            if meta_body:
                candidates.append(("meta", f"<div>{meta_body}</div>"))
            
            # Method 6: Density-based selection
            best_node = self._find_content_node(tree)
            if best_node is not None:
                candidates.append(("density", lxml_html.tostring(best_node, encoding="unicode")))
            
            # Select best candidate
            best_html = self._select_best_candidate(candidates)
            
            # Fallback to body
            if not best_html:
                body = tree.find("body")
                best_html = lxml_html.tostring(body or tree, encoding="unicode")
            
            # Clean and convert
            soup = BeautifulSoup(best_html, "lxml")
            for tag in soup.find_all(["img", "video", "source", "svg", "noscript"]):
                tag.decompose()
            
            text = soup.get_text("\n").strip()
            text = re.sub(r'\n{3,}', '\n\n', text)  # Remove excessive newlines
            
            markdown = self._html_to_markdown(str(soup))
            
            return title, text, markdown
            
        except Exception as e:
            self.logger.error(f"Content extraction failed for {url}: {e}")
            return "", "", ""
    
    def _extract_title(self, html: str) -> str:
        """Extract title from HTML"""
        try:
            soup = BeautifulSoup(html, "lxml")
            
            # Try Open Graph title
            og_title = soup.find("meta", property="og:title")
            if og_title and og_title.get("content"):
                return og_title["content"].strip()
            
            # Try Twitter title
            tw_title = soup.find("meta", {"name": "twitter:title"})
            if tw_title and tw_title.get("content"):
                return tw_title["content"].strip()
            
            # Try regular title tag
            title_tag = soup.find("title")
            if title_tag:
                return title_tag.get_text().strip()
            
            # Try h1
            h1 = soup.find("h1")
            if h1:
                return h1.get_text().strip()
                
        except Exception:
            pass
        
        return ""
    
    def _clean_dom(self, tree):
        """Remove unwanted elements from DOM"""
        for tag in self.BLOCK_KILL:
            for el in tree.xpath(f"//{tag}"):
                el.getparent().remove(el)
        return tree
    
    def _try_domain_selectors(self, tree, domain: str) -> List[str]:
        """Try domain-specific CSS selectors"""
        fragments = []
        if not domain or domain not in self.DOMAIN_SELECTORS:
            return fragments
        
        try:
            from cssselect import GenericTranslator
            for css in self.DOMAIN_SELECTORS[domain]:
                try:
                    xpath = GenericTranslator().css_to_xpath(css)
                    for el in tree.xpath(xpath):
                        fragments.append(lxml_html.tostring(el, encoding="unicode"))
                except Exception:
                    continue
        except ImportError:
            self.logger.debug("cssselect not installed, skipping domain selectors")
        except Exception as e:
            self.logger.debug(f"Domain selector failed: {e}")
        
        return fragments
    
    def _extract_jsonld_body(self, tree) -> str:
        """Extract article body from JSON-LD structured data"""
        try:
            for script in tree.xpath("//script[@type='application/ld+json']"):
                text = (script.text or "").strip()
                if not text:
                    continue
                try:
                    obj = json.loads(text)
                    if isinstance(obj, list):
                        obj = obj[0] if obj else {}
                    
                    obj_type = obj.get("@type", "").lower()
                    if obj_type in ["article", "newsarticle", "blogposting"]:
                        body = obj.get("articleBody") or obj.get("text")
                        if body and len(str(body).strip()) > 100:
                            return str(body).strip()
                except json.JSONDecodeError:
                    continue
        except Exception:
            pass
        return ""
    
    def _extract_meta_body(self, tree) -> str:
        """Extract content from meta tags"""
        meta = {}
        for m in tree.xpath("//meta[@property or @name]"):
            key = (m.get("property") or m.get("name") or "").lower()
            value = m.get("content") or ""
            if key and value:
                meta[key] = value
        
        for key in ["article:content", "og:description", "description"]:
            if key in meta and len(meta[key].strip()) > 100:
                return meta[key].strip()
        
        return ""
    
    def _find_content_node(self, tree):
        """Find content node using density heuristics"""
        try:
            candidates = tree.xpath("//article | //*[@role='main'] | //main | //div | //section")
            
            best_node = None
            best_score = 0
            
            for node in candidates:
                text = (node.text_content() or "").strip()
                words = len(re.findall(r'\w+', text))
                links = len(node.xpath(".//a"))
                
                # Penalize nodes with too many links
                link_density = links / max(words, 1)
                if link_density > 0.5:
                    continue
                
                # Score based on word count
                score = words * (1 - link_density * 0.5)
                
                if score > best_score and words > 100:
                    best_score = score
                    best_node = node
            
            return best_node
        except Exception:
            return None
    
    def _select_best_candidate(self, candidates: List[Tuple[str, str]]) -> str:
        """Select best content from candidates"""
        best_html = ""
        best_score = -1
        
        for method, html in candidates:
            try:
                soup = BeautifulSoup(html, "lxml")
                text = soup.get_text("\n").strip()
                words = len(re.findall(r'\w+', text))
                lines = text.count("\n") + 1
                
                # Score: prioritize word count with line bonus
                score = words + min(lines, 200) * 0.1
                
                if score > best_score:
                    best_score = score
                    best_html = html
            except Exception:
                continue
        
        return best_html
    
    def _html_to_markdown(self, html: str) -> str:
        """Convert HTML to Markdown"""
        try:
            return md(
                html,
                strip=["img", "video", "source", "svg"],
                heading_style="ATX",
                bullets="*",
            ).strip()
        except Exception:
            soup = BeautifulSoup(html, "lxml")
            return soup.get_text("\n").strip()


# ============================================================================
# Parquet Manager
# ============================================================================

class ParquetManager:
    """Manage parquet file operations"""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logger = logging.getLogger(__name__)
    
    def get_output_path(self, source: str) -> Path:
        """Get output parquet path for source"""
        safe_name = re.sub(r'[^\w\-]', '_', source)
        return self.output_dir / f"{safe_name}.parquet"
    
    def load_existing_urls(self, source: str) -> Set[str]:
        """Load existing URLs from parquet"""
        path = self.get_output_path(source)
        
        if not path.exists():
            return set()
        
        try:
            df = pd.read_parquet(path, columns=["url"])
            urls = df["url"].dropna().tolist()
            self.logger.info(f"Loaded {len(urls)} existing URLs from {path.name}")
            return set(urls)
        except Exception as e:
            self.logger.warning(f"Failed to load existing URLs: {e}")
            return set()
    
    def save_articles(self, articles: List[Article], source: str, append: bool = True):
        """Save articles to parquet"""
        if not articles:
            return
        
        path = self.get_output_path(source)
        
        # Convert to DataFrame
        df_new = pd.DataFrame([asdict(article) for article in articles])
        
        # Ensure correct types
        for col in df_new.columns:
            if col == "word_count":
                df_new[col] = df_new[col].astype("int64")
            else:
                df_new[col] = df_new[col].astype("string")
        
        if append and path.exists():
            try:
                df_old = pd.read_parquet(path)
                df_all = pd.concat([df_old, df_new], ignore_index=True)
            except Exception as e:
                self.logger.warning(f"Failed to load existing parquet: {e}")
                df_all = df_new
        else:
            df_all = df_new
        
        # Remove duplicates (keep latest)
        df_all = df_all.drop_duplicates(subset="url", keep="last")
        
        # Write to parquet
        try:
            table = pa.Table.from_pandas(df_all, preserve_index=False)
            
            # Write to temporary file first
            tmp_path = path.with_suffix(".tmp")
            pq.write_table(table, tmp_path, compression="zstd")
            
            # Atomic replace
            tmp_path.replace(path)
            
            self.logger.info(f"Saved {len(df_all)} articles to {path.name}")
        except Exception as e:
            self.logger.error(f"Failed to save parquet: {e}")
            raise


# ============================================================================
# Article Scraper
# ============================================================================

class ArticleScraper:
    """Scrape articles from URLs"""
    
    def __init__(self, http_session: HTTPSession, config: ScraperConfig):
        self.http = http_session
        self.config = config
        self.extractor = ContentExtractor()
        self.logger = logging.getLogger(__name__)
    
    def scrape_article(self, url: str, source: str) -> Optional[Article]:
        """Scrape single article"""
        try:
            # Fetch HTML
            status, html = self.http.get(url)
            
            if status != 200 or not html:
                self.logger.debug(f"Failed to fetch {url} (status={status})")
                return None
            
            # Extract content
            title, text, markdown = self.extractor.extract(html, url)
            
            # Validate content
            if not text or len(text.strip()) < self.config.min_content_length:
                self.logger.debug(f"Content too short for {url} ({len(text)} chars)")
                return None
            
            # Detect language
            lang = self._detect_language(url)
            
            # Create article
            article = Article(
                url=url,
                title=title,
                text=text,
                markdown=markdown,
                lang=lang,
                source=source,
                crawled_at=datetime.now(timezone.utc).isoformat(),
                word_count=len(re.findall(r'\w+', text)),
                status="success"
            )
            
            return article
            
        except Exception as e:
            self.logger.error(f"Error scraping {url}: {e}")
            return None
    
    @staticmethod
    def _detect_language(url: str) -> str:
        """Detect language from URL"""
        try:
            path = urlparse(url).path or ""
            parts = path.strip("/").split("/")
            if parts and parts[0].lower() in ["en", "ru", "uz"]:
                return parts[0].lower()
        except Exception:
            pass
        return "uz"


# ============================================================================
# Main Scraper
# ============================================================================

class NewsScraper:
    """Main news scraper orchestrator"""
    
    def __init__(self, config: ScraperConfig):
        self.config = config
        self.logger = setup_logging(config.log_level, log_file="logs/scraper.log")
        self.http = HTTPSession(config)
        self.sitemap_parser = SitemapParser(self.http)
        self.article_scraper = ArticleScraper(self.http, config)
        self.parquet_manager = ParquetManager(config.output_dir)
        
        self.logger.info("=" * 80)
        self.logger.info("News Scraper Initialized")
        self.logger.info(f"Output directory: {config.output_dir}")
        self.logger.info(f"Max workers: {config.max_workers}")
        self.logger.info(f"Max pages: {config.max_pages or 'unlimited'}")
        self.logger.info("=" * 80)
    
    def run(self):
        """Run scraper for all sitemaps"""
        for sitemap_url in self.config.sitemap_urls:
            try:
                self.scrape_sitemap(sitemap_url)
            except Exception as e:
                self.logger.error(f"Failed to process sitemap {sitemap_url}: {e}")
                continue
        
        self.logger.info("=" * 80)
        self.logger.info("Scraping completed!")
        self.logger.info("=" * 80)
    
    def scrape_sitemap(self, sitemap_url: str):
        """Scrape articles from a sitemap"""
        self.logger.info(f"\nProcessing sitemap: {sitemap_url}")
        
        # Parse sitemap
        urls = self.sitemap_parser.parse_sitemap(sitemap_url, self.config.max_pages)
        
        if not urls:
            self.logger.warning(f"No URLs found in sitemap: {sitemap_url}")
            return
        
        # Get source name
        source = urlparse(sitemap_url).netloc
        
        # Filter existing URLs
        if self.config.skip_existing:
            existing_urls = self.parquet_manager.load_existing_urls(source)
            initial_count = len(urls)
            urls = [url for url in urls if url not in existing_urls]
            skipped = initial_count - len(urls)
            if skipped > 0:
                self.logger.info(f"Skipped {skipped} existing URLs, {len(urls)} remaining")
        
        if not urls:
            self.logger.info("No new URLs to scrape")
            return
        
        # Scrape articles
        self.logger.info(f"Scraping {len(urls)} articles with {self.config.max_workers} workers...")
        
        articles = []
        checkpoint_buffer = []
        
        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            # Submit all tasks
            future_to_url = {
                executor.submit(self.article_scraper.scrape_article, url, source): url
                for url in urls
            }
            
            # Process completed tasks with progress bar
            with tqdm(total=len(urls), desc=f"Scraping {source}", unit="article") as pbar:
                for future in as_completed(future_to_url):
                    url = future_to_url[future]
                    
                    try:
                        article = future.result()
                        if article:
                            articles.append(article)
                            checkpoint_buffer.append(article)
                            
                            # Checkpoint save
                            if len(checkpoint_buffer) >= self.config.checkpoint_every:
                                self.parquet_manager.save_articles(checkpoint_buffer, source)
                                checkpoint_buffer = []
                                
                    except Exception as e:
                        self.logger.error(f"Error processing {url}: {e}")
                    
                    pbar.update(1)
        
        # Final save
        if checkpoint_buffer:
            self.parquet_manager.save_articles(checkpoint_buffer, source)
        
        # Summary
        success_count = len(articles)
        self.logger.info(f"Successfully scraped {success_count}/{len(urls)} articles from {source}")
        
        if success_count > 0:
            avg_words = sum(a.word_count for a in articles) / success_count
            self.logger.info(f"Average word count: {avg_words:.0f}")


# ============================================================================
# CLI
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Professional News Scraper - Extract articles from news websites",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Single sitemap
  python news_scraper.py --sitemap https://kun.uz/sitemap.xml

  # Multiple sitemaps
  python news_scraper.py --sitemap https://kun.uz/sitemap.xml https://daryo.uz/sitemap.xml

  # With custom settings
  python news_scraper.py --sitemap https://kun.uz/sitemap.xml --max-pages 1000 --max-workers 20

  # Resume from existing data
  python news_scraper.py --sitemap https://kun.uz/sitemap.xml --skip-existing
        """
    )
    
    parser.add_argument(
        "--sitemap",
        nargs="+",
        required=True,
        help="Sitemap URL(s) to scrape"
    )
    
    parser.add_argument(
        "--output-dir",
        default="data",
        help="Output directory for parquet files (default: data)"
    )
    
    parser.add_argument(
        "--max-pages",
        type=int,
        default=0,
        help="Maximum pages to scrape per sitemap (0 = unlimited)"
    )
    
    parser.add_argument(
        "--max-workers",
        type=int,
        default=10,
        help="Number of concurrent workers (default: 10)"
    )
    
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=100,
        help="Save checkpoint every N articles (default: 100)"
    )
    
    parser.add_argument(
        "--no-skip-existing",
        action="store_true",
        help="Re-scrape existing URLs"
    )
    
    parser.add_argument(
        "--rate-limit",
        type=float,
        default=0.1,
        help="Rate limit in seconds between requests (default: 0.1)"
    )
    
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="Request timeout in seconds (default: 30)"
    )
    
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Logging level (default: INFO)"
    )
    
    args = parser.parse_args()
    
    # Create config
    config = ScraperConfig(
        sitemap_urls=args.sitemap,
        output_dir=args.output_dir,
        max_pages=args.max_pages,
        max_workers=args.max_workers,
        checkpoint_every=args.checkpoint_every,
        skip_existing=not args.no_skip_existing,
        rate_limit=args.rate_limit,
        timeout=args.timeout,
        log_level=args.log_level
    )
    
    # Run scraper
    try:
        scraper = NewsScraper(config)
        scraper.run()
    except KeyboardInterrupt:
        print("\n\nScraping interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n\nFatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()