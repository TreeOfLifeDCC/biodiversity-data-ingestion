import math
from typing import List, Dict, Optional, Any
import asyncio

import aiohttp
from collections import defaultdict

from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from xml.etree import ElementTree as ET
import warnings
import re
import html
import requests
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, urlunparse


TOKEN_URL = "https://wellcomeopenresearch.org/api/token"
GATEWAY_URL = "https://wellcomeopenresearch.org/content?gatewayIds[0]=231&gatewayIds[1]=239&gatewayIds[2]=240&gatewayIds[3]=255&gatewayIds[4]=241"
EBI_API_BASE = "https://www.ebi.ac.uk/ena/browser/api/xml/"
MAX_CONCURRENT_REQUESTS = 1
REQUEST_TIMEOUT = 30.0

# RIO genome notes collection (RIO Journal topical collection)
RIO_BASE_URL = "https://riojournal.com/browse_topical_collection_documents.php?journal_name=rio&collection_id=280&lang=&journal_id=17&p=0"
ERGA_API_URL = "https://portal.erga-biodiversity.eu/api/data_portal"


def clean_study_id(study_id: str) -> str:
    if "?" in study_id:
        return study_id.split("?")[0]
    if "&" in study_id:
        return study_id.split("&")[0]
    return study_id


async def fetch(
    session: aiohttp.ClientSession, url: str, headers: Optional[Dict[str, str]] = None
) -> Any:
    try:
        async with session.get(url, headers=headers) as response:
            response.raise_for_status()
            return (
                await response.json()
                if response.content_type == "application/json"
                else await response.text()
            )
    except Exception as e:
        raise


async def get_auth_token(session: aiohttp.ClientSession) -> str:

    try:
        response = await fetch(
            session, TOKEN_URL, headers={"content-type": "application/json"}
        )
        return response["token"]
    except Exception as e:
        raise


async def extract_article_versions(
    session: aiohttp.ClientSession, article_ids: List[str], headers: Dict[str, str]
) -> List[str]:
    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_with_semaphore(article_id):
        async with semaphore:
            return await fetch(
                session,
                f"https://wellcomeopenresearch.org/api/articles?id={article_id}&publishedOnly=true",
                headers,
            )

    tasks = [fetch_with_semaphore(article_id) for article_id in article_ids]
    responses = await asyncio.gather(*tasks)
    return [resp[0]["versionIds"][0] for resp in responses]


async def get_articles_bulk(
    session: aiohttp.ClientSession,
    article_ids: List[str],
    headers: Dict[str, str],
    batch_size=20,
) -> tuple:
    all_responses = []
    failed_ids = []

    for i in range(0, len(article_ids), batch_size):
        batch_ids = article_ids[i : i + batch_size]
        id_params = "&".join([f"id={article_id}" for article_id in batch_ids])
        url = f"https://wellcomeopenresearch.org/api/versions?{id_params}&publishedOnly=true"

        try:
            response = await fetch(session, url, headers)
            # Check if the response is valid
            if response is not None:
                all_responses.extend(response)
            else:
                print(f"Received None response for batch {i // batch_size + 1}")
                failed_ids.extend(batch_ids)
        except Exception as e:
            print(f"Failed to fetch batch {i // batch_size + 1}: {e}")
            print(len(batch_ids))
            failed_ids.extend(batch_ids)

    return all_responses, failed_ids


async def get_articles_sequential(
    session: aiohttp.ClientSession, article_ids: List[str], headers: Dict[str, str]
) -> List[Any]:
    """
    Fetch articles sequentially one by one for IDs that failed in batch processing.
    """
    successful_responses = []

    for article_id in article_ids:
        print(f"Attempting sequential fetch for article ID: {article_id}")
        url = f"https://wellcomeopenresearch.org/api/versions?id={article_id}&publishedOnly=true"

        try:
            response = await fetch(session, url, headers)
            if response is not None:
                successful_responses.extend(response)
                print(f"Successfully fetched article ID: {article_id}")
            else:
                print(
                    f"Failed to fetch article ID: {article_id} (received None response)"
                )
        except Exception as e:
            print(f"Failed to fetch article ID: {article_id}: {e}")

    return successful_responses


async def fetch_study_data_bulk(
    session: aiohttp.ClientSession, study_ids: List[str]
) -> Dict[str, Optional[str]]:
    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    results = {}

    async def fetch_batch_with_semaphore(batch_ids):
        batch = ",".join(batch_ids)
        url = f"{EBI_API_BASE}{batch}"
        async with semaphore:
            try:
                async with session.get(url) as response:
                    xml_data = await response.text()
                    return parse_study_xml(xml_data, batch)
            except Exception as e:
                print(f"Failed to fetch study data for batch {batch}: {e}")
                return {}

    # Create batches of 20 study IDs
    batches = [study_ids[i : i + 20] for i in range(0, len(study_ids), 20)]

    # Fetch all batches concurrently with limited concurrency
    batch_results = await asyncio.gather(
        *[fetch_batch_with_semaphore(batch) for batch in batches]
    )

    # Combine all results
    for batch_result in batch_results:
        results.update(batch_result)

    return results


def parse_study_xml(xml_data: str, study_ids: str) -> Dict[str, str]:
    tax_id_map = {}
    study_ids_list = study_ids.split(",") if isinstance(study_ids, str) else study_ids
    try:
        root = ET.fromstring(xml_data)
        for project, study_id in zip(root.findall("PROJECT"), study_ids_list):
            tax_id = None
            try:
                tax_id = (
                    project.find("UMBRELLA_PROJECT")
                    .find("ORGANISM")
                    .find("TAXON_ID")
                    .text
                )
            except AttributeError:
                try:
                    tax_id = (
                        project.find("SUBMISSION_PROJECT")
                        .find("ORGANISM")
                        .find("TAXON_ID")
                        .text
                    )
                except AttributeError:
                    print(f"Tax ID not found for {study_id}")
            if tax_id:
                tax_id_map[study_id] = tax_id
    except ET.ParseError as e:
        print(f"XML Parsing error: {e}")

    return tax_id_map


async def fetch_html(session: aiohttp.ClientSession, url: str) -> Optional[str]:
    try:
        async with session.get(url) as response:
            return await response.text()
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        raise


async def parse_genome_notes(
    session: aiohttp.ClientSession, articles: List[Any]
) -> Dict[str, List[Dict[str, Any]]]:
    genome_notes = defaultdict(list)
    visited_studies = set()

    # Create a semaphore to limit concurrent requests
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_html_with_semaphore(url):
        async with semaphore:
            return await fetch_html(session, url)

    # Fetch all HTML pages in parallel with limited concurrency
    print(f"Fetching HTML for {len(articles)} articles...")
    tasks = [fetch_html_with_semaphore(article["htmlUrl"]) for article in articles]
    html_texts = await asyncio.gather(*tasks)

    study_id_map = {}
    for index, (article, html_text) in enumerate(zip(articles, html_texts)):
        print(
            f"Collecting study IDs from article {index + 1}/{len(articles)}",
            end="\r",
            flush=True,
        )
        if not html_text:
            continue
        warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)
        soup = BeautifulSoup(html_text, "html.parser")
        article_study_ids = []

        for link in soup.find_all("a", href=True):
            href = link["href"]

            # Try all possible formats for extracting study ID
            study_id_1 = clean_study_id(href.split(":")[-1]) if ":" in href else ""
            study_id_2 = clean_study_id(href.split("/")[-1]) if "/" in href else ""
            study_id_3 = clean_study_id(href.split("%3D")[-1]) if "%3D" in href else ""
            study_id_4 = (
                clean_study_id(href.split("/")[-2])
                if "/" in href and len(href.split("/")) > 2
                else ""
            )

            chosen_study_id = None
            if study_id_1.startswith("PRJ"):
                chosen_study_id = study_id_1
            elif study_id_2.startswith("PRJ"):
                chosen_study_id = study_id_2
            elif study_id_3.startswith("PRJ"):
                chosen_study_id = study_id_3
            elif study_id_4.startswith("PRJ"):
                chosen_study_id = study_id_4

            if chosen_study_id and chosen_study_id not in visited_studies:
                visited_studies.add(chosen_study_id)
                article_study_ids.append(chosen_study_id)

        if article_study_ids:
            study_id_map[article["htmlUrl"]] = {
                "study_ids": article_study_ids,
                "article_data": article,
                "html_text": html_text,  # Store HTML text to avoid lookup later
            }

    # Fetch all study data in bulk
    print(f"Fetching data for {len(visited_studies)} unique study IDs...")
    study_data = await fetch_study_data_bulk(session, list(visited_studies))

    # Process genome notes
    processed_count = 0
    for html_url, study_info in study_id_map.items():
        html_text = study_info["html_text"]
        soup = BeautifulSoup(html_text, "html.parser")
        article_data = study_info["article_data"]

        for study_id in study_info["study_ids"]:
            tax_id = study_data.get(study_id)

            if tax_id:
                # Find the figure URI
                figure_uri = "#"
                for img in soup.find_all("img"):
                    src = img.get("src", "")
                    if src and "figure1.gif" in src:
                        figure_uri = src
                        break

                # Find the caption
                caption = None
                for cap in soup.find_all("div", {"class": "caption"}):
                    if cap.h3 and "Figure 1" in cap.h3.text:
                        caption = cap.h3.text
                        break

                genome_note = {
                    "tax_id": tax_id,
                    "study_id": study_id,
                    "url": article_data["pdfUrl"].split("/pdf")[0],
                    "citeURL": f"https://doi.org/{article_data['doi']}",
                    "title": article_data["title"],
                    "abstract": article_data["abstractText"],
                    "figureURI": figure_uri,
                    "caption": caption,
                }

                processed_count += 1
                print(f"Processed {processed_count} genome notes", end="\r", flush=True)
                genome_notes[tax_id].append(genome_note)

    print(
        f"\nCompleted processing {processed_count} genome notes across {len(genome_notes)} taxa"
    )
    return genome_notes


def get_article_links_from_headlines(url: str) -> List[Dict[str, Any]]:
    """
    Parse a RIO topical collection page and return basic article metadata
    (URL, title, provisional ID text, figure URI).
    """
    resp = requests.get(url, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")
    items_by_url: Dict[str, Dict[str, Any]] = {}

    article_containers = soup.find_all("div", class_="article")
    for article in article_containers:
        headline_div = article.find("div", class_="articleHeadline")
        if not headline_div:
            continue

        img_src = ""
        image_holder = article.find("div", class_="articleCoverImageHolder")
        if image_holder:
            for img in image_holder.find_all("img", src=True):
                img_src = urljoin(url, img["src"])

        for a in headline_div.find_all("a", href=True):
            href = a["href"]
            raw_title = a.get_text(" ", strip=True)
            text = " ".join(raw_title.split())
            if not text:
                continue

            em_tag = a.find("em")
            em_text = em_tag.get_text(strip=True) if em_tag else ""

            i_tag = a.find("i")
            i_text = i_tag.get_text(strip=True) if i_tag else ""

            chosen_id = em_text or i_text
            if not chosen_id:
                matches = re.findall(r"\(([^)]+)\)", text)
                if matches:
                    chosen_id = matches[-1].strip()

            full_url = urljoin(url, href)
            items_by_url[full_url] = {
                "url": full_url,
                "title": text,
                "id": chosen_id,
                "figureURI": img_src,
            }

    return list(items_by_url.values())


def build_page_url(base_url: str, page_index: int) -> str:
    """
    Return a copy of base_url with the query parameter p set to page_index.
    """
    parsed = urlparse(base_url)
    query = parse_qs(parsed.query)
    query["p"] = [str(page_index)]
    new_query = urlencode(query, doseq=True)
    return urlunparse(parsed._replace(query=new_query))


def get_number_of_papers_in_press_pages(soup: BeautifulSoup) -> int:
    """
    Look for element with id 'bookInfo', find the <p> containing
    'Papers in press:' and read the numeric value inside its <span>.
    That value controls how many times we call the RIO_BASE_URL (pages).
    """
    book_info = soup.find(id="bookInfo")
    if not book_info:
        return 1

    p_tag = book_info.find(
        lambda tag: tag.name == "p" and "Papers in press:" in tag.get_text(strip=True)
    )
    if not p_tag:
        return 1

    span = p_tag.find("span")
    if not span:
        return 1

    raw_text = span.get_text(strip=True)
    digits = "".join(ch for ch in raw_text if ch.isdigit())
    if not digits:
        return 1

    value = int(digits)
    return max(1, value)


def clean_abstract_text(raw: str) -> str:
    """
    Normalise abstract text by stripping ALL JATS/XML/HTML tags.
    """
    if not raw:
        return ""
    raw = html.unescape(raw).strip()
    if not raw:
        return ""

    no_tags = re.sub(r"<[^>]+>", "", raw, flags=re.DOTALL)
    cleaned = " ".join(no_tags.split())
    return cleaned


def get_erga_id_or_same(search_value: str) -> Optional[str]:
    """
    Call the ERGA data_portal API with `search_value` and return its tax_id/_id.
    """
    if not search_value:
        return None

    params = {
        "limit": 15,
        "offset": 0,
        "search": search_value,
        "sort": "currentStatus:asc",
        "current_class": "kingdom",
    }

    try:
        resp = requests.get(ERGA_API_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return None

    results = data.get("results") or []
    if not results:
        return None

    first = results[0]
    src = first.get("_source", {}) or {}
    erga_id = src.get("tax_id") or first.get("_id")
    if not erga_id:
        return None

    return str(erga_id)


def fetch_abstract_from_crossref(doi: str) -> str:
    """
    Try to fetch an abstract using the CrossRef API for a given DOI.
    """
    if not doi:
        return ""

    api_url = f"https://api.crossref.org/works/{doi}"
    headers = {"User-Agent": "articles-scraper/1.0 (mailto:example@example.com)"}
    try:
        resp = requests.get(api_url, headers=headers, timeout=20)
        resp.raise_for_status()
    except Exception:
        return ""

    try:
        message = resp.json().get("message", {})
    except Exception:
        return ""

    raw_abstract = message.get("abstract") or ""
    return clean_abstract_text(raw_abstract)


def fetch_abstract(url: str) -> str:
    """
    Fetch an article page and try to extract its abstract text.
    """
    parsed = urlparse(url)

    if parsed.netloc == "doi.org":
        doi = parsed.path.lstrip("/")
        return fetch_abstract_from_crossref(doi)

    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception:
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    for meta_name in ("citation_abstract", "dc.description", "DC.Description", "description"):
        meta = soup.find("meta", attrs={"name": meta_name})
        if meta and meta.get("content"):
            return clean_abstract_text(meta["content"])

    for prop_name in ("og:description", "twitter:description"):
        meta = soup.find("meta", attrs={"property": prop_name})
        if meta and meta.get("content"):
            return clean_abstract_text(meta["content"])

    candidates = [
        soup.select_one("div.abstract"),
        soup.select_one("section.abstract"),
        soup.select_one("div#abstract"),
        soup.select_one("section#abstract"),
        soup.select_one("div.articleAbstract"),
        soup.select_one("section.article-abstract"),
    ]
    for node in candidates:
        if node:
            text = node.get_text(" ", strip=True)
            if text:
                return clean_abstract_text(text)

    heading = soup.find(
        lambda tag: tag.name in ("h1", "h2", "h3", "h4")
        and "abstract" in tag.get_text(strip=True).lower()
    )
    if heading:
        parts: List[str] = []
        for sib in heading.find_all_next():
            if sib.name in ("h1", "h2", "h3", "h4") and sib is not heading:
                break
            if sib.name in ("p", "div"):
                txt = sib.get_text(" ", strip=True)
                if txt:
                    parts.append(txt)
        if parts:
            return clean_abstract_text(" ".join(parts))

    label_node = soup.find(
        lambda tag: tag.name in ("p", "div", "span", "strong")
        and tag.get_text(strip=True).lower() == "abstract"
    )
    if label_node:
        if label_node.name in ("p", "div"):
            next_p = label_node.find_next_sibling("p")
            if next_p:
                txt = next_p.get_text(" ", strip=True)
                if txt:
                    return clean_abstract_text(txt)
        parent = label_node.parent
        if parent and parent.name in ("p", "div"):
            next_p = parent.find_next_sibling("p")
            if next_p:
                txt = next_p.get_text(" ", strip=True)
                if txt:
                    return clean_abstract_text(txt)

    para = soup.find(
        lambda tag: tag.name == "p"
        and "abstract" in tag.get_text(strip=True).lower()
    )
    if para:
        raw = para.get_text(" ", strip=True)
        lower = raw.lower()
        idx = lower.find("abstract")
        if idx != -1:
            cleaned = raw[idx + len("abstract") :].lstrip(" :.-\u2013")
            if cleaned:
                return clean_abstract_text(cleaned)
        return clean_abstract_text(raw)

    return ""


def fetch_rio_genome_notes() -> Dict[str, List[Dict[str, Any]]]:
    """
    Scrape the RIO Journal topical collection and build genome notes
    records keyed by ERGA tax_id, matching the existing genome_notes structure.
    """
    genome_notes: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    try:
        resp = requests.get(RIO_BASE_URL, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"Failed to fetch RIO base page: {e}")
        return genome_notes

    soup = BeautifulSoup(resp.text, "html.parser")
    total_pages = get_number_of_papers_in_press_pages(soup)

    all_items: List[Dict[str, Any]] = []
    for page_index in range(total_pages):
        page_url = build_page_url(RIO_BASE_URL, page_index)
        try:
            page_items = get_article_links_from_headlines(page_url)
            all_items.extend(page_items)
        except Exception as e:
            print(f"Failed to parse RIO page {page_url}: {e}")

    final_items: List[Dict[str, Any]] = []
    for item in all_items:
        url = item.get("url")
        if not url:
            continue

        item["abstract"] = fetch_abstract(url)
        erga_id = get_erga_id_or_same(item.get("id"))  # maps to tax_id/_id in ERGA
        if not erga_id:
            continue

        genome_note = {
            "tax_id": erga_id,
            "study_id": None,
            "url": url,
            "citeURL": url,
            "title": item.get("title", ""),
            "abstract": item.get("abstract", ""),
            "figureURI": item.get("figureURI") or "#",
            "caption": None,
        }
        genome_notes[erga_id].append(genome_note)
        final_items.append(item)

    missing_abstracts = [it for it in final_items if not it.get("abstract")]
    if missing_abstracts:
        print("RIO articles missing abstract:")
        for it in missing_abstracts:
            print(" -", it.get("url", ""))

    return genome_notes


async def main():
    async with aiohttp.ClientSession() as session:
        try:
            headers = {
                "f1000-authbearer": await get_auth_token(session),
                "content-type": "application/json",
            }

            # Fetch article IDs
            article_ids = (
                await get_all_ids_async(GATEWAY_URL))
            article_version_ids = await extract_article_versions(
                session, article_ids, headers
            )
            print(f"Total article version IDs: {len(article_version_ids)}")

            # Fetch articles in bulk
            articles, failed_ids = await get_articles_bulk(
                session, article_version_ids, headers
            )
            print(f"Successfully fetched {len(articles)} articles in bulk")
            # If there are any failed IDs, try to fetch them sequentially
            if failed_ids:
                print(
                    f"Attempting to fetch {len(failed_ids)} failed articles sequentially..."
                )
                sequential_articles = await get_articles_sequential(
                    session, failed_ids, headers
                )
                if sequential_articles:
                    print(
                        f"Successfully fetched {len(sequential_articles)} articles sequentially"
                    )
                    articles.extend(sequential_articles)
                else:
                    print("No articles were fetched sequentially")

            # Process the combined articles from Wellcome Open Research
            genome_notes = await parse_genome_notes(session, articles)

            # Enrich with additional genome notes from the RIO topical collection
            rio_genome_notes = fetch_rio_genome_notes()
            for tax_id, notes in rio_genome_notes.items():
                genome_notes[tax_id].extend(notes)

            print(f"Processing complete! Total entries (including RIO): {len(genome_notes)}")
            return genome_notes
        except Exception as e:
            print(f"An error occurred during processing: {e}")

async def fetch_page(session, base_url, params):
    """Fetch a single page asynchronously and return its JSON content."""
    async with session.get(base_url, params=params) as resp:
        resp.raise_for_status()
        return await resp.json()

async def get_all_ids_async(base_url: str, content_type: str = "ARTICLE", show: int = 100, concurrency: int = 10):
    """
    Asynchronously fetch all content IDs from the Wellcome Open Research API.

    Args:
        base_url (str): The API base URL (e.g. 'https://wellcomeopenresearch.org/content')
        content_type (str): Content type filter (default: 'ARTICLE')
        show (int): Number of records per page (default: 20)
        concurrency (int): Max number of pages fetched concurrently.

    Returns:
        list[str]: A list of all content IDs.
    """
    params = {"contentTypes": content_type, "page": 1, "show": show}

    async with aiohttp.ClientSession() as session:
        print("Fetching first page to detect total...")
        first_page_data = await fetch_page(session, base_url, params)

        total = first_page_data.get("total", 0)
        if total == 0:
            print("No data found.")
            return []

        total_pages = math.ceil(total / show)
        print(f"Total records: {total}, pages: {total_pages}")

        # Detect where the content list is stored
        items = first_page_data.get("contents") or first_page_data.get("content") or first_page_data.get("results") or []
        all_ids = [item.get("id") or item.get("uuid") for item in items if isinstance(item, dict)]

        # Prepare page tasks
        pages = list(range(2, total_pages + 1))

        # Semaphore to control concurrency
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_with_semaphore(page_num):
            async with semaphore:
                p = {"contentTypes": content_type, "page": page_num, "show": show}
                data = await fetch_page(session, base_url, p)
                items = data.get("contents") or data.get("content") or data.get("results") or []
                return [item.get("id") or item.get("uuid") for item in items if isinstance(item, dict)]

        # Fetch all pages concurrently
        tasks = [asyncio.create_task(fetch_with_semaphore(page)) for page in pages]
        print(f"Fetching {len(tasks)} remaining pages asynchronously...")
        results = await asyncio.gather(*tasks, return_exceptions=False)

        # Flatten all results
        for r in results:
            all_ids.extend(r)

        print(f"✅ Collected {len(all_ids)} IDs in total.")
        return all_ids

