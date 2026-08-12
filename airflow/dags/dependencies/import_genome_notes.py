import math
from typing import List, Dict, Optional, Any
import asyncio

import aiohttp
from collections import defaultdict

from bs4 import BeautifulSoup,  XMLParsedAsHTMLWarning
from xml.etree import ElementTree as ET
import warnings


TOKEN_URL = "https://wellcomeopenresearch.org/api/token"
GATEWAY_URL = "https://wellcomeopenresearch.org/content?gatewayIds[0]=231&gatewayIds[1]=239&gatewayIds[2]=240&gatewayIds[3]=255&gatewayIds[4]=241"
EBI_API_BASE = "https://www.ebi.ac.uk/ena/browser/api/xml/"
MAX_CONCURRENT_REQUESTS = 1
REQUEST_TIMEOUT = 30.0
CLIENT_TIMEOUT = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)

# The Wellcome Open Research API rate-limits bursts of requests with HTTP 429,
# and once tripped it keeps returning 429 for a cooldown window well over a
# minute. Every fetch() in this module hits that host, so we (1) proactively
# space requests apart to stay under the limit and (2) retry transient statuses
# with exponential backoff (honouring Retry-After) long enough to ride out a
# cooldown. Tune REQUEST_DELAY_S up if 429s still appear, down if the task is
# too slow.
RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
REQUEST_DELAY_S = 1.0
MAX_RETRIES = 6
BACKOFF_BASE_S = 2.0
BACKOFF_MAX_S = 60.0


def clean_study_id(study_id: str) -> str:
    if "?" in study_id:
        return study_id.split("?")[0]
    if "&" in study_id:
        return study_id.split("&")[0]
    return study_id


def _retry_delay(response_headers: Any, attempt: int) -> float:
    retry_after = response_headers.get("Retry-After") if response_headers else None
    if retry_after and str(retry_after).isdigit():
        return float(retry_after)
    return min(BACKOFF_BASE_S * (2 ** attempt), BACKOFF_MAX_S)


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    headers: Optional[Dict[str, str]] = None,
    force_text: bool = False,
) -> Any:
    # Proactively throttle: space every request apart so we stay under the
    # API's rate limit rather than tripping it and relying on retries.
    if REQUEST_DELAY_S:
        await asyncio.sleep(REQUEST_DELAY_S)

    last_exc: Optional[Exception] = None
    for attempt in range(MAX_RETRIES):
        try:
            async with session.get(url, headers=headers) as response:
                if response.status in RETRYABLE_STATUSES and attempt < MAX_RETRIES - 1:
                    delay = _retry_delay(response.headers, attempt)
                    print(
                        f"{response.status} for {url}; retrying in {delay:.1f}s "
                        f"(attempt {attempt + 1}/{MAX_RETRIES})"
                    )
                    await asyncio.sleep(delay)
                    continue
                response.raise_for_status()
                # HTML file endpoints sometimes mislabel content as JSON, so
                # callers that want raw text force it rather than trusting
                # content_type.
                if force_text:
                    return await response.text()
                return (
                    await response.json()
                    if response.content_type == "application/json"
                    else await response.text()
                )
        except aiohttp.ClientResponseError:
            # A raised HTTP status here is permanent (retryable statuses are
            # handled above and looped): 403/404/401 etc. won't change on retry,
            # so fail fast instead of burning the whole backoff schedule per URL.
            raise
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            # Transient transport failures (connection reset, timeout) — retry.
            last_exc = e
            if attempt >= MAX_RETRIES - 1:
                break
            delay = min(BACKOFF_BASE_S * (2 ** attempt), BACKOFF_MAX_S)
            print(
                f"Request error for {url}: {e}; retrying in {delay:.1f}s "
                f"(attempt {attempt + 1}/{MAX_RETRIES})"
            )
            await asyncio.sleep(delay)
    raise last_exc or RuntimeError(f"Exhausted retries fetching {url}")


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
        # Reuse fetch's throttle/retry, but HTML file endpoints can mislabel the
        # response as JSON, so always read the body as text.
        return await fetch(session, url, force_text=True)
    except Exception as e:
        # A single unreachable page must not fail the whole 1810-article batch;
        # parse_genome_notes skips falsy html_text.
        print(f"Failed to fetch {url}: {e}")
        return None


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
        # Newline-terminated progress every 100 articles: a "\r" spinner is
        # invisible in Cloud Logging (records split on newlines), making this
        # CPU-bound parse loop look hung.
        if index % 100 == 0 or index + 1 == len(articles):
            print(f"Collecting study IDs from article {index + 1}/{len(articles)}")
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
                if processed_count % 100 == 0:
                    print(f"Processed {processed_count} genome notes")
                genome_notes[tax_id].append(genome_note)

    print(
        f"\nCompleted processing {processed_count} genome notes across {len(genome_notes)} taxa"
    )
    return genome_notes


async def main():
    async with aiohttp.ClientSession(timeout=CLIENT_TIMEOUT) as session:
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

            # Process the combined articles
            genome_notes = await parse_genome_notes(session, articles)
            print(f"Processing complete! Total entries: {len(genome_notes)}")
            return genome_notes
        except Exception as e:
            # Never swallow the error into a None return: the caller does
            # genome_notes.items() and would otherwise crash with a misleading
            # AttributeError that hides the real failure.
            print(f"An error occurred during processing: {e}")
            raise

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

    async with aiohttp.ClientSession(timeout=CLIENT_TIMEOUT) as session:
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

