"""
Al-Mudeer - Link Reader Service
Extracts and scrapes content from URLs to provide context to the LLM.
"""

import re
import asyncio
from typing import List, Optional
import httpx
from httpx import HTTPStatusError
from bs4 import BeautifulSoup
from logging_config import get_logger

logger = get_logger(__name__)

# Regular expression for finding URLs
URL_PATTERN = re.compile(
    r'http[s]?://(?:[a-zA-Z]|[0-9]|[$\-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
)

async def extract_and_scrape_links(text: str, limit: int = 1) -> str:
    """
    Find URLs in text, scrape them, and return formatted content.
    
    Args:
        text: The message text containing URLs
        limit: Max number of URLs to scrape (default 1 to avoid context overflow)
        
    Returns:
        Formatted string with scraped content or empty string
    """
    if not text:
        return ""
        
    urls = URL_PATTERN.findall(text)
    if not urls:
        return ""
        
    # Unique URLs only, maintaining order
    urls = list(dict.fromkeys(urls))[:limit]
    
    scraped_content = []
    
    async with httpx.AsyncClient(timeout=10.0, follow_redirects=True) as client:
        for url in urls:
            try:
                # Basic safety check (very simple)
                if "localhost" in url or "127.0.0.1" in url:
                    continue
                    
                logger.info(f"Scraping URL: {url}")
                
                # Fetch content
                response = await client.get(
                    url, 
                    headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"}
                )
                response.raise_for_status()
                
                # Parse HTML
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Remove script and style elements
                for script in soup(["script", "style", "nav", "footer", "header"]):
                    script.extract()
                
                # Get text
                text = soup.get_text(separator='\n')
                
                # Clean text (remove empty lines and excessive whitespace)
                lines = (line.strip() for line in text.splitlines())
                chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
                clean_text = '\n'.join(chunk for chunk in chunks if chunk)
                
                # Limit content length (approx 2000 chars)
                if len(clean_text) > 2000:
                    clean_text = clean_text[:2000] + "...\n[تم قص باقي المحتوى]"
                
                title = soup.title.string.strip() if soup.title else "No Title"
                
                scraped_content.append(f"--- محتوى الرابط: {url} ---\nالعنوان: {title}\n\n{clean_text}\n------------------")
                
            except HTTPStatusError as e:
                if e.response.status_code == 404:
                    logger.info(f"Skipping unreachable URL (404): {url}")
                else:
                    logger.warning(f"HTTP error scraping {url}: {e}")
            except Exception as e:
                logger.warning(f"Failed to scrape {url}: {e}")
                
    return "\n\n".join(scraped_content)
