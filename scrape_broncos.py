#!/usr/bin/env python3
"""
Web scraper for Denver Broncos season Wikipedia pages.

This script fetches Wikipedia pages for Denver Broncos seasons
and saves the HTML content locally for analysis. It follows web scraping
best practices including proper user agent headers, error handling, 
timeout settings, and respectful delays.

Usage:
    # Scrape 2024 season (default)
    python scrape_broncos.py
    
    # Scrape specific season
    python scrape_broncos.py --season 2025
    
    # Force re-download existing file
    python scrape_broncos.py --season 2024 --force
    
    # Scrape multiple seasons
    python scrape_broncos.py --season 2023 --season 2024 --season 2025

Features:
    - Multi-season support with dynamic URL construction
    - File existence checking with skip functionality
    - Robust error handling for network issues
    - Proper Wikipedia-friendly user agent
    - Progress indicators and file size reporting
    - Handles encoding issues gracefully
    - Creates necessary directories automatically
    - Command line interface with season and force options
"""

import argparse
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, Timeout, ConnectionError
from urllib3.util.retry import Retry

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('scrape_broncos.log', mode='a')
    ]
)

logger = logging.getLogger(__name__)

class BroncosWikiScraper:
    """
    Web scraper for Denver Broncos Wikipedia content.
    
    Handles fetching and storing Wikipedia pages for different seasons with 
    proper error handling, rate limiting, and web scraping etiquette.
    
    Supports:
    - Multi-season scraping
    - File existence checking
    - Dynamic URL construction
    - Configurable output paths
    """
    
    # Wikipedia-friendly user agent
    USER_AGENT = (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 "
        "fantasy-analysis-scraper/1.0"
    )
    
    # Wikipedia URL template for Denver Broncos seasons
    URL_TEMPLATE = "https://en.wikipedia.org/wiki/{season}_Denver_Broncos_season"
    
    # Default output directory and season
    DEFAULT_OUTPUT_DIR = Path("data_source")
    DEFAULT_SEASON = 2024
    
    def __init__(
        self,
        season: int = None,
        output_dir: Optional[Path] = None,
        filename: Optional[str] = None,
        timeout: int = 30,
        max_retries: int = 3,
        force_overwrite: bool = False,
    ):
        """
        Initialize the scraper with configuration options.
        
        Args:
            season: Season year to scrape (e.g., 2024, 2025)
            output_dir: Directory to save scraped content
            filename: Name for the saved HTML file (auto-generated if None)
            timeout: Request timeout in seconds
            max_retries: Maximum number of retry attempts
            force_overwrite: Whether to overwrite existing files
        """
        self.season = season or self.DEFAULT_SEASON
        self.output_dir = output_dir or self.DEFAULT_OUTPUT_DIR
        self.timeout = timeout
        self.max_retries = max_retries
        self.force_overwrite = force_overwrite
        
        # Generate filename based on season if not provided
        self.filename = filename or f"{self.season}_denver_broncos_wikipedia.html"
        
        # Construct URL for the specific season
        self.target_url = self.URL_TEMPLATE.format(season=self.season)
        
        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Configure requests session with retry strategy
        self.session = self._create_session()
        
        logger.info(f"Initialized BroncosWikiScraper for {self.season} season")
        logger.info(f"Output directory: {self.output_dir.absolute()}")
        logger.info(f"Target URL: {self.target_url}")
        logger.info(f"Output filename: {self.filename}")
    
    def _create_session(self) -> requests.Session:
        """
        Create a requests session with proper configuration.
        
        Returns:
            Configured requests session with retry strategy
        """
        session = requests.Session()
        
        # Configure retry strategy
        retry_strategy = Retry(
            total=self.max_retries,
            backoff_factor=1,  # Wait 1, 2, 4 seconds between retries
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"]
        )
        
        # Mount adapter with retry strategy
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        session.headers.update({
            'User-Agent': self.USER_AGENT,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })
        
        return session
    
    def check_file_exists(self) -> Tuple[bool, Optional[Path]]:
        """
        Check if the output file already exists.
        
        Returns:
            Tuple of (file_exists, file_path)
        """
        output_path = self.output_dir / self.filename
        exists = output_path.exists()
        
        if exists:
            file_size = output_path.stat().st_size
            modified_time = datetime.fromtimestamp(output_path.stat().st_mtime)
            logger.info(f"File already exists: {output_path.name}")
            logger.info(f"File size: {file_size:,} bytes")
            logger.info(f"Last modified: {modified_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        return exists, output_path if exists else None
    
    def fetch_page(self) -> Optional[str]:
        """
        Fetch the Wikipedia page content.
        
        Returns:
            HTML content as string, or None if fetch failed
            
        Raises:
            RequestException: If all retry attempts fail
        """
        logger.info(f"Fetching page: {self.target_url}")
        
        try:
            # Add small delay to be respectful to Wikipedia servers
            time.sleep(1)
            
            response = self.session.get(self.target_url, timeout=self.timeout)
            response.raise_for_status()
            
            # Log response details
            logger.info(f"Successfully fetched page")
            logger.info(f"Response status: {response.status_code}")
            logger.info(f"Content-Type: {response.headers.get('content-type', 'unknown')}")
            logger.info(f"Content-Length: {len(response.content):,} bytes")
            
            # Handle encoding properly
            response.encoding = response.apparent_encoding
            html_content = response.text
            
            logger.info(f"Decoded content length: {len(html_content):,} characters")
            return html_content
            
        except Timeout as e:
            logger.error(f"Request timed out after {self.timeout} seconds: {e}")
            raise
        except ConnectionError as e:
            logger.error(f"Connection error: {e}")
            raise
        except RequestException as e:
            logger.error(f"Request failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response status: {e.response.status_code}")
                logger.error(f"Response headers: {dict(e.response.headers)}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during fetch: {e}")
            raise
    
    def save_html(self, html_content: str) -> Path:
        """
        Save HTML content to local file.
        
        Args:
            html_content: HTML content to save
            
        Returns:
            Path to the saved file
            
        Raises:
            IOError: If file write fails
        """
        output_path = self.output_dir / self.filename
        
        logger.info(f"Saving HTML content to: {output_path.absolute()}")
        
        try:
            # Write HTML content with UTF-8 encoding
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(html_content)
            
            # Get file size for reporting
            file_size = output_path.stat().st_size
            logger.info(f"Successfully saved {file_size:,} bytes to {output_path.name}")
            
            return output_path
            
        except IOError as e:
            logger.error(f"Failed to save HTML content: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during save: {e}")
            raise
    
    def add_metadata(self, output_path: Path) -> None:
        """
        Add metadata file alongside the HTML content.
        
        Args:
            output_path: Path to the saved HTML file
        """
        metadata_path = output_path.with_suffix('.metadata.json')
        
        metadata = {
            'season': self.season,
            'url': self.target_url,
            'scraped_at': datetime.now().isoformat(),
            'file_size_bytes': output_path.stat().st_size,
            'filename': output_path.name,
            'user_agent': self.USER_AGENT,
            'scraper_version': '2.0'
        }
        
        try:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2)
            
            logger.info(f"Metadata saved to: {metadata_path.name}")
            
        except Exception as e:
            logger.warning(f"Failed to save metadata: {e}")
    
    def scrape(self) -> Optional[Path]:
        """
        Main scraping method that orchestrates the entire process.
        
        Returns:
            Path to saved HTML file, or None if scraping failed
        """
        try:
            logger.info(f"Starting Denver Broncos {self.season} season Wikipedia scraping")
            
            # Check if file already exists
            file_exists, existing_path = self.check_file_exists()
            
            if file_exists and not self.force_overwrite:
                logger.info(f"File already exists and force_overwrite is False. Skipping download.")
                logger.info(f"Use force_overwrite=True or --force flag to re-download.")
                return existing_path
            
            if file_exists and self.force_overwrite:
                logger.info(f"File exists but force_overwrite is True. Re-downloading...")
            
            # Fetch the page
            html_content = self.fetch_page()
            if not html_content:
                logger.error("Failed to fetch page content")
                return None
            
            # Save the HTML content
            output_path = self.save_html(html_content)
            
            # Add metadata
            self.add_metadata(output_path)
            
            logger.info(f"Scraping completed successfully!")
            logger.info(f"File saved at: {output_path.absolute()}")
            
            return output_path
            
        except Exception as e:
            logger.error(f"Scraping failed: {e}")
            return None
        
        finally:
            # Clean up session
            if hasattr(self, 'session'):
                self.session.close()


def parse_args() -> argparse.Namespace:
    """
    Parse command line arguments.
    
    Returns:
        Parsed command line arguments
    """
    parser = argparse.ArgumentParser(
        description="Scrape Denver Broncos season Wikipedia pages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                           # Scrape 2024 season (default)
  %(prog)s --season 2025             # Scrape 2025 season
  %(prog)s --season 2024 --force     # Force re-download 2024 season
  %(prog)s -s 2023 -s 2024 -s 2025   # Scrape multiple seasons
        """
    )
    
    parser.add_argument(
        '--season', '-s',
        type=int,
        action='append',
        help='Season year to scrape (e.g., 2024, 2025). Can be used multiple times.'
    )
    
    parser.add_argument(
        '--force', '-f',
        action='store_true',
        help='Force overwrite existing files'
    )
    
    parser.add_argument(
        '--output-dir', '-o',
        type=Path,
        help=f'Output directory (default: {BroncosWikiScraper.DEFAULT_OUTPUT_DIR})'
    )
    
    return parser.parse_args()


def scrape_season(season: int, force: bool = False, output_dir: Optional[Path] = None) -> Tuple[Optional[Path], bool]:
    """
    Scrape a single season.
    
    Args:
        season: Season year to scrape
        force: Whether to force overwrite existing files
        output_dir: Output directory
        
    Returns:
        Tuple of (Path to saved file or None if failed, whether file was actually downloaded)
    """
    print(f"\n{'='*50}")
    print(f"Scraping Denver Broncos {season} Season")
    print(f"{'='*50}")
    
    scraper = BroncosWikiScraper(
        season=season,
        force_overwrite=force,
        output_dir=output_dir
    )
    
    # Check if file exists before scraping
    file_existed_before = scraper.check_file_exists()[0]
    
    result_path = scraper.scrape()
    
    if result_path:
        file_size = result_path.stat().st_size
        modified_time = datetime.fromtimestamp(result_path.stat().st_mtime)
        
        print(f"\n✅ Success! HTML content saved to:")
        print(f"   {result_path.absolute()}")
        print(f"\n📊 File size: {file_size:,} bytes")
        print(f"🕐 Last modified: {modified_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Determine if this was actually downloaded (file was modified recently) or just skipped
        was_downloaded = not (file_existed_before and not force)
        
        return result_path, was_downloaded
    else:
        print(f"\n❌ Failed to scrape {season} season. Check the logs for details.")
        return None, False


def main():
    """
    Main function to run the scraper from command line.
    """
    args = parse_args()
    
    # Default to 2024 if no seasons specified
    seasons = args.season or [BroncosWikiScraper.DEFAULT_SEASON]
    
    print("Denver Broncos Wikipedia Scraper v2.0")
    print("=====================================")
    print(f"Seasons to scrape: {', '.join(map(str, seasons))}")
    print(f"Force overwrite: {args.force}")
    print(f"Output directory: {args.output_dir or BroncosWikiScraper.DEFAULT_OUTPUT_DIR}")
    
    # Track results
    successful_downloads = []
    failed_downloads = []
    skipped_files = []
    
    # Scrape each season
    for season in seasons:
        try:
            result_path, was_downloaded = scrape_season(season, args.force, args.output_dir)
            if result_path:
                if was_downloaded:
                    successful_downloads.append((season, result_path))
                else:
                    skipped_files.append((season, result_path))
            else:
                failed_downloads.append(season)
        except KeyboardInterrupt:
            print("\n\n⚠️ Scraping interrupted by user.")
            break
        except Exception as e:
            logger.error(f"Unexpected error scraping {season}: {e}")
            failed_downloads.append(season)
    
    # Print summary
    print(f"\n\n{'='*60}")
    print("SCRAPING SUMMARY")
    print(f"{'='*60}")
    
    if successful_downloads:
        print(f"\n✅ Successfully downloaded ({len(successful_downloads)}):")
        for season, path in successful_downloads:
            print(f"   {season}: {path.name}")
    
    if skipped_files:
        print(f"\n⏭️ Skipped (already exists, {len(skipped_files)}):")
        for season, path in skipped_files:
            print(f"   {season}: {path.name}")
    
    if failed_downloads:
        print(f"\n❌ Failed ({len(failed_downloads)}):")
        for season in failed_downloads:
            print(f"   {season}")
    
    print(f"\n📁 Output directory: {args.output_dir or BroncosWikiScraper.DEFAULT_OUTPUT_DIR}")
    
    # Return appropriate exit code
    if failed_downloads:
        return 1
    elif successful_downloads or skipped_files:
        return 0
    else:
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
