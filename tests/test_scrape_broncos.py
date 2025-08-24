"""
Tests for the Denver Broncos Wikipedia scraper.

Tests the BroncosWikiScraper class functionality including HTTP requests,
file operations, error handling, and integration scenarios.
"""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pytest
import requests

from scrape_broncos import BroncosWikiScraper


class TestBroncosWikiScraper:
    """Test suite for BroncosWikiScraper class."""
    
    def test_scraper_initialization_default(self):
        """Test scraper initialization with default parameters."""
        scraper = BroncosWikiScraper()
        
        assert scraper.output_dir == Path("data_source")
        assert scraper.filename == "2024_denver_broncos_wikipedia.html"
        assert scraper.timeout == 30
        assert scraper.max_retries == 3
        assert scraper.TARGET_URL == "https://en.wikipedia.org/wiki/2024_Denver_Broncos_season"
    
    def test_scraper_initialization_custom(self):
        """Test scraper initialization with custom parameters."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir)
            filename = "custom_broncos.html"
            timeout = 60
            max_retries = 5
            
            scraper = BroncosWikiScraper(
                output_dir=output_dir,
                filename=filename,
                timeout=timeout,
                max_retries=max_retries
            )
            
            assert scraper.output_dir == output_dir
            assert scraper.filename == filename
            assert scraper.timeout == timeout
            assert scraper.max_retries == max_retries
    
    def test_output_directory_creation(self):
        """Test that output directory is created automatically."""
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "nonexistent" / "nested"
            
            scraper = BroncosWikiScraper(output_dir=output_dir)
            
            assert output_dir.exists()
            assert output_dir.is_dir()
    
    def test_session_configuration(self):
        """Test that requests session is properly configured."""
        scraper = BroncosWikiScraper()
        
        # Check session exists and is configured
        assert hasattr(scraper, 'session')
        assert isinstance(scraper.session, requests.Session)
        
        # Check headers
        headers = scraper.session.headers
        assert 'User-Agent' in headers
        assert 'fantasy-analysis-scraper' in headers['User-Agent']
        assert headers['Accept'].startswith('text/html')
        assert headers['DNT'] == '1'
    
    @patch('scrape_broncos.requests.Session')
    def test_fetch_page_success(self, mock_session_class):
        """Test successful page fetch."""
        # Setup mock response
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.text = "<html><body>Denver Broncos content</body></html>"
        mock_response.content = b"content"
        mock_response.headers = {'content-type': 'text/html; charset=utf-8'}
        mock_response.apparent_encoding = 'utf-8'
        
        # Setup mock session
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        scraper = BroncosWikiScraper()
        result = scraper.fetch_page()
        
        assert result == "<html><body>Denver Broncos content</body></html>"
        mock_session.get.assert_called_once_with(scraper.TARGET_URL, timeout=30)
    
    @patch('scrape_broncos.requests.Session')
    def test_fetch_page_timeout(self, mock_session_class):
        """Test page fetch with timeout error."""
        mock_session = Mock()
        mock_session.get.side_effect = requests.Timeout("Request timed out")
        mock_session_class.return_value = mock_session
        
        scraper = BroncosWikiScraper()
        
        with pytest.raises(requests.Timeout):
            scraper.fetch_page()
    
    @patch('scrape_broncos.requests.Session')
    def test_fetch_page_connection_error(self, mock_session_class):
        """Test page fetch with connection error."""
        mock_session = Mock()
        mock_session.get.side_effect = requests.ConnectionError("Connection failed")
        mock_session_class.return_value = mock_session
        
        scraper = BroncosWikiScraper()
        
        with pytest.raises(requests.ConnectionError):
            scraper.fetch_page()
    
    @patch('scrape_broncos.requests.Session')
    def test_fetch_page_http_error(self, mock_session_class):
        """Test page fetch with HTTP error."""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
        
        mock_session = Mock()
        mock_session.get.return_value = mock_response
        mock_session_class.return_value = mock_session
        
        scraper = BroncosWikiScraper()
        
        with pytest.raises(requests.HTTPError):
            scraper.fetch_page()
    
    def test_save_html_success(self):
        """Test successful HTML content save."""
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
            html_content = "<html><body>Test content</body></html>"
            
            result_path = scraper.save_html(html_content)
            
            assert result_path.exists()
            assert result_path.is_file()
            
            # Verify content
            with open(result_path, 'r', encoding='utf-8') as f:
                saved_content = f.read()
            
            assert saved_content == html_content
    
    def test_save_html_file_already_exists(self):
        """Test that existing files are overwritten."""
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
            
            # Create existing file
            output_path = Path(temp_dir) / scraper.filename
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write("Old content")
            
            # Save new content
            new_content = "<html><body>New content</body></html>"
            result_path = scraper.save_html(new_content)
            
            # Verify overwrite
            with open(result_path, 'r', encoding='utf-8') as f:
                saved_content = f.read()
            
            assert saved_content == new_content
    
    def test_add_metadata(self):
        """Test metadata file creation."""
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
            
            # Create dummy HTML file
            html_file = Path(temp_dir) / scraper.filename
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write("<html></html>")
            
            # Add metadata
            scraper.add_metadata(html_file)
            
            # Check metadata file
            metadata_file = html_file.with_suffix('.metadata.json')
            assert metadata_file.exists()
            
            # Verify metadata content
            with open(metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            assert metadata['url'] == scraper.TARGET_URL
            assert 'scraped_at' in metadata
            assert metadata['file_size_bytes'] == html_file.stat().st_size
            assert metadata['filename'] == scraper.filename
            assert metadata['user_agent'] == scraper.USER_AGENT
            assert metadata['scraper_version'] == '1.0'
    
    @patch('scrape_broncos.BroncosWikiScraper.fetch_page')
    def test_scrape_success_integration(self, mock_fetch):
        """Test complete scraping workflow success."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup mock
            mock_html = "<html><body>Denver Broncos 2024 Season</body></html>"
            mock_fetch.return_value = mock_html
            
            scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
            result_path = scraper.scrape()
            
            # Verify result
            assert result_path is not None
            assert result_path.exists()
            assert result_path.is_file()
            
            # Verify content
            with open(result_path, 'r', encoding='utf-8') as f:
                content = f.read()
            assert content == mock_html
            
            # Verify metadata exists
            metadata_path = result_path.with_suffix('.metadata.json')
            assert metadata_path.exists()
    
    @patch('scrape_broncos.BroncosWikiScraper.fetch_page')
    def test_scrape_fetch_failure(self, mock_fetch):
        """Test scraping with fetch failure."""
        mock_fetch.side_effect = requests.RequestException("Network error")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
            result = scraper.scrape()
            
            assert result is None
    
    @patch('scrape_broncos.BroncosWikiScraper.fetch_page')
    @patch('scrape_broncos.BroncosWikiScraper.save_html')
    def test_scrape_save_failure(self, mock_save, mock_fetch):
        """Test scraping with save failure."""
        mock_fetch.return_value = "<html>content</html>"
        mock_save.side_effect = IOError("Write permission denied")
        
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
            result = scraper.scrape()
            
            assert result is None
    
    def test_session_cleanup(self):
        """Test that session is properly closed after scraping."""
        with tempfile.TemporaryDirectory() as temp_dir:
            scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
            
            # Mock session close method
            scraper.session.close = Mock()
            
            # Mock fetch to fail so we test cleanup in finally block
            with patch.object(scraper, 'fetch_page', side_effect=Exception("Test error")):
                scraper.scrape()
            
            # Verify session was closed
            scraper.session.close.assert_called_once()


class TestMainFunction:
    """Test suite for main function."""
    
    @patch('scrape_broncos.BroncosWikiScraper')
    def test_main_success(self, mock_scraper_class):
        """Test main function success scenario."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup mock
            result_path = Path(temp_dir) / "test.html"
            with open(result_path, 'w') as f:
                f.write("test content")
            
            mock_scraper = Mock()
            mock_scraper.scrape.return_value = result_path
            mock_scraper_class.return_value = mock_scraper
            
            # Import and run main
            from scrape_broncos import main
            
            with patch('builtins.print'):  # Suppress print output
                result = main()
            
            assert result == 0
            mock_scraper.scrape.assert_called_once()
    
    @patch('scrape_broncos.BroncosWikiScraper')
    def test_main_failure(self, mock_scraper_class):
        """Test main function failure scenario."""
        # Setup mock to return None (failure)
        mock_scraper = Mock()
        mock_scraper.scrape.return_value = None
        mock_scraper_class.return_value = mock_scraper
        
        # Import and run main
        from scrape_broncos import main
        
        with patch('builtins.print'):  # Suppress print output
            result = main()
        
        assert result == 1
        mock_scraper.scrape.assert_called_once()


# Integration test (runs against real Wikipedia if network available)
@pytest.mark.integration
def test_real_wikipedia_fetch():
    """Integration test with real Wikipedia (requires network)."""
    with tempfile.TemporaryDirectory() as temp_dir:
        scraper = BroncosWikiScraper(output_dir=Path(temp_dir))
        
        try:
            # This will make a real HTTP request
            html_content = scraper.fetch_page()
            
            # Basic validation
            assert html_content is not None
            assert len(html_content) > 1000  # Should be substantial content
            assert "Denver Broncos" in html_content
            assert "2024" in html_content
            
        except requests.RequestException:
            pytest.skip("Network not available or Wikipedia unreachable")