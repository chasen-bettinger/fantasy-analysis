#!/usr/bin/env python3
"""
Parse Denver Broncos season Wikipedia HTML files to extract coaching staff information.

This script finds all available season HTML files on disk and extracts head coach, 
offensive coordinator, and defensive coordinator information from each file,
saving the results to individual JSON files.

The script handles multiple HTML structures:
- Legacy format (2024 and earlier): Uses <b> tags for coaching sections
- New format (2025 and later): Uses <dt> tags for coaching sections

For each season, the script automatically tries both methods to ensure compatibility
with Wikipedia's evolving HTML structure.
"""

import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Tuple

from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class BroncosCoachingParser:
    """Parser for extracting coaching staff information from Denver Broncos Wikipedia HTML."""
    
    def __init__(self, html_file_path: str, season: int = None):
        """Initialize the parser with the HTML file path and season."""
        self.html_file_path = Path(html_file_path)
        self.season = season or self._extract_season_from_filename()
        self.soup = None
        self.coaching_staff = {}
        
    def _extract_season_from_filename(self) -> int:
        """Extract season year from the filename."""
        filename = self.html_file_path.name
        match = re.search(r'(\d{4})_denver_broncos_wikipedia\.html', filename)
        if match:
            return int(match.group(1))
        return 2024  # Default fallback
        
    def load_html(self) -> None:
        """Load and parse the HTML file."""
        if not self.html_file_path.exists():
            raise FileNotFoundError(f"HTML file not found: {self.html_file_path}")
            
        logger.info(f"Loading HTML file: {self.html_file_path}")
        
        with open(self.html_file_path, 'r', encoding='utf-8') as file:
            content = file.read()
            
        self.soup = BeautifulSoup(content, 'lxml')
        logger.info("HTML file loaded successfully")
        
    def extract_head_coach(self) -> str:
        """Extract head coach from the infobox."""
        try:
            # Look for head coach in the infobox
            infobox = self.soup.find('table', class_='infobox')
            if infobox:
                head_coach_row = infobox.find('th', string='Head coach')
                if head_coach_row:
                    head_coach_cell = head_coach_row.find_next_sibling('td')
                    if head_coach_cell:
                        link = head_coach_cell.find('a')
                        if link:
                            return link.get_text().strip()
                            
            logger.warning("Head coach not found in infobox, trying alternative method")
            
            # Alternative: Look in the coaching staff section
            coaching_section = self.soup.find('b', string='Head coaches')
            if coaching_section:
                parent = coaching_section.parent
                link = parent.find_next('a')
                if link:
                    return link.get_text().strip()
                    
        except Exception as e:
            logger.error(f"Error extracting head coach: {e}")
            
        return None
        
    def extract_offensive_coordinator(self) -> str:
        """Extract offensive coordinator from the coaching staff section."""
        try:
            # Determine which method to try first based on season
            if self.season >= 2025:
                # For 2025+, try dt method first, then b method
                result = self._extract_offensive_coordinator_dt_method()
                if result:
                    logger.info(f"Successfully extracted offensive coordinator using dt method for {self.season}")
                    return result
                    
                logger.debug(f"dt method failed for {self.season}, trying b method")
                result = self._extract_offensive_coordinator_b_method()
                if result:
                    logger.info(f"Successfully extracted offensive coordinator using b method for {self.season}")
                    return result
            else:
                # For pre-2025, try b method first, then dt method
                result = self._extract_offensive_coordinator_b_method()
                if result:
                    logger.info(f"Successfully extracted offensive coordinator using b method for {self.season}")
                    return result
                    
                logger.debug(f"b method failed for {self.season}, trying dt method")
                result = self._extract_offensive_coordinator_dt_method()
                if result:
                    logger.info(f"Successfully extracted offensive coordinator using dt method for {self.season}")
                    return result
                    
        except Exception as e:
            logger.error(f"Error extracting offensive coordinator: {e}")
            
        logger.warning(f"Failed to extract offensive coordinator using both methods for {self.season}")
        return None
    
    def _extract_offensive_coordinator_b_method(self) -> str:
        """Extract offensive coordinator using the original <b> tag method."""
        try:
            # Look for offensive coordinator in the coaching staff section
            offensive_section = self.soup.find('b', string='Offensive coaches')
            if offensive_section:
                # Find the next ul element that contains the coaching list
                ul_element = offensive_section.find_next_sibling('ul')
                if not ul_element:
                    # Try finding it in the parent's next sibling
                    parent = offensive_section.parent
                    ul_element = parent.find_next_sibling('ul')
                    
                if ul_element:
                    # Look for the list item containing "Offensive coordinator"
                    for li in ul_element.find_all('li'):
                        li_text = li.get_text()
                        if 'Offensive coordinator' in li_text:
                            link = li.find('a')
                            if link:
                                return link.get_text().strip()
                                
        except Exception as e:
            logger.debug(f"Error in b method for offensive coordinator: {e}")
            
        return None
    
    def _extract_offensive_coordinator_dt_method(self) -> str:
        """Extract offensive coordinator using the new <dt> tag method."""
        try:
            # Look for offensive coordinator in the coaching staff section using dt tags
            offensive_section = self.soup.find('dt', string='Offensive coaches')
            if offensive_section:
                # The structure is: <dl><dt>Offensive coaches</dt></dl><ul>...
                # So we need to find the parent <dl> and then the next <ul> sibling
                dl_parent = offensive_section.parent
                if dl_parent and dl_parent.name == 'dl':
                    # Find the next ul element after the dl parent
                    ul_element = dl_parent.find_next_sibling('ul')
                    
                    if ul_element:
                        # Look for the list item containing "Offensive coordinator"
                        for li in ul_element.find_all('li'):
                            li_text = li.get_text()
                            if 'Offensive coordinator' in li_text:
                                link = li.find('a')
                                if link:
                                    return link.get_text().strip()
                
                # Fallback: try other methods if the above doesn't work
                ul_element = offensive_section.find_next_sibling('ul')
                
                if not ul_element:
                    # Try finding ul within parent's next sibling
                    parent = offensive_section.parent
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        ul_element = next_sibling.find('ul') if next_sibling.name != 'ul' else next_sibling
                
                if ul_element:
                    # Look for the list item containing "Offensive coordinator"
                    for li in ul_element.find_all('li'):
                        li_text = li.get_text()
                        if 'Offensive coordinator' in li_text:
                            link = li.find('a')
                            if link:
                                return link.get_text().strip()
                                    
        except Exception as e:
            logger.debug(f"Error in dt method for offensive coordinator: {e}")
            
        return None
        
    def extract_defensive_coordinator(self) -> str:
        """Extract defensive coordinator from the coaching staff section."""
        try:
            # Determine which method to try first based on season
            if self.season >= 2025:
                # For 2025+, try dt method first, then b method
                result = self._extract_defensive_coordinator_dt_method()
                if result:
                    logger.info(f"Successfully extracted defensive coordinator using dt method for {self.season}")
                    return result
                    
                logger.debug(f"dt method failed for {self.season}, trying b method")
                result = self._extract_defensive_coordinator_b_method()
                if result:
                    logger.info(f"Successfully extracted defensive coordinator using b method for {self.season}")
                    return result
            else:
                # For pre-2025, try b method first, then dt method
                result = self._extract_defensive_coordinator_b_method()
                if result:
                    logger.info(f"Successfully extracted defensive coordinator using b method for {self.season}")
                    return result
                    
                logger.debug(f"b method failed for {self.season}, trying dt method")
                result = self._extract_defensive_coordinator_dt_method()
                if result:
                    logger.info(f"Successfully extracted defensive coordinator using dt method for {self.season}")
                    return result
                    
        except Exception as e:
            logger.error(f"Error extracting defensive coordinator: {e}")
            
        logger.warning(f"Failed to extract defensive coordinator using both methods for {self.season}")
        return None
    
    def _extract_defensive_coordinator_b_method(self) -> str:
        """Extract defensive coordinator using the original <b> tag method."""
        try:
            # Look for defensive coordinator in the coaching staff section
            defensive_section = self.soup.find('b', string='Defensive coaches')
            if defensive_section:
                # Find the next ul element that contains the coaching list
                ul_element = defensive_section.find_next_sibling('ul')
                if not ul_element:
                    # Try finding it in the parent's next sibling
                    parent = defensive_section.parent
                    ul_element = parent.find_next_sibling('ul')
                    
                if ul_element:
                    # Look for the list item containing "Defensive coordinator"
                    for li in ul_element.find_all('li'):
                        li_text = li.get_text()
                        if 'Defensive coordinator' in li_text:
                            link = li.find('a')
                            if link:
                                return link.get_text().strip()
                                
        except Exception as e:
            logger.debug(f"Error in b method for defensive coordinator: {e}")
            
        return None
    
    def _extract_defensive_coordinator_dt_method(self) -> str:
        """Extract defensive coordinator using the new <dt> tag method."""
        try:
            # Look for defensive coordinator in the coaching staff section using dt tags
            defensive_section = self.soup.find('dt', string='Defensive coaches')
            if defensive_section:
                # The structure is: <dl><dt>Defensive coaches</dt></dl><ul>...
                # So we need to find the parent <dl> and then the next <ul> sibling
                dl_parent = defensive_section.parent
                if dl_parent and dl_parent.name == 'dl':
                    # Find the next ul element after the dl parent
                    ul_element = dl_parent.find_next_sibling('ul')
                    
                    if ul_element:
                        # Look for the list item containing "Defensive coordinator"
                        for li in ul_element.find_all('li'):
                            li_text = li.get_text()
                            if 'Defensive coordinator' in li_text:
                                link = li.find('a')
                                if link:
                                    return link.get_text().strip()
                
                # Fallback: try other methods if the above doesn't work
                ul_element = defensive_section.find_next_sibling('ul')
                
                if not ul_element:
                    # Try finding ul within parent's next sibling
                    parent = defensive_section.parent
                    next_sibling = parent.find_next_sibling()
                    if next_sibling:
                        ul_element = next_sibling.find('ul') if next_sibling.name != 'ul' else next_sibling
                
                if ul_element:
                    # Look for the list item containing "Defensive coordinator"
                    for li in ul_element.find_all('li'):
                        li_text = li.get_text()
                        if 'Defensive coordinator' in li_text:
                            link = li.find('a')
                            if link:
                                return link.get_text().strip()
                                    
        except Exception as e:
            logger.debug(f"Error in dt method for defensive coordinator: {e}")
            
        return None
        
    def clean_text(self, text: str) -> str:
        """Clean extracted text by removing extra whitespace and HTML entities."""
        if not text:
            return None
            
        # Remove extra whitespace and newlines
        text = ' '.join(text.split())
        
        # Remove any remaining HTML entities
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        text = text.replace('&quot;', '"')
        
        return text.strip()
        
    def extract_coaching_staff(self) -> dict:
        """Extract all coaching staff information."""
        logger.info("Extracting coaching staff information")
        
        head_coach = self.extract_head_coach()
        offensive_coordinator = self.extract_offensive_coordinator()
        defensive_coordinator = self.extract_defensive_coordinator()
        
        # Clean the extracted text
        head_coach = self.clean_text(head_coach)
        offensive_coordinator = self.clean_text(offensive_coordinator)
        defensive_coordinator = self.clean_text(defensive_coordinator)
        
        self.coaching_staff = {
            'head_coach': head_coach,
            'offensive_coordinator': offensive_coordinator,
            'defensive_coordinator': defensive_coordinator
        }
        
        logger.info(f"Extracted coaching staff: {self.coaching_staff}")
        return self.coaching_staff
        
    def validate_extracted_data(self) -> bool:
        """Validate that all required coaching staff information was extracted."""
        required_coaches = ['head_coach', 'offensive_coordinator', 'defensive_coordinator']
        missing_coaches = []
        
        for coach_type in required_coaches:
            if not self.coaching_staff.get(coach_type):
                missing_coaches.append(coach_type)
                
        if missing_coaches:
            logger.warning(f"Missing coaching staff information for {self.season}: {missing_coaches}")
            return False
            
        # Only validate expected names for 2024 season
        if self.season == 2024:
            expected_values = {
                'head_coach': 'Sean Payton',
                'offensive_coordinator': 'Joe Lombardi',
                'defensive_coordinator': 'Vance Joseph'
            }
            
            for coach_type, expected_name in expected_values.items():
                actual_name = self.coaching_staff.get(coach_type)
                if actual_name != expected_name:
                    logger.warning(f"Unexpected {coach_type} for 2024: expected '{expected_name}', got '{actual_name}'")
                
        return True
        
    def save_to_json(self, output_path: str) -> None:
        """Save the extracted coaching staff information to a JSON file."""
        output_data = {
            'team': 'Denver Broncos',
            'season': self.season,
            'coaching_staff': self.coaching_staff,
            'source': f'Wikipedia - {self.season} Denver Broncos season',
            'extracted_at': datetime.now().isoformat()
        }
        
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Saving coaching staff data to: {output_path}")
        
        with open(output_path, 'w', encoding='utf-8') as file:
            json.dump(output_data, file, indent=2, ensure_ascii=False)
            
        logger.info(f"Data saved successfully to {output_path}")
        
    def parse(self, output_path: str) -> dict:
        """Main parsing method that orchestrates the extraction process."""
        try:
            self.load_html()
            coaching_staff = self.extract_coaching_staff()
            
            if not self.validate_extracted_data():
                logger.warning("Validation failed, but proceeding with available data")
                
            self.save_to_json(output_path)
            return coaching_staff
            
        except Exception as e:
            logger.error(f"Error during parsing: {e}")
            raise


def find_available_seasons(data_dir: Path) -> List[Tuple[int, Path]]:
    """Find all available Denver Broncos season HTML files."""
    html_files = []
    pattern = r'(\d{4})_denver_broncos_wikipedia\.html'
    
    for file in data_dir.glob('*_denver_broncos_wikipedia.html'):
        match = re.search(pattern, file.name)
        if match:
            season = int(match.group(1))
            html_files.append((season, file))
    
    # Sort by season year
    html_files.sort(key=lambda x: x[0])
    return html_files

def process_single_season(season: int, html_file: Path, output_dir: Path) -> Dict:
    """Process a single season's HTML file."""
    logger.info(f"Processing {season} season: {html_file}")
    
    output_file = output_dir / f'{season}_broncos_coaching_staff.json'
    
    try:
        parser = BroncosCoachingParser(html_file, season)
        coaching_staff = parser.parse(output_file)
        
        logger.info(f"Successfully processed {season} season")
        return {
            'season': season,
            'success': True,
            'coaching_staff': coaching_staff,
            'output_file': output_file
        }
        
    except Exception as e:
        logger.error(f"Failed to process {season} season: {e}")
        return {
            'season': season,
            'success': False,
            'error': str(e),
            'output_file': output_file
        }

def main():
    """Main function to process all available season files."""
    script_dir = Path(__file__).parent
    data_dir = script_dir / 'data_source'
    
    if not data_dir.exists():
        logger.error(f"Data directory not found: {data_dir}")
        sys.exit(1)
    
    # Find all available season HTML files
    available_seasons = find_available_seasons(data_dir)
    
    if not available_seasons:
        logger.error(f"No Denver Broncos season HTML files found in {data_dir}")
        print(f"Looking for files matching pattern: *_denver_broncos_wikipedia.html")
        sys.exit(1)
    
    logger.info(f"Found {len(available_seasons)} season files to process")
    
    results = []
    successful_seasons = []
    failed_seasons = []
    
    # Process each season
    for season, html_file in available_seasons:
        result = process_single_season(season, html_file, data_dir)
        results.append(result)
        
        if result['success']:
            successful_seasons.append(season)
        else:
            failed_seasons.append(season)
    
    # Print summary results
    print("\n" + "=" * 80)
    print("DENVER BRONCOS COACHING STAFF EXTRACTION RESULTS")
    print("=" * 80)
    
    for result in results:
        season = result['season']
        print(f"\n{season} Season:")
        print(f"{'='*12}")
        
        if result['success']:
            coaching_staff = result['coaching_staff']
            print(f"✅ SUCCESS")
            print(f"Head Coach: {coaching_staff.get('head_coach', 'NOT FOUND')}")
            print(f"Offensive Coordinator: {coaching_staff.get('offensive_coordinator', 'NOT FOUND')}")
            print(f"Defensive Coordinator: {coaching_staff.get('defensive_coordinator', 'NOT FOUND')}")
            print(f"Output: {result['output_file']}")
        else:
            print(f"❌ FAILED: {result['error']}")
    
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Total seasons processed: {len(results)}")
    print(f"Successful: {len(successful_seasons)} - {successful_seasons}")
    print(f"Failed: {len(failed_seasons)} - {failed_seasons}")
    
    if failed_seasons:
        sys.exit(1)


if __name__ == '__main__':
    main()