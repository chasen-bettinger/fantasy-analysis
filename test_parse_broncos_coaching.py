#!/usr/bin/env python3
"""
Test script for the Broncos coaching staff parser.
"""

import json
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from parse_broncos_coaching import BroncosCoachingParser


class TestBroncosCoachingParser:
    """Test cases for the BroncosCoachingParser class."""
    
    def test_parser_initialization(self):
        """Test that the parser initializes correctly."""
        parser = BroncosCoachingParser('test_file.html')
        assert parser.html_file_path == Path('test_file.html')
        assert parser.soup is None
        assert parser.coaching_staff == {}
    
    def test_clean_text(self):
        """Test the text cleaning functionality."""
        parser = BroncosCoachingParser('test_file.html')
        
        # Test normal text
        assert parser.clean_text('Sean Payton') == 'Sean Payton'
        
        # Test text with extra whitespace
        assert parser.clean_text('  Joe Lombardi  \n') == 'Joe Lombardi'
        
        # Test text with HTML entities
        assert parser.clean_text('Vance&nbsp;Joseph') == 'Vance Joseph'
        
        # Test None input
        assert parser.clean_text(None) is None
        
        # Test empty string
        assert parser.clean_text('') is None
    
    def test_validate_extracted_data_success(self):
        """Test validation with complete coaching staff data."""
        parser = BroncosCoachingParser('test_file.html')
        parser.coaching_staff = {
            'head_coach': 'Sean Payton',
            'offensive_coordinator': 'Joe Lombardi',
            'defensive_coordinator': 'Vance Joseph'
        }
        
        assert parser.validate_extracted_data() is True
    
    def test_validate_extracted_data_missing_coach(self):
        """Test validation with missing coaching staff data."""
        parser = BroncosCoachingParser('test_file.html')
        parser.coaching_staff = {
            'head_coach': 'Sean Payton',
            'offensive_coordinator': None,
            'defensive_coordinator': 'Vance Joseph'
        }
        
        assert parser.validate_extracted_data() is False
    
    def test_save_to_json(self):
        """Test saving coaching staff data to JSON file."""
        parser = BroncosCoachingParser('test_file.html')
        parser.coaching_staff = {
            'head_coach': 'Sean Payton',
            'offensive_coordinator': 'Joe Lombardi',
            'defensive_coordinator': 'Vance Joseph'
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tmp_file:
            temp_path = tmp_file.name
        
        try:
            parser.save_to_json(temp_path)
            
            # Verify the file was created and contains expected data
            assert Path(temp_path).exists()
            
            with open(temp_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            assert data['team'] == 'Denver Broncos'
            assert data['season'] == 2024
            assert data['coaching_staff'] == parser.coaching_staff
            assert data['source'] == 'Wikipedia - 2024 Denver Broncos season'
            assert 'extracted_at' in data
            
        finally:
            # Clean up
            if Path(temp_path).exists():
                os.unlink(temp_path)


def test_integration_with_actual_file():
    """Integration test with the actual HTML file."""
    script_dir = Path(__file__).parent
    html_file = script_dir / 'data_source' / '2024_denver_broncos_wikipedia.html'
    
    if not html_file.exists():
        pytest.skip("HTML file not found, skipping integration test")
    
    parser = BroncosCoachingParser(html_file)
    
    # Test that the file loads without error
    parser.load_html()
    assert parser.soup is not None
    
    # Test extraction
    coaching_staff = parser.extract_coaching_staff()
    
    # Verify expected coaches are found
    assert coaching_staff['head_coach'] == 'Sean Payton'
    assert coaching_staff['offensive_coordinator'] == 'Joe Lombardi'
    assert coaching_staff['defensive_coordinator'] == 'Vance Joseph'


if __name__ == '__main__':
    # Run basic tests
    test_integration_with_actual_file()
    print("All tests passed!")