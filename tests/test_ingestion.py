"""
Tests for the ingestion module.
"""

import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from config import config
from database import FantasyDatabase
from ingestion import DataIngestion, IngestionError


class TestDataIngestion:
    """Test cases for DataIngestion class."""
    
    def setup_method(self):
        """Set up test database and ingestion for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.db = FantasyDatabase(self.db_path)
        self.ingestion = DataIngestion(self.db)
    
    def teardown_method(self):
        """Clean up after each test."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def create_test_teams_file(self) -> str:
        """Create a temporary teams JSON file for testing."""
        teams_data = {
            "proTeams": [
                {
                    "id": 1,
                    "name": "Test Team",
                    "location": "Test City",
                    "abbrev": "TT",
                    "byeWeek": 10,
                    "proGamesByScoringPeriod": {
                        "1": [
                            {
                                "id": 101,
                                "homeProTeamId": 1,
                                "awayProTeamId": 2,
                                "date": 1640995200000,
                                "scoringPeriodId": 1,
                                "startTimeTBD": False,
                                "statsOfficial": False,
                                "validForLocking": True
                            }
                        ]
                    }
                },
                {
                    "id": 2,
                    "name": "Another Team",
                    "location": "Another City",
                    "abbrev": "AT",
                    "byeWeek": 12,
                    "proGamesByScoringPeriod": {}
                }
            ]
        }
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        json.dump(teams_data, temp_file)
        temp_file.close()
        return temp_file.name
    
    def create_test_draft_file(self) -> str:
        """Create a temporary draft history JSON file for testing."""
        draft_data = [
            {
                "draftDetail": {
                    "picks": [
                        {
                            "id": 1,
                            "playerId": 10001,
                            "teamId": 6,
                            "roundId": 1,
                            "roundPickNumber": 1,
                            "overallPickNumber": 1,
                            "lineupSlotId": 2,
                            "keeper": False,
                            "autoDraftTypeId": 0,
                            "memberId": "member-1"
                        },
                        {
                            "id": 2,
                            "playerId": 10002,
                            "teamId": 7,
                            "roundId": 1,
                            "roundPickNumber": 2,
                            "overallPickNumber": 2,
                            "lineupSlotId": 2,
                            "keeper": False,
                            "autoDraftTypeId": 0,
                            "memberId": "member-2"
                        }
                    ]
                }
            }
        ]
        
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        json.dump(draft_data, temp_file)
        temp_file.close()
        return temp_file.name
    
    def test_load_teams_data(self):
        """Test loading teams data from JSON file."""
        teams_file = self.create_test_teams_file()
        
        try:
            self.ingestion.load_teams_data(teams_file)
            
            # Check teams were loaded
            teams = self.db.execute_query("SELECT * FROM nfl_teams ORDER BY id")
            assert len(teams) == 2
            assert teams[0]['name'] == 'Test Team'
            assert teams[0]['abbreviation'] == 'TT'
            assert teams[1]['name'] == 'Another Team'
            
            # Check games were loaded
            games = self.db.execute_query("SELECT * FROM games")
            assert len(games) == 1
            assert games[0]['home_team_id'] == 1
            assert games[0]['away_team_id'] == 2
            
        finally:
            os.unlink(teams_file)
    
    def test_load_teams_data_file_not_found(self):
        """Test error handling when teams file doesn't exist."""
        with pytest.raises(IngestionError, match="Teams file not found"):
            self.ingestion.load_teams_data("nonexistent_file.json")
    
    def test_load_draft_history(self):
        """Test loading draft history from JSON file."""
        draft_file = self.create_test_draft_file()
        
        try:
            self.ingestion.load_draft_history(draft_file)
            
            # Check fantasy teams were created
            teams = self.db.execute_query("SELECT * FROM fantasy_teams ORDER BY espn_team_id")
            assert len(teams) == 2
            assert teams[0]['espn_team_id'] == 6
            assert teams[1]['espn_team_id'] == 7
            
            # Check draft picks were loaded
            picks = self.db.execute_query("SELECT * FROM draft_picks ORDER BY overall_pick_number")
            assert len(picks) == 2
            assert picks[0]['player_id'] == 10001
            assert picks[0]['overall_pick_number'] == 1
            assert picks[1]['player_id'] == 10002
            assert picks[1]['overall_pick_number'] == 2
            
        finally:
            os.unlink(draft_file)
    
    def test_load_draft_history_file_not_found(self):
        """Test error handling when draft file doesn't exist."""
        with pytest.raises(IngestionError, match="Draft history file not found"):
            self.ingestion.load_draft_history("nonexistent_draft.json")
    
    def test_load_draft_history_invalid_data(self):
        """Test error handling with invalid draft data."""
        # Create file with invalid data structure
        temp_file = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json')
        json.dump({"invalid": "data"}, temp_file)
        temp_file.close()
        
        try:
            with pytest.raises(IngestionError, match="Draft history data should be a non-empty list"):
                self.ingestion.load_draft_history(temp_file.name)
        finally:
            os.unlink(temp_file.name)
    
    @patch('espn.get_players')
    def test_load_players_data(self, mock_get_players):
        """Test loading players data from ESPN API."""
        # Mock ESPN API response
        mock_players_data = [
            {
                "id": 20001,
                "fullName": "Test Player 1",
                "eligibleSlots": [0],  # QB
                "proTeamId": 1,
                "injuryStatus": "ACTIVE"
            },
            {
                "id": 20002,
                "fullName": "Test Player 2",
                "eligibleSlots": [2],  # RB
                "proTeamId": 2,
                "injuryStatus": "QUESTIONABLE"
            }
        ]
        mock_get_players.return_value = mock_players_data
        
        self.ingestion.load_players_data()
        
        # Check players were loaded
        players = self.db.execute_query("SELECT * FROM players ORDER BY espn_player_id")
        assert len(players) == 2
        assert players[0]['name'] == 'Test Player 1'
        assert players[0]['position'] == 'QB'
        assert players[0]['is_active'] == 1
        assert players[1]['name'] == 'Test Player 2'
        assert players[1]['position'] == 'RB'
        assert players[1]['is_active'] == 0  # QUESTIONABLE status
    
    @patch('espn.get_players')
    def test_load_players_data_skip_existing(self, mock_get_players):
        """Test that existing player data is not reloaded unless forced."""
        # First, insert some existing player data
        self.db.execute_insert(
            "INSERT INTO players (espn_player_id, name, position) VALUES (?, ?, ?)",
            (30001, "Existing Player", "QB")
        )
        
        # Mock should not be called when data exists and force_refresh=False
        mock_get_players.return_value = []
        self.ingestion.load_players_data(force_refresh=False)
        mock_get_players.assert_not_called()
        
        # Mock should be called when force_refresh=True
        mock_get_players.return_value = [
            {
                "id": 30002,
                "fullName": "New Player",
                "eligibleSlots": [4],  # WR
                "proTeamId": 1,
                "injuryStatus": "ACTIVE"
            }
        ]
        self.ingestion.load_players_data(force_refresh=True)
        mock_get_players.assert_called_once()
        
        # Check new player was added
        players = self.db.execute_query("SELECT COUNT(*) as count FROM players")
        assert players[0]['count'] == 2
    
    def test_determine_position(self):
        """Test position determination from eligibility slots."""
        # Test known positions
        assert self.ingestion._determine_position([0]) == 'QB'
        assert self.ingestion._determine_position([2]) == 'RB'
        assert self.ingestion._determine_position([4]) == 'WR'
        assert self.ingestion._determine_position([6]) == 'TE'
        assert self.ingestion._determine_position([17]) == 'K'
        assert self.ingestion._determine_position([16]) == 'DST'
        
        # Test multiple eligibilities (should return first recognized)
        assert self.ingestion._determine_position([0, 4]) == 'QB'
        assert self.ingestion._determine_position([2, 4]) == 'RB'
        
        # Test unknown position
        assert self.ingestion._determine_position([99]) == 'UNKNOWN'
        assert self.ingestion._determine_position([]) == 'UNKNOWN'
    
    @patch('espn.get_players')
    def test_run_full_ingestion(self, mock_get_players):
        """Test complete ingestion process."""
        # Create test files
        teams_file = self.create_test_teams_file()
        draft_file = self.create_test_draft_file()
        
        # Mock players API
        mock_get_players.return_value = [
            {
                "id": 40001,
                "fullName": "Full Ingestion Player",
                "eligibleSlots": [0],
                "proTeamId": 1,
                "injuryStatus": "ACTIVE"
            }
        ]
        
        try:
            stats = self.ingestion.run_full_ingestion(teams_file, draft_file, force_player_refresh=True)
            
            # Check all data was loaded
            assert stats['nfl_teams'] == 2
            assert stats['games'] == 1
            assert stats['fantasy_teams'] == 2
            assert stats['draft_picks'] == 2
            assert stats['players'] == 1
            
        finally:
            os.unlink(teams_file)
            os.unlink(draft_file)
    
    def test_run_full_ingestion_missing_files(self):
        """Test full ingestion with missing files."""
        with pytest.raises(IngestionError, match="Full ingestion failed"):
            self.ingestion.run_full_ingestion("missing_" + config.TEAMS_FILE, "missing_" + config.DRAFT_HISTORY_FILE)