"""
Tests for the analysis module.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from database import FantasyDatabase
from queries import FantasyQueries
from analysis import FantasyAnalysis, AnalysisError


class TestFantasyAnalysis:
    """Test cases for FantasyAnalysis class."""
    
    def setup_method(self):
        """Set up test database and analysis for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.db = FantasyDatabase(self.db_path)
        self.queries = FantasyQueries(self.db)
        self.analysis = FantasyAnalysis(self.queries)
        
        # Set up test data
        self._setup_test_data()
    
    def teardown_method(self):
        """Clean up after each test."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
        
        # Clean up any generated output files
        if hasattr(self.analysis, 'output_dir') and self.analysis.output_dir.exists():
            for file in self.analysis.output_dir.glob('*'):
                if file.is_file():
                    file.unlink()
            self.analysis.output_dir.rmdir()
    
    def _setup_test_data(self):
        """Insert comprehensive test data for analysis."""
        # Insert NFL teams
        team_data = [
            (1, 'Team A', 'City A', 'TA', 10),
            (2, 'Team B', 'City B', 'TB', 11),
            (3, 'Team C', 'City C', 'TC', 12)
        ]
        self.db.execute_many(
            "INSERT INTO nfl_teams (id, name, location, abbreviation, bye_week) VALUES (?, ?, ?, ?, ?)",
            team_data
        )
        
        # Insert fantasy teams
        fantasy_team_data = [
            (6, 'Fantasy Team 1', 'member-1'),
            (7, 'Fantasy Team 2', 'member-2'),
            (8, 'Fantasy Team 3', 'member-3')
        ]
        self.db.execute_many(
            "INSERT INTO fantasy_teams (espn_team_id, name, member_id) VALUES (?, ?, ?)",
            fantasy_team_data
        )
        
        # Insert players with varied positions
        player_data = [
            (10001, 'QB Player 1', 'QB', 1, 'ACTIVE', True),
            (10002, 'QB Player 2', 'QB', 2, 'ACTIVE', True),
            (10003, 'RB Player 1', 'RB', 1, 'ACTIVE', True),
            (10004, 'RB Player 2', 'RB', 2, 'ACTIVE', True),
            (10005, 'RB Player 3', 'RB', 3, 'ACTIVE', True),
            (10006, 'WR Player 1', 'WR', 1, 'ACTIVE', True),
            (10007, 'WR Player 2', 'WR', 2, 'ACTIVE', True),
            (10008, 'WR Player 3', 'WR', 3, 'ACTIVE', True),
            (10009, 'TE Player 1', 'TE', 1, 'ACTIVE', True),
            (10010, 'K Player 1', 'K', 2, 'ACTIVE', True)
        ]
        self.db.execute_many(
            "INSERT INTO players (espn_player_id, name, position, nfl_team_id, eligibility_status, is_active) VALUES (?, ?, ?, ?, ?, ?)",
            player_data
        )
        
        # Insert draft picks across multiple rounds
        draft_pick_data = [
            # Round 1
            (1, 10001, 6, 1, 1, 1, 0, False, 0, 'member-1'),   # QB
            (2, 10003, 7, 1, 2, 2, 2, False, 0, 'member-2'),   # RB
            (3, 10006, 8, 1, 3, 3, 4, False, 0, 'member-3'),   # WR
            # Round 2
            (4, 10002, 8, 2, 1, 4, 0, False, 0, 'member-3'),   # QB
            (5, 10004, 6, 2, 2, 5, 2, False, 0, 'member-1'),   # RB
            (6, 10007, 7, 2, 3, 6, 4, False, 0, 'member-2'),   # WR
            # Round 3
            (7, 10005, 7, 3, 1, 7, 2, False, 0, 'member-2'),   # RB
            (8, 10008, 6, 3, 2, 8, 4, False, 0, 'member-1'),   # WR
            (9, 10009, 8, 3, 3, 9, 6, False, 0, 'member-3'),   # TE
            # Round 4
            (10, 10010, 6, 4, 1, 10, 17, False, 0, 'member-1'), # K
        ]
        self.db.execute_many(
            "INSERT INTO draft_picks (espn_pick_id, player_id, fantasy_team_id, round_id, round_pick_number, overall_pick_number, lineup_slot_id, is_keeper, auto_draft_type_id, member_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            draft_pick_data
        )
    
    def test_analyze_draft_patterns(self):
        """Test draft pattern analysis."""
        # Mock matplotlib to avoid display issues during testing
        with patch('matplotlib.pyplot.savefig'), patch('matplotlib.pyplot.close'):
            results = self.analysis.analyze_draft_patterns(save_plots=False)
        
        assert isinstance(results, dict)
        assert 'total_picks' in results
        assert 'rounds_drafted' in results
        assert 'positions_drafted' in results
        assert 'position_distribution' in results
        assert 'average_pick_by_position' in results
        
        # Check values
        assert results['total_picks'] == 10
        assert results['rounds_drafted'] == 4
        assert results['positions_drafted'] == 5  # QB, RB, WR, TE, K
        
        # Check position distribution
        pos_dist = results['position_distribution']
        assert pos_dist['RB'] == 3  # Most RBs drafted
        assert pos_dist['WR'] == 3  # Equal WRs
        assert pos_dist['QB'] == 2
        assert pos_dist['TE'] == 1
        assert pos_dist['K'] == 1
        
        # Check average draft positions
        avg_picks = results['average_pick_by_position']
        assert avg_picks['QB'] < avg_picks['K']  # QBs drafted earlier than kickers
    
    def test_analyze_draft_patterns_with_plots(self):
        """Test draft pattern analysis with plot generation."""
        with patch('matplotlib.pyplot.savefig') as mock_savefig, \
             patch('matplotlib.pyplot.close') as mock_close:
            results = self.analysis.analyze_draft_patterns(save_plots=True)
            
            # Verify plots were attempted to be saved
            mock_savefig.assert_called()
            mock_close.assert_called()
        
        assert results['total_picks'] == 10
    
    def test_analyze_team_strategies(self):
        """Test team strategy analysis."""
        with patch('matplotlib.pyplot.savefig'), patch('matplotlib.pyplot.close'):
            results = self.analysis.analyze_team_strategies(save_plots=False)
        
        assert isinstance(results, dict)
        assert 'teams_analyzed' in results
        assert 'avg_picks_per_team' in results
        assert 'position_strategies' in results
        
        # Check values
        assert results['teams_analyzed'] == 3
        assert abs(results['avg_picks_per_team'] - 10/3) < 0.1  # ~3.33 picks per team
        
        # Check position strategies
        pos_strategies = results['position_strategies']
        assert 'QB' in pos_strategies
        assert 'RB' in pos_strategies
        assert pos_strategies['RB']['avg_picks'] == 1.0  # Each team got 1 RB on average
    
    def test_analyze_position_scarcity(self):
        """Test position scarcity analysis."""
        with patch('matplotlib.pyplot.savefig'), patch('matplotlib.pyplot.close'):
            results = self.analysis.analyze_position_scarcity(save_plots=False)
        
        assert isinstance(results, dict)
        assert 'position_counts' in results
        assert 'scarcity_score' in results
        assert 'draft_urgency' in results
        assert 'position_depth' in results
        
        # Check position counts
        pos_counts = results['position_counts']
        assert pos_counts['RB'] == 3
        assert pos_counts['WR'] == 3
        
        # Check scarcity scores (higher score = more scarce/drafted earlier)
        scarcity = results['scarcity_score']
        assert 'QB' in scarcity
        assert 'K' in scarcity
        assert scarcity['QB'] > scarcity['K']  # QBs more scarce than kickers
        
        # Check draft urgency (percentage drafted in first 3 rounds)
        urgency = results['draft_urgency']
        assert urgency['K'] == 0.0  # K drafted in round 4, so 0% urgency
        assert urgency['QB'] == 100.0  # All QBs drafted in first 3 rounds
    
    def test_calculate_scarcity_score(self):
        """Test scarcity score calculation."""
        # Create mock data
        import pandas as pd
        mock_picks = pd.DataFrame({
            'position': ['QB', 'QB', 'RB', 'K'],
            'overall_pick_number': [1, 4, 2, 10]
        })
        
        scores = self.analysis._calculate_scarcity_score(mock_picks)
        
        # QB average pick = 2.5, RB average pick = 2, K average pick = 10
        # Scarcity = 100 / average_pick
        assert scores['RB'] > scores['QB']  # RB more scarce (earlier average)
        assert scores['QB'] > scores['K']   # QB more scarce than K
    
    def test_calculate_draft_urgency(self):
        """Test draft urgency calculation."""
        import pandas as pd
        mock_picks = pd.DataFrame({
            'position': ['QB', 'QB', 'RB', 'K'],
            'round_id': [1, 2, 1, 4],
            'overall_pick_number': [1, 4, 2, 10]
        })
        
        urgency = self.analysis._calculate_draft_urgency(mock_picks)
        
        # QB: 2 picks, both in first 3 rounds = 100%
        # RB: 1 pick in round 1 (first 3 rounds) = 100%
        # K: 1 pick in round 4 (not in first 3) = 0%
        assert urgency['QB'] == 100.0
        assert urgency['RB'] == 100.0
        assert urgency['K'] == 0.0
    
    def test_generate_comprehensive_report(self):
        """Test comprehensive report generation."""
        with patch('matplotlib.pyplot.savefig'), \
             patch('matplotlib.pyplot.close'), \
             patch('builtins.open', create=True) as mock_open:
            
            mock_open.return_value.__enter__.return_value.write = MagicMock()
            
            report = self.analysis.generate_comprehensive_report(save_plots=False)
        
        assert isinstance(report, dict)
        assert 'database_summary' in report
        assert 'draft_patterns' in report
        assert 'team_strategies' in report
        assert 'position_scarcity' in report
        
        # Check that each analysis component returns expected data
        assert report['draft_patterns']['total_picks'] == 10
        assert report['team_strategies']['teams_analyzed'] == 3
        assert len(report['position_scarcity']['position_counts']) > 0
    
    def test_comprehensive_report_with_empty_optional_data(self):
        """Test comprehensive report when optional analyses return empty data."""
        # Mock queries to return empty DataFrames for optional analyses
        with patch.object(self.queries, 'get_keeper_analysis') as mock_keeper, \
             patch.object(self.queries, 'get_auto_draft_analysis') as mock_auto, \
             patch('matplotlib.pyplot.savefig'), \
             patch('matplotlib.pyplot.close'):
            
            import pandas as pd
            mock_keeper.return_value = pd.DataFrame()  # Empty keeper data
            mock_auto.return_value = pd.DataFrame()    # Empty auto-draft data
            
            report = self.analysis.generate_comprehensive_report(save_plots=False)
        
        # Should still complete successfully
        assert isinstance(report, dict)
        assert 'draft_patterns' in report
        # Optional analyses should not be in report when empty
        assert 'keeper_analysis' not in report or len(report.get('keeper_analysis', {})) == 0
    
    def test_analysis_with_empty_data(self):
        """Test analysis methods with empty data."""
        # Create empty database
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        empty_db = FantasyDatabase(temp_db.name)
        empty_queries = FantasyQueries(empty_db)
        empty_analysis = FantasyAnalysis(empty_queries)
        
        try:
            # Should raise AnalysisError for empty data
            with pytest.raises(AnalysisError, match="No draft pick data available"):
                empty_analysis.analyze_draft_patterns(save_plots=False)
            
            with pytest.raises(AnalysisError, match="No team data available"):
                empty_analysis.analyze_team_strategies(save_plots=False)
            
            with pytest.raises(AnalysisError, match="No position data available"):
                empty_analysis.analyze_position_scarcity(save_plots=False)
        
        finally:
            os.unlink(temp_db.name)
    
    def test_output_directory_creation(self):
        """Test that output directory is created."""
        # The output directory should be created during initialization
        assert self.analysis.output_dir.exists()
        assert self.analysis.output_dir.is_dir()
    
    def test_save_report_summary(self):
        """Test saving of report summary."""
        mock_report = {
            'database_summary': {
                'table_counts': {'players': 10, 'draft_picks': 5}
            },
            'draft_patterns': {
                'total_picks': 5,
                'rounds_drafted': 3,
                'positions_drafted': 4,
                'position_distribution': {'QB': 1, 'RB': 2}
            },
            'team_strategies': {
                'teams_analyzed': 2,
                'avg_picks_per_team': 2.5
            },
            'position_scarcity': {
                'scarcity_score': {'QB': 10.5, 'RB': 8.2}
            }
        }
        
        # Mock file operations
        with patch('builtins.open', create=True) as mock_open:
            mock_file = MagicMock()
            mock_open.return_value.__enter__.return_value = mock_file
            
            self.analysis._save_report_summary(mock_report)
            
            # Verify file was opened for writing
            mock_open.assert_called_once()
            # Verify content was written
            mock_file.write.assert_called()
            
            # Check that summary content includes key information
            written_content = ''.join(call.args[0] for call in mock_file.write.call_args_list)
            assert 'Fantasy Football Draft Analysis Report' in written_content
            assert 'Total picks: 5' in written_content
            assert 'Teams analyzed: 2' in written_content