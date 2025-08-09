"""
Tests for the queries module.
"""

import pytest
import tempfile
import os
import pandas as pd

from database import FantasyDatabase
from queries import FantasyQueries, QueryError


class TestFantasyQueries:
    """Test cases for FantasyQueries class."""
    
    def setup_method(self):
        """Set up test database and queries for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.db = FantasyDatabase(self.db_path)
        self.queries = FantasyQueries(self.db)
        
        # Set up test data
        self._setup_test_data()
    
    def teardown_method(self):
        """Clean up after each test."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def _setup_test_data(self):
        """Insert test data for queries."""
        # Insert NFL teams
        team_data = [
            (1, 'Team A', 'City A', 'TA', 10),
            (2, 'Team B', 'City B', 'TB', 11)
        ]
        self.db.execute_many(
            "INSERT INTO nfl_teams (id, name, location, abbreviation, bye_week) VALUES (?, ?, ?, ?, ?)",
            team_data
        )
        
        # Insert fantasy teams
        fantasy_team_data = [
            (6, 'Fantasy Team 1', 'member-1'),
            (7, 'Fantasy Team 2', 'member-2')
        ]
        self.db.execute_many(
            "INSERT INTO fantasy_teams (espn_team_id, name, member_id) VALUES (?, ?, ?)",
            fantasy_team_data
        )
        
        # Insert players
        player_data = [
            (10001, 'QB Player', 'QB', 1, 'ACTIVE', True),
            (10002, 'RB Player', 'RB', 1, 'ACTIVE', True),
            (10003, 'WR Player', 'WR', 2, 'ACTIVE', True),
            (10004, 'TE Player', 'TE', 2, 'ACTIVE', True)
        ]
        self.db.execute_many(
            "INSERT INTO players (espn_player_id, name, position, nfl_team_id, eligibility_status, is_active) VALUES (?, ?, ?, ?, ?, ?)",
            player_data
        )
        
        # Insert draft picks
        draft_pick_data = [
            (1, 10001, 6, 1, 1, 1, 0, False, 0, 'member-1'),  # QB first overall
            (2, 10002, 7, 1, 2, 2, 2, False, 0, 'member-2'),  # RB second overall
            (3, 10003, 6, 2, 1, 3, 4, False, 0, 'member-1'),  # WR third overall
            (4, 10004, 7, 2, 2, 4, 6, False, 0, 'member-2')   # TE fourth overall
        ]
        self.db.execute_many(
            "INSERT INTO draft_picks (espn_pick_id, player_id, fantasy_team_id, round_id, round_pick_number, overall_pick_number, lineup_slot_id, is_keeper, auto_draft_type_id, member_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            draft_pick_data
        )
        
        # Insert games
        game_data = [
            (101, 1, 2, 1640995200000, 1, False, False, True)
        ]
        self.db.execute_many(
            "INSERT INTO games (espn_game_id, home_team_id, away_team_id, game_date, scoring_period_id, start_time_tbd, stats_official, valid_for_locking) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            game_data
        )
    
    def test_get_draft_picks_by_round(self):
        """Test querying draft picks by round."""
        # Get all rounds
        all_picks = self.queries.get_draft_picks_by_round()
        assert isinstance(all_picks, pd.DataFrame)
        assert len(all_picks) == 4
        assert 'round_id' in all_picks.columns
        assert 'player_name' in all_picks.columns
        assert 'position' in all_picks.columns
        
        # Get specific round
        round_1_picks = self.queries.get_draft_picks_by_round(round_id=1)
        assert len(round_1_picks) == 2
        assert all(round_1_picks['round_id'] == 1)
        
        # Check data integrity
        first_pick = round_1_picks[round_1_picks['overall_pick_number'] == 1].iloc[0]
        assert first_pick['player_name'] == 'QB Player'
        assert first_pick['position'] == 'QB'
    
    def test_get_picks_by_position(self):
        """Test querying picks by position."""
        # Get all positions
        all_positions = self.queries.get_picks_by_position()
        assert isinstance(all_positions, pd.DataFrame)
        assert len(all_positions) == 4
        assert set(all_positions['position'].unique()) == {'QB', 'RB', 'WR', 'TE'}
        
        # Get specific position
        qb_picks = self.queries.get_picks_by_position('QB')
        assert len(qb_picks) == 1
        assert qb_picks.iloc[0]['position'] == 'QB'
        assert qb_picks.iloc[0]['player_name'] == 'QB Player'
    
    def test_get_team_draft_summary(self):
        """Test team draft summary query."""
        # Get all teams
        all_teams = self.queries.get_team_draft_summary()
        assert isinstance(all_teams, pd.DataFrame)
        assert len(all_teams) == 2
        assert 'fantasy_team_name' in all_teams.columns
        assert 'total_picks' in all_teams.columns
        
        # Check pick counts
        team_6_data = all_teams[all_teams['espn_team_id'] == 6].iloc[0]
        assert team_6_data['total_picks'] == 2
        assert team_6_data['qb_picks'] == 1
        assert team_6_data['wr_picks'] == 1
        
        # Get specific team
        team_summary = self.queries.get_team_draft_summary(fantasy_team_id=7)
        assert len(team_summary) == 1
        assert team_summary.iloc[0]['espn_team_id'] == 7
        assert team_summary.iloc[0]['total_picks'] == 2
    
    def test_get_position_draft_trends(self):
        """Test position draft trends query."""
        trends = self.queries.get_position_draft_trends()
        assert isinstance(trends, pd.DataFrame)
        assert len(trends) == 4  # 4 positions across 2 rounds
        assert 'round_id' in trends.columns
        assert 'position' in trends.columns
        assert 'picks_count' in trends.columns
        
        # Check that each position appears once per round
        qb_trends = trends[trends['position'] == 'QB']
        assert len(qb_trends) == 1
        assert qb_trends.iloc[0]['round_id'] == 1
        assert qb_trends.iloc[0]['picks_count'] == 1
    
    def test_get_nfl_team_draft_distribution(self):
        """Test NFL team draft distribution query."""
        distribution = self.queries.get_nfl_team_draft_distribution()
        assert isinstance(distribution, pd.DataFrame)
        assert len(distribution) == 2  # 2 NFL teams
        assert 'nfl_team_name' in distribution.columns
        assert 'players_drafted' in distribution.columns
        
        # Check data
        team_a_data = distribution[distribution['abbreviation'] == 'TA'].iloc[0]
        assert team_a_data['players_drafted'] == 2  # QB and RB
        assert team_a_data['qb_drafted'] == 1
        assert team_a_data['rb_drafted'] == 1
    
    def test_get_draft_pick_value_analysis(self):
        """Test draft pick value analysis query."""
        analysis = self.queries.get_draft_pick_value_analysis()
        assert isinstance(analysis, pd.DataFrame)
        assert 'round_id' in analysis.columns
        assert 'position' in analysis.columns
        assert 'position_picks_in_round' in analysis.columns
        assert 'round_percentage' in analysis.columns
        
        # Check that percentages add up to 100 for each round
        round_1_data = analysis[analysis['round_id'] == 1]
        round_1_percentage = round_1_data['round_percentage'].sum()
        assert abs(round_1_percentage - 100.0) < 0.01  # Allow for floating point precision
    
    def test_get_keeper_analysis(self):
        """Test keeper analysis query."""
        # With current test data (no keepers), should return empty DataFrame
        keepers = self.queries.get_keeper_analysis()
        assert isinstance(keepers, pd.DataFrame)
        assert len(keepers) == 0
        
        # Insert a keeper pick and test again
        self.db.execute_insert(
            "INSERT INTO draft_picks (espn_pick_id, player_id, fantasy_team_id, round_id, round_pick_number, overall_pick_number, is_keeper, member_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (5, 10001, 6, 3, 1, 5, True, 'member-1')
        )
        
        keepers = self.queries.get_keeper_analysis()
        assert len(keepers) == 1
        assert keepers.iloc[0]['player_name'] == 'QB Player'
    
    def test_get_auto_draft_analysis(self):
        """Test auto-draft analysis query."""
        analysis = self.queries.get_auto_draft_analysis()
        assert isinstance(analysis, pd.DataFrame)
        assert 'draft_type' in analysis.columns
        assert 'position' in analysis.columns
        assert 'pick_count' in analysis.columns
        
        # All current test picks are manual (auto_draft_type_id = 0)
        manual_picks = analysis[analysis['draft_type'] == 'Manual']
        assert len(manual_picks) == 4  # 4 positions
        
        auto_picks = analysis[analysis['draft_type'] == 'Auto']
        assert len(auto_picks) == 0  # No auto picks in test data
    
    def test_get_games_by_week(self):
        """Test games by week query."""
        # Get all games
        all_games = self.queries.get_games_by_week()
        assert isinstance(all_games, pd.DataFrame)
        assert len(all_games) == 1
        assert 'week' in all_games.columns
        assert 'home_team' in all_games.columns
        assert 'away_team' in all_games.columns
        
        # Get specific week
        week_1_games = self.queries.get_games_by_week(scoring_period=1)
        assert len(week_1_games) == 1
        assert week_1_games.iloc[0]['week'] == 1
        assert week_1_games.iloc[0]['home_team'] == 'Team A'
        assert week_1_games.iloc[0]['away_team'] == 'Team B'
        
        # Get non-existent week
        no_games = self.queries.get_games_by_week(scoring_period=99)
        assert len(no_games) == 0
    
    def test_get_database_summary(self):
        """Test database summary query."""
        summary = self.queries.get_database_summary()
        assert isinstance(summary, dict)
        assert 'table_counts' in summary
        assert 'positions' in summary
        assert 'rounds' in summary
        
        # Check table counts
        table_counts = summary['table_counts']
        assert table_counts['nfl_teams'] == 2
        assert table_counts['fantasy_teams'] == 2
        assert table_counts['players'] == 4
        assert table_counts['draft_picks'] == 4
        assert table_counts['games'] == 1
        
        # Check position distribution
        positions = summary['positions']
        assert len(positions) == 4
        position_names = [p['position'] for p in positions]
        assert set(position_names) == {'QB', 'RB', 'WR', 'TE'}
        
        # Check rounds
        rounds = summary['rounds']
        assert len(rounds) == 2
        assert rounds[0]['round_id'] == 1
        assert rounds[0]['picks'] == 2
    
    def test_empty_database_queries(self):
        """Test queries on empty database."""
        # Create a fresh database with no test data
        temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        temp_db.close()
        empty_db = FantasyDatabase(temp_db.name)
        empty_queries = FantasyQueries(empty_db)
        
        try:
            # Most queries should return empty DataFrames gracefully
            assert len(empty_queries.get_draft_picks_by_round()) == 0
            assert len(empty_queries.get_picks_by_position()) == 0
            assert len(empty_queries.get_team_draft_summary()) == 0
            assert len(empty_queries.get_position_draft_trends()) == 0
            assert len(empty_queries.get_nfl_team_draft_distribution()) == 0
            assert len(empty_queries.get_games_by_week()) == 0
            
            # Database summary should still work
            summary = empty_queries.get_database_summary()
            assert all(count == 0 for count in summary['table_counts'].values())
            
        finally:
            os.unlink(temp_db.name)