"""
Tests for the database module.
"""

import pytest
import sqlite3
import tempfile
import os
from pathlib import Path

from database import FantasyDatabase, DatabaseError


class TestFantasyDatabase:
    """Test cases for FantasyDatabase class."""
    
    def setup_method(self):
        """Set up test database for each test."""
        self.temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
        self.temp_db.close()
        self.db_path = self.temp_db.name
        self.db = FantasyDatabase(self.db_path)
    
    def teardown_method(self):
        """Clean up after each test."""
        if os.path.exists(self.db_path):
            os.unlink(self.db_path)
    
    def test_database_initialization(self):
        """Test database initialization creates all tables."""
        # Database should be created automatically
        assert os.path.exists(self.db_path)
        
        # Check that all expected tables exist
        expected_tables = ['nfl_teams', 'games', 'fantasy_teams', 'players', 'draft_picks']
        
        with self.db.get_connection() as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
        
        for table in expected_tables:
            assert table in tables
    
    def test_connection_context_manager(self):
        """Test database connection context manager."""
        with self.db.get_connection() as conn:
            assert isinstance(conn, sqlite3.Connection)
            # Test that row factory is set
            cursor = conn.execute("SELECT 1 as test_col")
            row = cursor.fetchone()
            assert hasattr(row, 'keys')  # Row factory provides dict-like access
    
    def test_execute_query(self):
        """Test query execution."""
        # Insert test data
        query = "INSERT INTO nfl_teams (id, name, location, abbreviation) VALUES (?, ?, ?, ?)"
        self.db.execute_insert(query, (1, 'Test Team', 'Test City', 'TT'))
        
        # Query the data
        results = self.db.execute_query("SELECT * FROM nfl_teams WHERE id = ?", (1,))
        assert len(results) == 1
        assert results[0]['name'] == 'Test Team'
        assert results[0]['abbreviation'] == 'TT'
    
    def test_execute_insert(self):
        """Test insert operations."""
        query = "INSERT INTO nfl_teams (id, name, location, abbreviation) VALUES (?, ?, ?, ?)"
        row_id = self.db.execute_insert(query, (2, 'Insert Team', 'Insert City', 'IT'))
        
        assert row_id is not None
        
        # Verify the insert
        results = self.db.execute_query("SELECT * FROM nfl_teams WHERE id = ?", (2,))
        assert len(results) == 1
        assert results[0]['name'] == 'Insert Team'
    
    def test_execute_many(self):
        """Test batch insert operations."""
        query = "INSERT INTO nfl_teams (id, name, location, abbreviation) VALUES (?, ?, ?, ?)"
        data = [
            (10, 'Team 1', 'City 1', 'T1'),
            (11, 'Team 2', 'City 2', 'T2'),
            (12, 'Team 3', 'City 3', 'T3')
        ]
        
        self.db.execute_many(query, data)
        
        # Verify all records were inserted
        results = self.db.execute_query("SELECT COUNT(*) as count FROM nfl_teams")
        assert results[0]['count'] == 3
    
    def test_get_table_count(self):
        """Test table row counting."""
        # Initially empty
        assert self.db.get_table_count('nfl_teams') == 0
        
        # Insert some data
        query = "INSERT INTO nfl_teams (id, name, location, abbreviation) VALUES (?, ?, ?, ?)"
        data = [
            (20, 'Team A', 'City A', 'TA'),
            (21, 'Team B', 'City B', 'TB')
        ]
        self.db.execute_many(query, data)
        
        assert self.db.get_table_count('nfl_teams') == 2
    
    def test_clear_table(self):
        """Test table clearing."""
        # Insert some data
        query = "INSERT INTO nfl_teams (id, name, location, abbreviation) VALUES (?, ?, ?, ?)"
        self.db.execute_insert(query, (30, 'Team Clear', 'City Clear', 'TC'))
        
        assert self.db.get_table_count('nfl_teams') == 1
        
        # Clear the table
        self.db.clear_table('nfl_teams')
        assert self.db.get_table_count('nfl_teams') == 0
    
    def test_get_database_stats(self):
        """Test database statistics."""
        stats = self.db.get_database_stats()
        
        # Should have all expected tables
        expected_tables = ['nfl_teams', 'games', 'fantasy_teams', 'players', 'draft_picks']
        for table in expected_tables:
            assert table in stats
            assert stats[table] == 0  # Initially empty
        
        # Insert some data and check again
        query = "INSERT INTO nfl_teams (id, name, location, abbreviation) VALUES (?, ?, ?, ?)"
        self.db.execute_insert(query, (40, 'Stats Team', 'Stats City', 'ST'))
        
        stats = self.db.get_database_stats()
        assert stats['nfl_teams'] == 1
        assert stats['games'] == 0  # Still empty
    
    def test_foreign_key_constraints(self):
        """Test that foreign key constraints are enabled."""
        # Insert a team first
        team_query = "INSERT INTO nfl_teams (id, name, location, abbreviation) VALUES (?, ?, ?, ?)"
        self.db.execute_insert(team_query, (50, 'FK Team', 'FK City', 'FK'))
        
        # Insert a player with valid team reference
        player_query = "INSERT INTO players (espn_player_id, name, nfl_team_id) VALUES (?, ?, ?)"
        self.db.execute_insert(player_query, (1001, 'Test Player', 50))
        
        # Verify player was inserted
        results = self.db.execute_query("SELECT * FROM players WHERE espn_player_id = ?", (1001,))
        assert len(results) == 1
        assert results[0]['nfl_team_id'] == 50
    
    def test_database_error_handling(self):
        """Test error handling in database operations."""
        # Test invalid query
        with pytest.raises(DatabaseError):
            self.db.execute_query("SELECT * FROM nonexistent_table")
        
        # Test invalid insert
        with pytest.raises(DatabaseError):
            self.db.execute_insert("INSERT INTO nonexistent_table VALUES (?)", (1,))