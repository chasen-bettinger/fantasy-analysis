"""
Database module for Fantasy Football analysis tool.

Provides SQLite database schema, connection management, and data access layer.
"""

import sqlite3
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, List, Dict, Any, Optional

from config import config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Database schema
SCHEMA = """
-- NFL Teams table
CREATE TABLE IF NOT EXISTS nfl_teams (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    location TEXT NOT NULL,
    abbreviation TEXT NOT NULL UNIQUE,
    bye_week INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Games table
CREATE TABLE IF NOT EXISTS games (
    id INTEGER PRIMARY KEY,
    season INTEGER NOT NULL,  -- Season year (e.g., 2015, 2016, 2017)
    espn_game_id INTEGER,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    game_date INTEGER NOT NULL,  -- Unix timestamp
    scoring_period_id INTEGER NOT NULL,
    start_time_tbd BOOLEAN DEFAULT FALSE,
    stats_official BOOLEAN DEFAULT FALSE,
    valid_for_locking BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (home_team_id) REFERENCES nfl_teams (id),
    FOREIGN KEY (away_team_id) REFERENCES nfl_teams (id),
    UNIQUE(season, espn_game_id)  -- Unique per season
);

-- Fantasy Teams table 
CREATE TABLE IF NOT EXISTS fantasy_teams (
    id INTEGER PRIMARY KEY,
    season INTEGER NOT NULL,  -- Season year (e.g., 2015, 2016, 2017)
    espn_team_id INTEGER NOT NULL,
    name TEXT,
    wins INTEGER,
    losses INTEGER,
    ties INTEGER,
    points_for INTEGER,
    points_against INTEGER,
    final_position INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(season, espn_team_id)  -- Unique per season
);

-- Rosters table 
CREATE TABLE IF NOT EXISTS rosters (
    id INTEGER PRIMARY KEY,
    team_id INTEGER,
    player_id INTEGER,
    lineup_slot_id INTEGER,
    season INTEGER NOT NULL,  -- Season year for direct filtering
    FOREIGN KEY (team_id) REFERENCES fantasy_teams (id),
    FOREIGN KEY (player_id) REFERENCES players (id),
    UNIQUE(season, team_id, player_id, lineup_slot_id)  -- Unique roster entry per season
);

-- Players table 
CREATE TABLE IF NOT EXISTS players (
    id INTEGER PRIMARY KEY,
    espn_player_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    position TEXT,  -- QB, RB, WR, TE, K, DST
    nfl_team_id INTEGER,
    eligibility_status TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    fantasy_score REAL DEFAULT 0.0,
    season INTEGER
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (nfl_team_id) REFERENCES nfl_teams (id)
    UNIQUE(season, espn_player_id)
);

-- Draft Picks table
CREATE TABLE IF NOT EXISTS draft_picks (
    id INTEGER PRIMARY KEY,
    season INTEGER NOT NULL,  -- Season year for direct filtering
    espn_pick_id INTEGER,
    player_id INTEGER,
    fantasy_team_id INTEGER,
    round_id INTEGER NOT NULL,
    round_pick_number INTEGER NOT NULL,
    overall_pick_number INTEGER NOT NULL,
    lineup_slot_id INTEGER,
    is_keeper BOOLEAN DEFAULT FALSE,
    auto_draft_type_id INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (player_id) REFERENCES players (id),
    FOREIGN KEY (fantasy_team_id) REFERENCES fantasy_teams (id),
    UNIQUE(season, espn_pick_id)  -- Unique pick per season
);

-- Indexes for performance 
CREATE INDEX IF NOT EXISTS idx_games_season ON games (season);
CREATE INDEX IF NOT EXISTS idx_games_teams ON games (home_team_id, away_team_id);
CREATE INDEX IF NOT EXISTS idx_games_date ON games (game_date);
CREATE INDEX IF NOT EXISTS idx_fantasy_teams_season ON fantasy_teams (season);
CREATE INDEX IF NOT EXISTS idx_fantasy_teams_season_espn ON fantasy_teams (season, espn_team_id);
CREATE INDEX IF NOT EXISTS idx_rosters_season ON rosters (season);
CREATE INDEX IF NOT EXISTS idx_players_team ON players (nfl_team_id);
CREATE INDEX IF NOT EXISTS idx_players_position ON players (position);
CREATE INDEX IF NOT EXISTS idx_draft_picks_season ON draft_picks (season);
CREATE INDEX IF NOT EXISTS idx_draft_picks_player ON draft_picks (player_id);
CREATE INDEX IF NOT EXISTS idx_draft_picks_team ON draft_picks (fantasy_team_id);
CREATE INDEX IF NOT EXISTS idx_draft_picks_round ON draft_picks (round_id, round_pick_number);
"""


class DatabaseError(Exception):
    """Custom exception for database operations."""

    pass


class FantasyDatabase:
    """Fantasy Football database management class."""

    def __init__(self, db_path: str = None):
        """
        Initialize database connection.

        Args:
            db_path: Path to SQLite database file (defaults to config.DATABASE_PATH)
        """
        if db_path is None:
            db_path = config.DATABASE_PATH
        self.db_path = Path(db_path)
        self.ensure_database_exists()

    def ensure_database_exists(self) -> None:
        """Create database and tables if they don't exist."""
        try:
            with self.get_connection() as conn:
                conn.executescript(SCHEMA)
                conn.commit()
                logger.info(f"Database initialized at {self.db_path}")
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to initialize database: {e}")

    @contextmanager
    def get_connection(self) -> Generator[sqlite3.Connection, None, None]:
        """
        Context manager for database connections.

        Yields:
            SQLite connection with row factory set
        """
        conn = None
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row  # Enable dict-like access to rows
            conn.execute("PRAGMA foreign_keys = OFF")  # Enable foreign key constraints
            yield conn
        except sqlite3.Error as e:
            if conn:
                conn.rollback()
            raise DatabaseError(f"Database operation failed: {e}")
        finally:
            if conn:
                conn.close()

    def execute_query(self, query: str, params: tuple = ()) -> List[sqlite3.Row]:
        """
        Execute a SELECT query and return results.

        Args:
            query: SQL query string
            params: Query parameters

        Returns:
            List of result rows
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(query, params)
                return cursor.fetchall()
        except sqlite3.Error as e:
            raise DatabaseError(f"Query execution failed: {e}")

    def execute_insert(self, query: str, params: tuple = ()) -> int:
        """
        Execute an INSERT query and return the last row ID.

        Args:
            query: SQL INSERT statement
            params: Query parameters

        Returns:
            Last inserted row ID
        """
        try:
            with self.get_connection() as conn:
                cursor = conn.execute(query, params)
                conn.commit()
                return cursor.lastrowid
        except sqlite3.Error as e:
            raise DatabaseError(f"Insert operation failed: {e}")

    def execute_many(self, query: str, params_list: List[tuple]) -> None:
        """
        Execute a query with multiple parameter sets.

        Args:
            query: SQL statement
            params_list: List of parameter tuples
        """
        try:
            with self.get_connection() as conn:
                conn.executemany(query, params_list)
                conn.commit()
                logger.info(f"Executed batch operation with {len(params_list)} records")
        except sqlite3.Error as e:
            raise DatabaseError(f"Batch operation failed: {e}")

    def get_table_count(self, table_name: str) -> int:
        """
        Get the number of rows in a table.

        Args:
            table_name: Name of the table

        Returns:
            Number of rows in the table
        """
        query = f"SELECT COUNT(*) as count FROM {table_name}"
        result = self.execute_query(query)
        return result[0]["count"] if result else 0

    def clear_table(self, table_name: str) -> None:
        """
        Clear all data from a table.

        Args:
            table_name: Name of the table to clear
        """
        try:
            with self.get_connection() as conn:
                conn.execute(f"DELETE FROM {table_name}")
                conn.commit()
                logger.info(f"Cleared all data from {table_name}")
        except sqlite3.Error as e:
            raise DatabaseError(f"Failed to clear table {table_name}: {e}")

    def get_database_stats(self) -> Dict[str, int]:
        """
        Get statistics about the database.

        Returns:
            Dictionary with table names and row counts
        """
        tables = ["nfl_teams", "games", "fantasy_teams", "players", "draft_picks"]
        stats = {}

        for table in tables:
            try:
                stats[table] = self.get_table_count(table)
            except DatabaseError:
                stats[table] = 0

        return stats


# Convenience function for quick database access
def get_database(db_path: str = None) -> FantasyDatabase:
    """
    Get a FantasyDatabase instance.

    Args:
        db_path: Path to database file (defaults to config.DATABASE_PATH)

    Returns:
        FantasyDatabase instance
    """
    return FantasyDatabase(db_path)


if __name__ == "__main__":
    # Quick test of database functionality
    db = get_database()
    stats = db.get_database_stats()
    print("Database Statistics:")
    for table, count in stats.items():
        print(f"  {table}: {count} records")
