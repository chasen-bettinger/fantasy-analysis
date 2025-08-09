"""
Query module for Fantasy Football analysis tool.

Provides high-level query functions for analyzing draft and player data.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd

from config import config
from database import FantasyDatabase, DatabaseError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueryError(Exception):
    """Custom exception for query operations."""

    pass


class FantasyQueries:
    """High-level query interface for fantasy football analysis."""

    def __init__(self, db: FantasyDatabase):
        """
        Initialize queries with database instance.

        Args:
            db: FantasyDatabase instance
        """
        self.db = db

    def get_draft_picks_by_round(self, round_id: Optional[int] = None) -> pd.DataFrame:
        """
        Get draft picks by round with player and team information.

        Args:
            round_id: Specific round to query (None for all rounds)

        Returns:
            DataFrame with draft pick details
        """
        base_query = """
        SELECT 
            dp.round_id,
            dp.round_pick_number,
            dp.overall_pick_number,
            p.name as player_name,
            p.position,
            nt.name as nfl_team_name,
            nt.abbreviation as nfl_team_abbrev,
            ft.name as fantasy_team_name,
            dp.is_keeper,
            dp.auto_draft_type_id
        FROM draft_picks dp
        LEFT JOIN players p ON dp.player_id = p.id
        LEFT JOIN nfl_teams nt ON p.nfl_team_id = nt.id
        LEFT JOIN fantasy_teams ft ON dp.fantasy_team_id = ft.id
        """

        if round_id is not None:
            query = (
                base_query + " WHERE dp.round_id = ? ORDER BY dp.overall_pick_number"
            )
            params = (round_id,)
        else:
            query = base_query + " ORDER BY dp.overall_pick_number"
            params = ()

        try:
            results = self.db.execute_query(query, params)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query draft picks by round: {e}")

    def get_picks_by_position(self, position: Optional[str] = None) -> pd.DataFrame:
        """
        Get draft picks grouped by position.

        Args:
            position: Specific position to query (None for all positions)

        Returns:
            DataFrame with picks by position
        """
        base_query = """
        SELECT 
            p.position,
            dp.round_id,
            dp.overall_pick_number,
            p.name as player_name,
            nt.abbreviation as nfl_team,
            ft.name as fantasy_team_name
        FROM draft_picks dp
        LEFT JOIN players p ON dp.player_id = p.id
        LEFT JOIN nfl_teams nt ON p.nfl_team_id = nt.id
        LEFT JOIN fantasy_teams ft ON dp.fantasy_team_id = ft.id
        WHERE p.position IS NOT NULL
        """

        if position:
            query = base_query + " AND p.position = ? ORDER BY dp.overall_pick_number"
            params = (position,)
        else:
            query = base_query + " ORDER BY p.position, dp.overall_pick_number"
            params = ()

        try:
            results = self.db.execute_query(query, params)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query picks by position: {e}")

    def get_team_draft_summary(
        self, fantasy_team_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Get draft summary by fantasy team.

        Args:
            fantasy_team_id: Specific team to query (None for all teams)

        Returns:
            DataFrame with team draft summaries
        """
        base_query = """
        SELECT 
            ft.name as fantasy_team_name,
            COUNT(CASE WHEN p.position = 'QB' THEN 1 END) as qb_picks,
            COUNT(CASE WHEN p.position = 'RB' THEN 1 END) as rb_picks,
            COUNT(CASE WHEN p.position = 'WR' THEN 1 END) as wr_picks,
            COUNT(CASE WHEN p.position = 'TE' THEN 1 END) as te_picks,
            COUNT(CASE WHEN p.position = 'K' THEN 1 END) as k_picks,
            COUNT(CASE WHEN p.position = 'DST' THEN 1 END) as dst_picks,
            MIN(dp.overall_pick_number) as earliest_pick,
            MAX(dp.overall_pick_number) as latest_pick
        FROM fantasy_teams ft
        LEFT JOIN draft_picks dp ON ft.id = dp.fantasy_team_id
        LEFT JOIN players p ON p.id = dp.player_id
        """

        if fantasy_team_id is not None:
            query = base_query + " WHERE ft.id = ? GROUP BY ft.id, ft.name"
            params = (fantasy_team_id,)
        else:
            query = base_query + " GROUP BY ft.id, ft.name ORDER BY ft.name"
            params = ()

        try:
            results = self.db.execute_query(query, params)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query team draft summary: {e}")

    def get_position_draft_trends(self) -> pd.DataFrame:
        """
        Get draft trends by position across rounds.

        Returns:
            DataFrame with position trends by round
        """
        query = """
        SELECT 
            dp.round_id,
            p.position,
            COUNT(*) as picks_count,
            AVG(dp.round_pick_number) as avg_round_position,
            MIN(dp.overall_pick_number) as earliest_overall,
            MAX(dp.overall_pick_number) as latest_overall
        FROM draft_picks dp
        LEFT JOIN players p ON dp.player_id = p.id
        WHERE p.position IS NOT NULL
        GROUP BY dp.round_id, p.position
        ORDER BY dp.round_id, picks_count DESC
        """

        try:
            results = self.db.execute_query(query)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query position draft trends: {e}")

    def get_nfl_team_draft_distribution(self) -> pd.DataFrame:
        """
        Get distribution of drafted players by NFL team.

        Returns:
            DataFrame with NFL team draft distribution
        """
        query = """
        SELECT 
            nt.name as nfl_team_name,
            nt.abbreviation,
            COUNT(*) as players_drafted,
            COUNT(CASE WHEN p.position = 'QB' THEN 1 END) as qb_drafted,
            COUNT(CASE WHEN p.position = 'RB' THEN 1 END) as rb_drafted,
            COUNT(CASE WHEN p.position = 'WR' THEN 1 END) as wr_drafted,
            COUNT(CASE WHEN p.position = 'TE' THEN 1 END) as te_drafted,
            AVG(dp.overall_pick_number) as avg_draft_position
        FROM draft_picks dp
        LEFT JOIN players p ON dp.player_id = p.id
        LEFT JOIN nfl_teams nt ON p.nfl_team_id = nt.id
        WHERE nt.name IS NOT NULL
        GROUP BY nt.id, nt.name, nt.abbreviation
        ORDER BY players_drafted DESC
        """

        try:
            results = self.db.execute_query(query)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query NFL team draft distribution: {e}")

    def get_draft_pick_value_analysis(self) -> pd.DataFrame:
        """
        Analyze draft pick values and patterns.

        Returns:
            DataFrame with draft value analysis
        """
        query = """
        SELECT 
            dp.round_id,
            p.position,
            COUNT(*) as position_picks_in_round,
            MIN(dp.overall_pick_number) as first_pick,
            MAX(dp.overall_pick_number) as last_pick,
            AVG(dp.overall_pick_number) as avg_pick,
            COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY dp.round_id) as round_percentage
        FROM draft_picks dp
        LEFT JOIN players p ON dp.player_id = p.id
        WHERE p.position IS NOT NULL
        GROUP BY dp.round_id, p.position
        ORDER BY dp.round_id, position_picks_in_round DESC
        """

        try:
            results = self.db.execute_query(query)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query draft pick value analysis: {e}")

    def get_keeper_analysis(self) -> pd.DataFrame:
        """
        Analyze keeper selections.

        Returns:
            DataFrame with keeper analysis
        """
        query = """
        SELECT 
            p.position,
            p.name as player_name,
            nt.abbreviation as nfl_team,
            dp.overall_pick_number,
            dp.round_id,
            ft.name as fantasy_team_name
        FROM draft_picks dp
        LEFT JOIN players p ON dp.player_id = p.id
        LEFT JOIN nfl_teams nt ON p.nfl_team_id = nt.id
        LEFT JOIN fantasy_teams ft ON dp.fantasy_team_id = ft.espn_team_id
        WHERE dp.is_keeper = 1
        ORDER BY dp.overall_pick_number
        """

        try:
            results = self.db.execute_query(query)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query keeper analysis: {e}")

    def get_auto_draft_analysis(self) -> pd.DataFrame:
        """
        Analyze auto-drafted picks vs manual picks.

        Returns:
            DataFrame with auto-draft analysis
        """
        query = """
        SELECT 
            CASE 
                WHEN dp.auto_draft_type_id = 0 THEN 'Manual'
                ELSE 'Auto'
            END as draft_type,
            p.position,
            COUNT(*) as pick_count,
            AVG(dp.overall_pick_number) as avg_pick_number,
            MIN(dp.overall_pick_number) as earliest_pick,
            MAX(dp.overall_pick_number) as latest_pick
        FROM draft_picks dp
        LEFT JOIN players p ON dp.player_id = p.id
        WHERE p.position IS NOT NULL
        GROUP BY 
            CASE WHEN dp.auto_draft_type_id = 0 THEN 'Manual' ELSE 'Auto' END,
            p.position
        ORDER BY draft_type, p.position
        """

        try:
            results = self.db.execute_query(query)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query auto-draft analysis: {e}")

    def get_games_by_week(self, scoring_period: Optional[int] = None) -> pd.DataFrame:
        """
        Get games scheduled by week/scoring period.

        Args:
            scoring_period: Specific week to query (None for all weeks)

        Returns:
            DataFrame with games by week
        """
        base_query = """
        SELECT 
            g.scoring_period_id as week,
            ht.name as home_team,
            ht.abbreviation as home_abbrev,
            at.name as away_team,
            at.abbreviation as away_abbrev,
            g.game_date,
            g.start_time_tbd,
            g.stats_official
        FROM games g
        LEFT JOIN nfl_teams ht ON g.home_team_id = ht.id
        LEFT JOIN nfl_teams at ON g.away_team_id = at.id
        """

        if scoring_period is not None:
            query = base_query + " WHERE g.scoring_period_id = ? ORDER BY g.game_date"
            params = (scoring_period,)
        else:
            query = base_query + " ORDER BY g.scoring_period_id, g.game_date"
            params = ()

        try:
            results = self.db.execute_query(query, params)
            return pd.DataFrame([dict(row) for row in results])
        except DatabaseError as e:
            raise QueryError(f"Failed to query games by week: {e}")

    def get_database_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of the database contents.

        Returns:
            Dictionary with database summary statistics
        """
        try:
            stats = self.db.get_database_stats()

            # Get additional summary stats
            position_counts = self.db.execute_query(
                """
                SELECT position, COUNT(*) as count 
                FROM players 
                WHERE position IS NOT NULL 
                GROUP BY position 
                ORDER BY count DESC
            """
            )

            round_counts = self.db.execute_query(
                """
                SELECT round_id, COUNT(*) as picks 
                FROM draft_picks 
                GROUP BY round_id 
                ORDER BY round_id
            """
            )

            return {
                "table_counts": stats,
                "positions": [dict(row) for row in position_counts],
                "rounds": [dict(row) for row in round_counts],
            }

        except DatabaseError as e:
            raise QueryError(f"Failed to get database summary: {e}")


def get_queries(db_path: str = None) -> FantasyQueries:
    """
    Convenience function to get a FantasyQueries instance.

    Args:
        db_path: Path to database file (defaults to config.DATABASE_PATH)

    Returns:
        FantasyQueries instance
    """
    if db_path is None:
        db_path = config.DATABASE_PATH
    db = FantasyDatabase(db_path)
    return FantasyQueries(db)


if __name__ == "__main__":
    # Quick test of query functionality
    try:
        queries = get_queries()
        summary = queries.get_database_summary()

        print("Database Summary:")
        print("Table Counts:", summary["table_counts"])
        print("Position Distribution:", summary["positions"])
        print("Draft Rounds:", summary["rounds"])

    except Exception as e:
        print(f"Query test failed: {e}")
