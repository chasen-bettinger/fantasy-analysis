"""
Data ingestion module for Fantasy Football analysis tool.

Handles loading data from JSON files and ESPN API into the SQLite database.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any

from config import config
from database import FantasyDatabase
import espn

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()), format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


class IngestionError(Exception):
    """Custom exception for data ingestion operations."""

    pass


class DataIngestion:
    """Handles data ingestion from various sources into the database."""

    def __init__(self, db: FantasyDatabase):
        """
        Initialize data ingestion with database instance.

        Args:
            db: FantasyDatabase instance
        """
        self.db = db

    def load_teams_data(self, teams_file: str = None, season: int = None) -> None:
        """
        Load NFL teams and games data from teams file.

        Args:
            teams_file: Path to teams JSON file (defaults to config.TEAMS_FILE)
            season: Season year for games (defaults to config.DEFAULT_SEASON)
        """
        try:
            if teams_file is None:
                teams_file = config.TEAMS_FILE
            if season is None:
                season = config.get_current_season()

            teams_path = Path(teams_file)
            if not teams_path.exists():
                raise IngestionError(f"Teams file not found: {teams_file}")

            with open(teams_path, "r") as f:
                data = json.load(f)

            pro_teams = data.get("proTeams", [])
            logger.info(f"Loading {len(pro_teams)} NFL teams for season {season}")

            # Load teams
            teams_loaded = self._load_nfl_teams(pro_teams)
            logger.info(f"Loaded {teams_loaded} NFL teams")

            # Load games
            games_loaded = self._load_games(pro_teams, season)
            logger.info(f"Loaded {games_loaded} games for season {season}")

        except (json.JSONDecodeError, KeyError) as e:
            raise IngestionError(f"Error parsing teams data: {e}")
        except Exception as e:
            raise IngestionError(f"Unexpected error loading teams data: {e}")

    def _load_nfl_teams(self, pro_teams: List[Dict[str, Any]]) -> int:
        """Load NFL teams into database."""
        teams_data = []

        for team in pro_teams:
            teams_data.append(
                (
                    team.get("id"),
                    team.get("name", ""),
                    team.get("location", ""),
                    team.get("abbrev", ""),
                    team.get("byeWeek"),
                )
            )

        # Use INSERT OR IGNORE to avoid duplicates
        query = """
        INSERT OR IGNORE INTO nfl_teams 
        (id, name, location, abbreviation, bye_week)
        VALUES (?, ?, ?, ?, ?)
        """

        self.db.execute_many(query, teams_data)
        return len(teams_data)

    def _load_games(self, pro_teams: List[Dict[str, Any]], season: int) -> int:
        """Load games from pro teams data."""
        games_data = []

        for team in pro_teams:
            games_by_period = team.get("proGamesByScoringPeriod", {})

            for games in games_by_period.values():
                for game in games:
                    games_data.append(
                        (
                            season,  # Add season as first parameter
                            game.get("id"),
                            game.get("homeProTeamId"),
                            game.get("awayProTeamId"),
                            game.get("date"),
                            game.get("scoringPeriodId"),
                            game.get("startTimeTBD", False),
                            game.get("statsOfficial", False),
                            game.get("validForLocking", True),
                        )
                    )

        # Remove duplicates (same game can appear in multiple teams' data)
        unique_games = {
            (game[0], game[1]): game for game in games_data
        }.values()  # Use season+game_id as key

        query = """
        INSERT OR IGNORE INTO games 
        (season, espn_game_id, home_team_id, away_team_id, game_date, scoring_period_id,
         start_time_tbd, stats_official, valid_for_locking)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.db.execute_many(query, list(unique_games))
        return len(unique_games)

    def load_draft_history(self, draft_file: str = None, season: int = None) -> None:
        """
        Load draft history data from JSON file.

        Args:
            draft_file: Path to draft history JSON file (defaults to config.DRAFT_HISTORY_FILE)
            season: Season year for draft data (defaults to config.DEFAULT_SEASON)
        """
        try:
            if season is None:
                season = config.get_current_season()

            data = espn.get_draft_history(season)
            # if draft_file is None:
            #     draft_file = config.DRAFT_HISTORY_FILE
            # draft_path = Path(draft_file)
            # if not draft_path.exists():
            #     raise IngestionError(f"Draft history file not found: {draft_file}")

            # with open(draft_path, "r") as f:
            #     data = json.load(f)

            # if not isinstance(data, list) or not data:
            #     raise IngestionError("Draft history data should be a non-empty list")

            # Extract draft details from the first league entry
            draft_detail = data[0].get("draftDetail", {})
            picks = draft_detail.get("picks", [])
            teams = data[0].get("teams", [])

            logger.info(f"Loading {len(teams)} teams for season {season}...")

            # Load fantasy teams first (extract from picks data)
            teams_loaded = self._load_fantasy_teams(teams, season)
            logger.info(f"Loaded {teams_loaded} fantasy teams for season {season}")

            # Load draft picks
            picks_loaded = self._load_draft_picks(picks, season)
            logger.info(f"Loaded {picks_loaded} draft picks for season {season}")

        except (json.JSONDecodeError, KeyError) as e:
            raise IngestionError(f"Error parsing draft history data: {e}")
        except Exception as e:
            raise IngestionError(f"Unexpected error loading draft history: {e}")

    def load_rosters(self, season: int = None) -> None:
        try:
            if season is None:
                season = config.get_current_season()

            data = espn.get_rosters(season)
            if len(data) == 0:
                logger.warn("load_rosters: data is empty")
                return

            teams = data[0].get("teams", [])
            for team in teams:
                roster_entries = team.get("roster", {}).get("entries")
                espn_team_id = team.get("id")
                for roster_entry in roster_entries:
                    self._load_player_stats(roster_entry, season)
                    self._load_roster_entry(roster_entry, espn_team_id, season)

                    if season == 2024:
                        self._load_projected_ranks(roster_entry, season)

            self._reconcile_player_ranks(season)
            logger.info(f"Loaded player stats for season {season}")
        except Exception as e:
            logger.error(f"Error in load_rosters: {e}")

    def _load_player_stats(self, roster_entry: Dict[str, Any], season: int) -> None:
        """Update player fantasy score from roster entry data."""
        try:
            # Extract player ID and stats
            player_id = roster_entry.get("playerId")
            if not player_id:
                return

            player_pool_entry = roster_entry.get("playerPoolEntry", {})
            player = player_pool_entry.get("player")
            stats = player.get("stats", [])

            if not stats or len(stats) == 0:
                return

            fantasy_score = 0.0

            for stat in stats:
                if stat.get("id") == "00" + str(season):
                    fantasy_score = stat.get("appliedTotal", 0.0)

            # Update the player's fantasy score in the database
            query = """
            UPDATE players 
            SET fantasy_score = ? 
            WHERE espn_player_id = ?
            AND season = ?
            """

            with self.db.get_connection() as conn:
                cursor = conn.execute(query, (fantasy_score, player_id, season))
                if cursor.rowcount > 0:
                    conn.commit()
                    logger.debug(
                        f"Updated fantasy score {fantasy_score} for player {player_id}"
                    )
                else:
                    logger.debug(f"Player {player_id} not found in database")

        except Exception as e:
            logger.warning(f"Error updating player stats for roster entry: {e}")

    def _load_roster_entry(
        self, roster_entry: Dict[str, Any], espn_team_id: int, season: int
    ) -> None:
        """Load roster entry into rosters table."""
        try:
            # Extract player ID
            player_espn_id = roster_entry.get("playerId")
            lineup_slot_id = roster_entry.get("lineupSlotId")
            if not player_espn_id or not espn_team_id:
                return

            # Look up team_id from fantasy_teams table for this season
            team_query = (
                "SELECT id FROM fantasy_teams WHERE espn_team_id = ? AND season = ?"
            )
            team_results = self.db.execute_query(team_query, (espn_team_id, season))
            if not team_results:
                logger.debug(
                    f"Team {espn_team_id} not found in database for season {season}"
                )
                return
            team_id = team_results[0]["id"]

            # Look up player_id from players table
            player_query = "SELECT id FROM players WHERE espn_player_id = ?"
            player_results = self.db.execute_query(player_query, (player_espn_id,))
            if not player_results:
                logger.debug(f"Player {player_espn_id} not found in database")
                return
            player_id = player_results[0]["id"]

            # Insert roster entry
            roster_query = """
            INSERT OR IGNORE INTO rosters (team_id, player_id, lineup_slot_id, season)
            VALUES (?, ?, ?, ?)
            """

            with self.db.get_connection() as conn:
                cursor = conn.execute(
                    roster_query, (team_id, player_id, lineup_slot_id, season)
                )
                conn.commit()
                if cursor.rowcount > 0:
                    logger.debug(
                        f"Added roster entry: team {team_id}, player {player_id}, season {season}"
                    )

        except Exception as e:
            logger.warning(f"Error loading roster entry: {e}")

    def _reconcile_player_ranks(self, season: int) -> None:
        """
        Calculate and update player rankings based on fantasy scores.

        Updates position_rank and overall_rank for all players in the specified season:
        - position_rank: Rank within each position (QB1, QB2, RB1, RB2, etc.)
        - overall_rank: Rank across all positions (1 = best overall player)

        Args:
            season: Season year to update rankings for
        """
        try:
            logger.info(f"Calculating player rankings for season {season}")

            # Update position ranks using SQL window function
            position_rank_query = """
            UPDATE players 
            SET position_rank = (
                SELECT rank_within_position
                FROM (
                    SELECT 
                        id,
                        ROW_NUMBER() OVER (
                            PARTITION BY position 
                            ORDER BY fantasy_score DESC, name ASC
                        ) as rank_within_position
                    FROM players 
                    WHERE season = ? 
                      AND fantasy_score IS NOT NULL 
                      AND fantasy_score > 0
                ) ranked_players
                WHERE players.id = ranked_players.id
            )
            WHERE season = ? 
              AND fantasy_score IS NOT NULL 
              AND fantasy_score > 0
            """

            # Update overall ranks using SQL window function
            overall_rank_query = """
            UPDATE players 
            SET overall_rank = (
                SELECT rank_overall
                FROM (
                    SELECT 
                        id,
                        ROW_NUMBER() OVER (
                            ORDER BY fantasy_score DESC, name ASC
                        ) as rank_overall
                    FROM players 
                    WHERE season = ? 
                      AND fantasy_score IS NOT NULL 
                      AND fantasy_score > 0
                ) ranked_players
                WHERE players.id = ranked_players.id
            )
            WHERE season = ? 
              AND fantasy_score IS NOT NULL 
              AND fantasy_score > 0
            """

            with self.db.get_connection() as conn:
                # Update position ranks
                cursor = conn.execute(position_rank_query, (season, season))
                position_updates = cursor.rowcount

                # Update overall ranks
                cursor = conn.execute(overall_rank_query, (season, season))
                overall_updates = cursor.rowcount

                conn.commit()

                logger.info(
                    f"Updated rankings for season {season}: "
                    f"{position_updates} position ranks, {overall_updates} overall ranks"
                )

                # Log some sample rankings for verification
                sample_query = """
                SELECT name, position, fantasy_score, position_rank, overall_rank
                FROM players 
                WHERE season = ? 
                  AND position_rank IS NOT NULL 
                  AND overall_rank IS NOT NULL
                ORDER BY overall_rank ASC 
                LIMIT 5
                """

                sample_results = self.db.execute_query(sample_query, (season,))
                if sample_results:
                    logger.debug(f"Top 5 ranked players for season {season}:")
                    for player in sample_results:
                        logger.debug(
                            f"  {player['overall_rank']}. {player['name']} ({player['position']}#{player['position_rank']}) - {player['fantasy_score']:.1f} pts"
                        )

        except Exception as e:
            logger.error(f"Error calculating player rankings for season {season}: {e}")
            raise IngestionError(f"Failed to reconcile player ranks: {e}")

    def _load_projected_ranks(self, roster_entry: Dict[str, Any], season: int) -> None:
        """
        Insert projected rankings.

        Args:
            roster_entry: Player to insert rankings for
            season: Current fantasy season
        """
        try:
            player_id = roster_entry.get("playerId")
            if not player_id:
                return

            player_pool_entry = roster_entry.get("playerPoolEntry")
            player=player_pool_entry.get("player")
            eligible_slots = player.get("eligibleSlots")
            valid_positions = ["QB", "RB", "WR", "TE"]
            position = self._determine_position(eligible_slots)
            if position not in valid_positions:
                return

            position_lower = position.lower()
            projection_document_name = f"{season}_{position_lower}.json"
            projection_document_full_path = f"data_source/{projection_document_name}"
            if not os.path.exists(projection_document_full_path):
                return

            projection_document = {}
            with open(projection_document_full_path) as f:
                projection_document = json.loads(f.read())

            player_pool_name = player.get("fullName")
            rankings = projection_document.get("rankings")
            position_rank_label = f"{position_lower}_rank"
            overall_rank_label = "overall_rank"
            position_rank = 0
            overall_rank = 0

            for item in rankings:
                ranking_name = item.get("name")
                if ranking_name == player_pool_name:
                    position_rank = item.get(position_rank_label)
                    overall_rank = item.get(overall_rank_label)
                    break

            # Update position ranks using SQL window function
            position_rank_query = """
            UPDATE players 
            SET 
                projected_position_rank = ?,
                projected_overall_rank = ? 
            WHERE season = ? 
              AND espn_player_id = ? 
            """

            with self.db.get_connection() as conn:
                conn.execute(
                    position_rank_query,
                    (position_rank, overall_rank, season, player_id),
                )

                conn.commit()

        except Exception as e:
            logger.error(f"Error loading projected rank for season {season}: {e}")
            raise IngestionError(f"Error loading projected rank: {e}")

    def _load_fantasy_teams(self, teams: List[Dict[str, Any]], season: int) -> int:
        """Extract and load fantasy teams from draft teams."""
        teams_data = set()  # Use set to avoid duplicates

        for team in teams:
            team_id = team.get("id")
            name = team.get("name")
            wins = team.get("record").get("overall").get("wins")
            losses = team.get("record").get("overall").get("losses")
            ties = team.get("record").get("overall").get("ties")
            pointsFor = team.get("record").get("overall").get("pointsFor")
            pointsAgainst = team.get("record").get("overall").get("pointsAgainst")
            finalPosition = team.get("rankCalculatedFinal")

            if team_id:
                teams_data.add(
                    (
                        season,  # Add season as first parameter
                        team_id,
                        name,
                        wins,
                        losses,
                        ties,
                        pointsFor,
                        pointsAgainst,
                        finalPosition,
                    )
                )

        query = """
        INSERT OR IGNORE INTO fantasy_teams 
        (season, espn_team_id, name, wins, losses, ties, points_for, points_against, final_position)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.db.execute_many(query, list(teams_data))
        return len(teams_data)

    def _load_draft_picks(self, picks: List[Dict[str, Any]], season: int) -> int:
        """Load draft picks into database."""
        picks_data = []

        # Get lookup maps for player and fantasy team database IDs
        player_id_map = self._get_player_id_mapping()
        fantasy_team_id_map = self._get_fantasy_team_id_mapping(season)

        for pick in picks:
            espn_player_id = pick.get("playerId")
            espn_team_id = pick.get("teamId")

            # Look up database IDs from ESPN IDs
            db_player_id = player_id_map.get(espn_player_id)
            db_fantasy_team_id = fantasy_team_id_map.get(espn_team_id)

            # Skip picks where we can't find the required foreign key references
            if db_player_id is None:
                logger.warning(
                    f"Player ID {espn_player_id} not found in players table, skipping pick {pick.get('id')}"
                )
                continue
            if db_fantasy_team_id is None:
                logger.warning(
                    f"Fantasy team ID {espn_team_id} not found in fantasy_teams table for season {season}, skipping pick {pick.get('id')}"
                )
                continue

            picks_data.append(
                (
                    season,  # Add season as first parameter
                    pick.get("id"),
                    db_player_id,  # Use database player ID
                    db_fantasy_team_id,  # Use database fantasy team ID
                    pick.get("roundId"),
                    pick.get("roundPickNumber"),
                    pick.get("overallPickNumber"),
                    pick.get("lineupSlotId"),
                    pick.get("keeper", False),
                    pick.get("autoDraftTypeId", 0),
                )
            )

        if picks_data:
            query = """
            INSERT OR IGNORE INTO draft_picks 
            (season, espn_pick_id, player_id, fantasy_team_id, round_id, round_pick_number,
             overall_pick_number, lineup_slot_id, is_keeper, auto_draft_type_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """

            try:
                self.db.execute_many(query, picks_data)
            except Exception as e:
                # Add better error reporting for foreign key constraints
                logger.error(f"Failed to insert draft picks: {e}")
                logger.error(
                    f"Sample pick data: {picks_data[0] if picks_data else 'No data'}"
                )
                raise

        return len(picks_data)

    def load_players_data(self, season: int, force_refresh: bool = False) -> None:
        """
        Load players data from ESPN API.

        Args:
            force_refresh: Whether to refresh existing player data
        """
        try:
            # Check if we already have player data
            player_count_rows = self.db.execute_query(
                """
                SELECT COUNT(*) as count 
                FROM players
                WHERE season = ? 
            """,
                (season,),
            )
            player_count = player_count_rows[0]["count"]
            if player_count > 0 and not force_refresh:
                logger.info(
                    f"Found {player_count} existing players, skipping API call. Use force_refresh=True to reload."
                )
                return

            logger.info("Fetching player data from ESPN API...")
            players_data = espn.get_players(season)

            if not players_data:
                raise IngestionError("No player data received from ESPN API")

            with open(config.PLAYERS_CACHE_FILE, "w") as f:
                f.write(json.dumps(players_data))

            players_loaded = self._load_players(season, players_data)
            logger.info(f"Loaded {players_loaded} players from ESPN API")

        except Exception as e:
            raise IngestionError(f"Error loading players from ESPN API: {e}")

    def _load_players(self, season: int, players_data: List[Dict[str, Any]]) -> int:
        """Load players data into database."""
        players_list = []

        for player in players_data:
            # Extract player information with safe defaults
            player_id = player.get("id")
            if not player_id:
                continue

            # Get player name
            name = player.get("fullName", "") or f"Player {player_id}"

            # Get position information
            eligibility = player.get("eligibleSlots", [])
            position = self._determine_position(eligibility)
            if position == "UNKNOWN":
                continue

            # Get team information
            pro_team_id = None
            if "proTeamId" in player:
                pro_team_id = player["proTeamId"]

            # Get eligibility status
            status = player.get("injuryStatus", "ACTIVE")
            is_active = status == "ACTIVE"

            players_list.append(
                (player_id, name, position, pro_team_id, status, is_active, season)
            )

        if players_list:
            query = """
            INSERT OR REPLACE INTO players 
            (espn_player_id, name, position, nfl_team_id, eligibility_status, is_active, season)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """

            self.db.execute_many(query, players_list)

        return len(players_list)

    def _determine_position(self, eligibility: List[int]) -> str:
        """
        Determine primary position from eligibility slots.

        ESPN slot IDs:
        0: QB, 2: RB, 4: WR, 6: TE, 17: K, 16: D/ST
        """
        position_map = {0: "QB", 2: "RB", 4: "WR", 6: "TE", 17: "K", 16: "DST"}

        # Find the first recognized position
        for slot_id in eligibility:
            if slot_id in position_map:
                return position_map[slot_id]

        return "UNKNOWN"

    def _get_player_id_mapping(self) -> Dict[int, int]:
        """Get mapping of ESPN player IDs to database player IDs."""
        query = "SELECT id, espn_player_id FROM players"
        results = self.db.execute_query(query)
        return {row["espn_player_id"]: row["id"] for row in results}

    def _get_fantasy_team_id_mapping(self, season: int = None) -> Dict[int, int]:
        """Get mapping of ESPN team IDs to database fantasy team IDs."""
        if season is None:
            query = "SELECT id, espn_team_id FROM fantasy_teams"
            results = self.db.execute_query(query)
        else:
            query = "SELECT id, espn_team_id FROM fantasy_teams WHERE season = ?"
            results = self.db.execute_query(query, (season,))
        return {row["espn_team_id"]: row["id"] for row in results}

    def run_full_ingestion(
        self,
        teams_file: str = None,
        draft_file: str = None,
        force_player_refresh: bool = False,
        season: int = None,
    ) -> Dict[str, int]:
        """
        Run complete data ingestion process.

        Args:
            teams_file: Path to teams JSON file (defaults to config.TEAMS_FILE)
            draft_file: Path to draft history JSON file (defaults to config.DRAFT_HISTORY_FILE)
            force_player_refresh: Whether to refresh player data from API
            season: Season year (defaults to config.DEFAULT_SEASON)

        Returns:
            Dictionary with ingestion statistics
        """
        try:
            if season is None:
                season = config.get_current_season()

            # Load teams and games data first
            if teams_file is None:
                teams_file = config.TEAMS_FILE
            if draft_file is None:
                draft_file = config.DRAFT_HISTORY_FILE

            logger.info(f"Starting full data ingestion for season {season}...")

            self.load_teams_data(teams_file, season)

            # Load players from API BEFORE draft picks (foreign key dependency)
            self.load_players_data(season, force_player_refresh)

            # Load draft history (requires players to exist)
            self.load_draft_history(draft_file, season)

            self.load_rosters(season)

            # Get final statistics
            stats = self.db.get_database_stats()
            logger.info(f"Data ingestion completed successfully for season {season}")

            return stats

        except Exception as e:
            logger.error(f"Data ingestion failed: {e}")
            raise IngestionError(f"Full ingestion failed: {e}")


def run_ingestion(db_path: str = None, **kwargs) -> Dict[str, int]:
    """
    Convenience function to run data ingestion.

    Args:
        db_path: Path to database file (defaults to config.DATABASE_PATH)
        **kwargs: Additional arguments for run_full_ingestion

    Returns:
        Dictionary with ingestion statistics
    """
    if db_path is None:
        db_path = config.DATABASE_PATH
    db = FantasyDatabase(db_path)
    ingestion = DataIngestion(db)
    return ingestion.run_full_ingestion(**kwargs)


if __name__ == "__main__":
    # Quick test of ingestion functionality
    try:
        stats = run_ingestion()
        print("Ingestion completed. Database statistics:")
        for table, count in stats.items():
            print(f"  {table}: {count} records")
    except Exception as e:
        print(f"Ingestion failed: {e}")
