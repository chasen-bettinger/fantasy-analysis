"""
Data ingestion module for Fantasy Football analysis tool.

Handles loading data from JSON files and ESPN API into the SQLite database.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Any

from config import config
from database import FantasyDatabase
import espn

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()),
    format=config.LOG_FORMAT
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

    def load_teams_data(self, teams_file: str = None) -> None:
        """
        Load NFL teams and games data from teams file.

        Args:
            teams_file: Path to teams JSON file (defaults to config.TEAMS_FILE)
        """
        try:
            if teams_file is None:
                teams_file = config.TEAMS_FILE
            teams_path = Path(teams_file)
            if not teams_path.exists():
                raise IngestionError(f"Teams file not found: {teams_file}")

            with open(teams_path, "r") as f:
                data = json.load(f)

            pro_teams = data.get("proTeams", [])
            logger.info(f"Loading {len(pro_teams)} NFL teams")

            # Load teams
            teams_loaded = self._load_nfl_teams(pro_teams)
            logger.info(f"Loaded {teams_loaded} NFL teams")

            # Load games
            games_loaded = self._load_games(pro_teams)
            logger.info(f"Loaded {games_loaded} games")

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

    def _load_games(self, pro_teams: List[Dict[str, Any]]) -> int:
        """Load games from pro teams data."""
        games_data = []

        for team in pro_teams:
            games_by_period = team.get("proGamesByScoringPeriod", {})

            for games in games_by_period.values():
                for game in games:
                    games_data.append(
                        (
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
        unique_games = {game[0]: game for game in games_data}.values()

        query = """
        INSERT OR IGNORE INTO games 
        (espn_game_id, home_team_id, away_team_id, game_date, scoring_period_id,
         start_time_tbd, stats_official, valid_for_locking)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """

        self.db.execute_many(query, list(unique_games))
        return len(unique_games)

    def load_draft_history(self, draft_file: str = None) -> None:
        """
        Load draft history data from JSON file.

        Args:
            draft_file: Path to draft history JSON file (defaults to config.DRAFT_HISTORY_FILE)
        """
        try:
            if draft_file is None:
                draft_file = config.DRAFT_HISTORY_FILE
            draft_path = Path(draft_file)
            if not draft_path.exists():
                raise IngestionError(f"Draft history file not found: {draft_file}")

            with open(draft_path, "r") as f:
                data = json.load(f)

            if not isinstance(data, list) or not data:
                raise IngestionError("Draft history data should be a non-empty list")

            # Extract draft details from the first league entry
            draft_detail = data[0].get("draftDetail", {})
            picks = draft_detail.get("picks", [])
            teams = data[0].get("teams", [])

            logger.info(f"Loading {len(teams)} teams...")

            # Load fantasy teams first (extract from picks data)
            teams_loaded = self._load_fantasy_teams(teams)
            logger.info(f"Loaded {teams_loaded} fantasy teams")

            # Load draft picks
            picks_loaded = self._load_draft_picks(picks)
            logger.info(f"Loaded {picks_loaded} draft picks")

        except (json.JSONDecodeError, KeyError) as e:
            raise IngestionError(f"Error parsing draft history data: {e}")
        except Exception as e:
            raise IngestionError(f"Unexpected error loading draft history: {e}")

    def _load_fantasy_teams(self, teams: List[Dict[str, Any]]) -> int:
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

            if team_id:
                teams_data.add(
                    (team_id, name, wins, losses, ties, pointsFor, pointsAgainst)
                )

        query = """
        INSERT OR IGNORE INTO fantasy_teams 
        (espn_team_id, name, wins, losses, ties, points_for, points_against)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """

        self.db.execute_many(query, list(teams_data))
        return len(teams_data)

    def _load_draft_picks(self, picks: List[Dict[str, Any]]) -> int:
        """Load draft picks into database."""
        picks_data = []

        # Get lookup maps for player and fantasy team database IDs
        player_id_map = self._get_player_id_mapping()
        fantasy_team_id_map = self._get_fantasy_team_id_mapping()

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
                    f"Fantasy team ID {espn_team_id} not found in fantasy_teams table, skipping pick {pick.get('id')}"
                )
                continue

            picks_data.append(
                (
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
            (espn_pick_id, player_id, fantasy_team_id, round_id, round_pick_number,
             overall_pick_number, lineup_slot_id, is_keeper, auto_draft_type_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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

    def load_players_data(self, force_refresh: bool = False) -> None:
        """
        Load players data from ESPN API.

        Args:
            force_refresh: Whether to refresh existing player data
        """
        try:
            # Check if we already have player data
            player_count = self.db.get_table_count("players")
            if player_count > 0 and not force_refresh:
                logger.info(
                    f"Found {player_count} existing players, skipping API call. Use force_refresh=True to reload."
                )
                return

            logger.info("Fetching player data from ESPN API...")
            players_data = espn.get_players()

            if not players_data:
                raise IngestionError("No player data received from ESPN API")

            with open(config.PLAYERS_CACHE_FILE, "w") as f:
                f.write(json.dumps(players_data))

            players_loaded = self._load_players(players_data)
            logger.info(f"Loaded {players_loaded} players from ESPN API")

        except Exception as e:
            raise IngestionError(f"Error loading players from ESPN API: {e}")

    def _load_players(self, players_data: List[Dict[str, Any]]) -> int:
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
                (player_id, name, position, pro_team_id, status, is_active)
            )

        if players_list:
            query = """
            INSERT OR REPLACE INTO players 
            (espn_player_id, name, position, nfl_team_id, eligibility_status, is_active)
            VALUES (?, ?, ?, ?, ?, ?)
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

    def _get_fantasy_team_id_mapping(self) -> Dict[int, int]:
        """Get mapping of ESPN team IDs to database fantasy team IDs."""
        query = "SELECT id, espn_team_id FROM fantasy_teams"
        results = self.db.execute_query(query)
        return {row["espn_team_id"]: row["id"] for row in results}

    def run_full_ingestion(
        self,
        teams_file: str = None,
        draft_file: str = None,
        force_player_refresh: bool = False,
    ) -> Dict[str, int]:
        """
        Run complete data ingestion process.

        Args:
            teams_file: Path to teams JSON file (defaults to config.TEAMS_FILE)
            draft_file: Path to draft history JSON file (defaults to config.DRAFT_HISTORY_FILE)
            force_player_refresh: Whether to refresh player data from API

        Returns:
            Dictionary with ingestion statistics
        """
        logger.info("Starting full data ingestion...")

        try:
            # Load teams and games data first
            if teams_file is None:
                teams_file = config.TEAMS_FILE
            if draft_file is None:
                draft_file = config.DRAFT_HISTORY_FILE
                
            self.load_teams_data(teams_file)

            # Load players from API BEFORE draft picks (foreign key dependency)
            self.load_players_data(force_player_refresh)

            # Load draft history (requires players to exist)
            self.load_draft_history(draft_file)

            # Get final statistics
            stats = self.db.get_database_stats()
            logger.info("Data ingestion completed successfully")

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
