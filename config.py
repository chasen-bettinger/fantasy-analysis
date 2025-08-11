"""
Configuration management for Fantasy Football analysis tool.

Loads settings from environment variables and .env file with sensible defaults.
"""

import os
from pathlib import Path
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()


class Config:
    """Configuration class for Fantasy Football analysis tool."""

    # Database Configuration
    DATABASE_PATH: str = os.getenv("DATABASE_PATH", "fantasy_football.db")
    DATABASE_TIMEOUT: int = int(os.getenv("DATABASE_TIMEOUT", "30"))

    # ESPN API Configuration
    ESPN_LEAGUE_ID: str = os.getenv("ESPN_LEAGUE_ID", "730477")
    ESPN_SEASON: str = os.getenv("ESPN_SEASON", "2015")  # Default season for backward compatibility
    ESPN_TEAM_ID: str = os.getenv("ESPN_TEAM_ID", "10")
    
    # Multi-season Configuration
    SUPPORTED_SEASONS: List[int] = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024]
    DEFAULT_SEASON: int = int(os.getenv("DEFAULT_SEASON", ESPN_SEASON))
    
    @classmethod
    def get_current_season(cls) -> int:
        """Get the current default season as integer."""
        return cls.DEFAULT_SEASON
    
    @classmethod
    def get_all_seasons(cls) -> List[int]:
        """Get list of all supported seasons."""
        return cls.SUPPORTED_SEASONS.copy()
    
    @classmethod
    def is_valid_season(cls, season: int) -> bool:
        """Check if a season is valid/supported."""
        return season in cls.SUPPORTED_SEASONS

    # ESPN API Headers
    ESPN_USER_AGENT: str = os.getenv(
        "ESPN_USER_AGENT",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:139.0) Gecko/20100101 Firefox/139.0",
    )
    ESPN_SWID: Optional[str] = os.getenv("ESPN_SWID")  # ESPN authentication cookie
    ESPN_S2: Optional[str] = os.getenv("ESPN_S2")  # ESPN authentication cookie

    # Data File Paths
    TEAMS_FILE: str = os.getenv("TEAMS_FILE", "teams.json")
    DRAFT_HISTORY_FILE: str = os.getenv("DRAFT_HISTORY_FILE", "draft_history.json")
    PLAYERS_CACHE_FILE: str = os.getenv("PLAYERS_CACHE_FILE", "players_data.json")

    # Output Configuration
    OUTPUT_DIR: str = os.getenv("OUTPUT_DIR", "analysis_output")
    SAVE_PLOTS: bool = os.getenv("SAVE_PLOTS", "true").lower() == "true"
    PLOT_DPI: int = int(os.getenv("PLOT_DPI", "300"))

    # Logging Configuration
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
    LOG_FORMAT: str = os.getenv(
        "LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )

    # API Rate Limiting
    API_RATE_LIMIT_DELAY: float = float(os.getenv("API_RATE_LIMIT_DELAY", "1.0"))
    API_TIMEOUT: int = int(os.getenv("API_TIMEOUT", "30"))

    # Analysis Configuration
    FORCE_PLAYER_REFRESH: bool = (
        os.getenv("FORCE_PLAYER_REFRESH", "false").lower() == "true"
    )
    ANALYSIS_CACHE_TTL: int = int(os.getenv("ANALYSIS_CACHE_TTL", "3600"))  # 1 hour

    @classmethod
    def get_espn_headers(cls) -> Dict[str, str]:
        """Get ESPN API headers with authentication."""
        headers = {
            "User-Agent": cls.ESPN_USER_AGENT,
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "X-Fantasy-Source": "kona",
            "X-Fantasy-Filter": '{"players":{}}',
            "X-Fantasy-Platform": "kona-PROD-871ba974fde0504c7ee3018049a715c0af70b886",
            "Origin": "https://fantasy.espn.com",
            "DNT": "1",
            "Connection": "keep-alive",
            "Referer": "https://fantasy.espn.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Sec-GPC": "1",
            "Pragma": "no-cache",
            "Cache-Control": "no-cache",
            "TE": "trailers",
        }

        # Add authentication cookies if available
        cookie_parts = []
        if cls.ESPN_SWID:
            cookie_parts.append(f"SWID={cls.ESPN_SWID}")
        if cls.ESPN_S2:
            cookie_parts.append(f"espn_s2={cls.ESPN_S2}")

        if cookie_parts:
            headers["Cookie"] = "; ".join(cookie_parts)

        return headers

    @classmethod
    def get_roster_url(cls, season: Optional[int] = None) -> str:
        """Get ESPN roster API URL."""
        if season is None:
            season = cls.DEFAULT_SEASON
        return (
            f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/leagueHistory/{cls.ESPN_LEAGUE_ID}"
            f"?rosterForTeamId=1&rosterForTeamId=2&rosterForTeamId=3&rosterForTeamId=4&rosterForTeamId=5"
            f"&rosterForTeamId=6&rosterForTeamId=7&rosterForTeamId=8&rosterForTeamId=9&rosterForTeamId=10"
            f"&rosterForTeamId=11&rosterForTeamId=12&view=mRoster&seasonId={season}"
        )

    @classmethod
    def get_draft_history_url(cls, season: Optional[int] = None) -> str:
        """Get ESPN draft history API URL."""
        if season is None:
            season = cls.DEFAULT_SEASON
        return (
            f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/leagueHistory/{cls.ESPN_LEAGUE_ID}"
            f"?view=mDraftDetail&view=mLiveScoring&view=mMatchupScore"
            f"&view=mPendingTransactions&view=mPositionalRatings&view=mSettings"
            f"&view=mTeam&view=modular&view=mNav&seasonId={season}"
        )

    @classmethod
    def get_players_url(cls, season: Optional[int] = None) -> str:
        """Get ESPN players API URL."""
        if season is None:
            season = cls.DEFAULT_SEASON
        return (
            f"https://lm-api-reads.fantasy.espn.com/apis/v3/games/ffl/seasons/{season}/players"
            f"?scoringPeriodId=0&view=players_wl"
        )

    @classmethod
    def validate_config(cls) -> bool:
        """Validate critical configuration settings."""
        required_settings = [
            ("ESPN_LEAGUE_ID", cls.ESPN_LEAGUE_ID),
            ("ESPN_SEASON", cls.ESPN_SEASON),
            ("DATABASE_PATH", cls.DATABASE_PATH),
        ]

        missing = []
        for name, value in required_settings:
            if not value:
                missing.append(name)

        if missing:
            raise ValueError(f"Missing required configuration: {', '.join(missing)}")

        return True

    @classmethod
    def ensure_directories(cls) -> None:
        """Ensure required directories exist."""
        output_path = Path(cls.OUTPUT_DIR)
        output_path.mkdir(exist_ok=True)

        # Ensure parent directory of database exists
        db_path = Path(cls.DATABASE_PATH)
        db_path.parent.mkdir(parents=True, exist_ok=True)

    @classmethod
    def get_summary(cls) -> Dict[str, Any]:
        """Get configuration summary for debugging."""
        return {
            "database_path": cls.DATABASE_PATH,
            "espn_league_id": cls.ESPN_LEAGUE_ID,
            "espn_season": cls.ESPN_SEASON,
            "default_season": cls.DEFAULT_SEASON,
            "supported_seasons": cls.SUPPORTED_SEASONS,
            "teams_file": cls.TEAMS_FILE,
            "draft_file": cls.DRAFT_HISTORY_FILE,
            "output_dir": cls.OUTPUT_DIR,
            "log_level": cls.LOG_LEVEL,
            "has_espn_auth": bool(cls.ESPN_SWID and cls.ESPN_S2),
        }


# Create default config instance
config = Config()

# Validate configuration on import
try:
    config.validate_config()
    config.ensure_directories()
except ValueError as e:
    print(f"Configuration error: {e}")
    print("Please check your .env file or environment variables")
