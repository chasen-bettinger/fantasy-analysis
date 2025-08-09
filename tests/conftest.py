"""
Pytest configuration and shared fixtures.
"""

import pytest
import tempfile
import os
import sys
from pathlib import Path

# Add the parent directory to the Python path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))


@pytest.fixture
def temp_db_path():
    """Provide a temporary database file path."""
    temp_db = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
    temp_db.close()
    yield temp_db.name
    # Cleanup
    if os.path.exists(temp_db.name):
        os.unlink(temp_db.name)


@pytest.fixture
def sample_teams_data():
    """Provide sample teams data for testing."""
    return {
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


@pytest.fixture
def sample_draft_data():
    """Provide sample draft data for testing."""
    return [
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


@pytest.fixture
def sample_players_data():
    """Provide sample players data for testing."""
    return [
        {
            "id": 20001,
            "fullName": "Test QB",
            "eligibleSlots": [0],  # QB
            "proTeamId": 1,
            "injuryStatus": "ACTIVE"
        },
        {
            "id": 20002,
            "fullName": "Test RB",
            "eligibleSlots": [2],  # RB
            "proTeamId": 2,
            "injuryStatus": "ACTIVE"
        },
        {
            "id": 20003,
            "fullName": "Test WR",
            "eligibleSlots": [4],  # WR
            "proTeamId": 1,
            "injuryStatus": "QUESTIONABLE"
        }
    ]