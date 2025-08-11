"""
Main entry point for Fantasy Football analysis tool.

Provides command-line interface for various operations including data ingestion,
analysis, and database management.
"""

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from config import config
from database import get_database, DatabaseError
from ingestion import run_ingestion, IngestionError
from queries import get_queries, QueryError
from analysis import get_analysis, AnalysisError

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL.upper()), format=config.LOG_FORMAT
)
logger = logging.getLogger(__name__)


def setup_database(db_path: str) -> None:
    """Initialize database and show status."""
    try:
        print(f"Setting up database: {db_path}")
        db = get_database(db_path)
        stats = db.get_database_stats()

        print("Database initialized successfully!")
        print("Current state:")
        for table, count in stats.items():
            print(f"  {table}: {count} records")

    except DatabaseError as e:
        print(f"Database setup failed: {e}")
        sys.exit(1)


def ingest_data(
    db_path: str, teams_file: str, draft_file: str, force_refresh: bool = False, season: Optional[int] = None
) -> None:
    """Run data ingestion process."""
    try:
        season_msg = f" for season {season}" if season else ""
        print(f"Starting data ingestion{season_msg}...")
        print(f"  Teams file: {teams_file}")
        print(f"  Draft file: {draft_file}")
        print(f"  Force player refresh: {force_refresh}")
        if season:
            print(f"  Season: {season}")

        # Check if files exist
        if not Path(teams_file).exists():
            print(f"Error: Teams file not found: {teams_file}")
            sys.exit(1)

        if not Path(draft_file).exists():
            print(f"Error: Draft file not found: {draft_file}")
            sys.exit(1)

        stats = run_ingestion(
            db_path=db_path,
            teams_file=teams_file,
            draft_file=draft_file,
            force_player_refresh=force_refresh,
            season=season,
        )

        print("Data ingestion completed successfully!")
        print("Final database state:")
        for table, count in stats.items():
            print(f"  {table}: {count} records")

    except IngestionError as e:
        print(f"Data ingestion failed: {e}")
        sys.exit(1)


def run_queries(db_path: str, query_type: Optional[str] = None, season: Optional[int] = None) -> None:
    """Run database queries and display results."""
    try:
        queries = get_queries(db_path)
        season_msg = f" for season {season}" if season else ""

        if query_type == "summary" or query_type is None:
            print(f"=== Database Summary{season_msg} ===")
            summary = queries.get_database_summary()

            print("Table counts:")
            for table, count in summary["table_counts"].items():
                print(f"  {table}: {count}")

            print("\nPosition distribution:")
            for pos_info in summary["positions"][:10]:
                print(f"  {pos_info['position']}: {pos_info['count']}")

        if query_type == "draft" or query_type is None:
            print(f"\n=== Draft Analysis{season_msg} ===")
            round_1 = queries.get_draft_picks_by_round(round_id=1, season=season)

            if not round_1.empty:
                print(f"Round 1 picks ({len(round_1)} total):")
                for _, pick in round_1.head(10).iterrows():
                    player = pick.get("player_name", "Unknown")
                    pos = pick.get("position", "N/A")
                    team = pick.get("nfl_team_abbrev", "N/A")
                    pick_num = pick.get("overall_pick_number", "N/A")
                    season_info = pick.get("season", "N/A")
                    print(f"  Pick {pick_num}: {player} ({pos}, {team}) [{season_info}]")
            else:
                print("No draft picks found")

        if query_type == "positions":
            print(f"\n=== Position Analysis{season_msg} ===")
            for position in ["QB", "RB", "WR", "TE", "K", "DST"]:
                picks = queries.get_picks_by_position(position, season=season)
                print(f"  {position}: {len(picks)} drafted")

    except QueryError as e:
        print(f"Query execution failed: {e}")
        sys.exit(1)


def run_analysis(
    db_path: str, analysis_type: Optional[str] = None, save_plots: bool = True, season: Optional[int] = None
) -> None:
    """Run statistical analysis and generate reports."""
    try:
        analysis = get_analysis(db_path)
        season_msg = f" for season {season}" if season else ""

        if analysis_type == "patterns" or analysis_type is None:
            print(f"=== Draft Pattern Analysis{season_msg} ===")
            patterns = analysis.analyze_draft_patterns(save_plots=save_plots, season=season)

            print(f"Total picks: {patterns.get('total_picks', 0)}")
            print(f"Rounds: {patterns.get('rounds_drafted', 0)}")
            print(f"Positions: {patterns.get('positions_drafted', 0)}")

            pos_dist = patterns.get("position_distribution", {})
            print("\nPosition distribution:")
            for pos, count in sorted(
                pos_dist.items(), key=lambda x: x[1], reverse=True
            ):
                print(f"  {pos}: {count}")

        if analysis_type == "scarcity" or analysis_type is None:
            print(f"\n=== Position Scarcity Analysis{season_msg} ===")
            scarcity = analysis.analyze_position_scarcity(save_plots=save_plots, season=season)

            scores = scarcity.get("scarcity_score", {})
            print("Scarcity scores (higher = more scarce):")
            for pos, score in sorted(scores.items(), key=lambda x: x[1], reverse=True):
                print(f"  {pos}: {score:.2f}")

        if analysis_type == "full":
            print(f"\n=== Comprehensive Analysis{season_msg} ===")
            # Note: generate_comprehensive_report doesn't have season parameter yet, but individual analyses do
            report = analysis.generate_comprehensive_report(save_plots=save_plots)

            print(f"Analysis components: {len(report)}")
            print("Generated comprehensive analysis report with visualizations")

            if save_plots:
                print(f"Output directory: {analysis.output_dir}")

    except AnalysisError as e:
        print(f"Analysis failed: {e}")
        sys.exit(1)


def show_status(db_path: str) -> None:
    """Show current database status and statistics."""
    try:
        db = get_database(db_path)
        queries = get_queries(db_path)

        print(f"Database: {db_path}")
        print(f"Database exists: {Path(db_path).exists()}")

        if Path(db_path).exists():
            size_kb = Path(db_path).stat().st_size / 1024
            print(f"Database size: {size_kb:.1f} KB")

            stats = db.get_database_stats()
            print("\nTable statistics:")
            for table, count in stats.items():
                print(f"  {table}: {count} records")

            # Quick data quality check
            summary = queries.get_database_summary()
            positions = summary.get("positions", [])
            rounds = summary.get("rounds", [])

            if positions:
                print(f"\nPositions represented: {len(positions)}")
            if rounds:
                print(f"Draft rounds: {len(rounds)}")

    except (DatabaseError, QueryError) as e:
        print(f"Status check failed: {e}")


def main():
    """Main entry point with command-line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Fantasy Football Analysis Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py setup                           # Initialize database
  python main.py ingest                         # Load data from JSON files
  python main.py ingest --season 2016           # Load data for 2016 season
  python main.py query --type summary           # Show database summary
  python main.py query --type draft -s 2015     # Show 2015 draft analysis
  python main.py analyze --type patterns        # Analyze draft patterns (all seasons)
  python main.py analyze --type patterns -s 2016 # Analyze 2016 draft patterns only
  python main.py analyze --type full --no-plots # Full analysis without plots
  python main.py status                         # Show database status
        """,
    )

    parser.add_argument(
        "command",
        choices=["setup", "ingest", "query", "analyze", "status"],
        help="Command to execute",
    )

    parser.add_argument(
        "--db",
        default=config.DATABASE_PATH,
        help=f"Database file path (default: {config.DATABASE_PATH})",
    )

    parser.add_argument(
        "--teams-file",
        default=config.TEAMS_FILE,
        help=f"Teams JSON file path (default: {config.TEAMS_FILE})",
    )

    parser.add_argument(
        "--draft-file",
        default=config.DRAFT_HISTORY_FILE,
        help=f"Draft history JSON file path (default: {config.DRAFT_HISTORY_FILE})",
    )

    parser.add_argument("--type", help="Specific type of query or analysis to run")

    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Force refresh of player data from ESPN API",
    )

    parser.add_argument(
        "--no-plots", action="store_true", help="Skip plot generation in analysis"
    )
    
    parser.add_argument(
        "--season", "-s", type=int, help="Specific season year to process (e.g., 2015, 2016)"
    )

    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Execute command
    try:
        if args.command == "setup":
            setup_database(args.db)

        elif args.command == "ingest":
            ingest_data(args.db, args.teams_file, args.draft_file, args.force_refresh, args.season)

        elif args.command == "query":
            run_queries(args.db, args.type, args.season)

        elif args.command == "analyze":
            run_analysis(args.db, args.type, not args.no_plots, args.season)

        elif args.command == "status":
            show_status(args.db)

    except KeyboardInterrupt:
        print("\nOperation interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
