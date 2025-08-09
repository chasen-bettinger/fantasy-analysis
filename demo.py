"""
Demo script for Fantasy Football analysis tool.

Demonstrates the complete workflow from data ingestion to analysis and visualization.
"""

import logging
import time
from pathlib import Path
from typing import Dict, Any

from database import FantasyDatabase, get_database
from ingestion import DataIngestion
from queries import FantasyQueries, get_queries
from analysis import FantasyAnalysis, get_analysis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",  # level name is a standard logging format
)
logger = logging.getLogger(__name__)


def print_section_header(title: str) -> None:
    """Print a formatted section header."""
    print("\n" + "=" * 60)
    print(f" {title}")
    print("=" * 60)


def print_subsection_header(title: str) -> None:
    """Print a formatted subsection header."""
    print(f"\n--- {title} ---")


def demo_database_setup() -> FantasyDatabase:
    """Demonstrate database setup and initialization."""
    print_section_header("DATABASE SETUP")

    # Initialize database
    print("Initializing SQLite database...")
    db = get_database("demo_fantasy_football.db")

    # Show initial database stats
    stats = db.get_database_stats()
    print("Initial database state:")
    for table, count in stats.items():
        print(f"  {table}: {count} records")

    return db


def demo_data_ingestion(db: FantasyDatabase) -> Dict[str, int]:
    """Demonstrate data ingestion from JSON files and ESPN API."""
    print_section_header("DATA INGESTION")

    ingestion = DataIngestion(db)

    print("Starting data ingestion process...")
    start_time = time.time()

    try:
        # Run full ingestion
        stats = ingestion.run_full_ingestion(
            teams_file="teams.json",
            draft_file="draft_history.json",
            force_player_refresh=False,  # Don't refresh players if they exist
            # force_player_refresh=True,  # Don't refresh players if they exist
        )

        ingestion_time = time.time() - start_time

        print(f"\nData ingestion completed in {ingestion_time:.2f} seconds")
        print("Final database state:")
        for table, count in stats.items():
            print(f"  {table}: {count} records")

        return stats

    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        print(f"Error during ingestion: {e}")
        return {}


def demo_basic_queries(queries: FantasyQueries) -> None:
    """Demonstrate basic database queries."""
    print_section_header("BASIC QUERIES")

    # Database summary
    print_subsection_header("Database Summary")
    summary = queries.get_database_summary()

    print("Table counts:")
    for table, count in summary["table_counts"].items():
        print(f"  {table}: {count}")

    print("\nPosition distribution:")
    for position_info in summary["positions"][:5]:  # Show top 5
        print(f"  {position_info['position']}: {position_info['count']} players")

    # Draft picks by round
    print_subsection_header("Draft Picks by Round")
    round_1_picks = queries.get_draft_picks_by_round(round_id=1)

    if not round_1_picks.empty:
        print(f"Round 1 picks ({len(round_1_picks)} total):")
        for _, pick in round_1_picks.head(5).iterrows():
            player_name = pick.get("player_name", "Unknown Player")
            position = pick.get("position", "N/A")
            team = pick.get("nfl_team_abbrev", "N/A")
            pick_num = pick.get("overall_pick_number", "N/A")
            print(f"  Pick {pick_num}: {player_name} ({position}, {team})")
    else:
        print("No draft picks found")

    # Position analysis
    print_subsection_header("Position Analysis")
    qb_picks = queries.get_picks_by_position("QB")
    rb_picks = queries.get_picks_by_position("RB")
    wr_picks = queries.get_picks_by_position("WR")

    print(f"Quarterbacks drafted: {len(qb_picks)}")
    print(f"Running backs drafted: {len(rb_picks)}")
    print(f"Wide receivers drafted: {len(wr_picks)}")


def demo_advanced_analysis(analysis: FantasyAnalysis) -> Dict[str, Any]:
    """Demonstrate advanced statistical analysis."""
    print_section_header("ADVANCED ANALYSIS")

    try:
        # Generate comprehensive report
        print("Generating comprehensive analysis report...")
        start_time = time.time()

        report = analysis.generate_comprehensive_report(save_plots=True)

        analysis_time = time.time() - start_time
        print(f"Analysis completed in {analysis_time:.2f} seconds")

        # Display key findings
        print_subsection_header("Draft Pattern Analysis")
        draft_patterns = report.get("draft_patterns", {})
        print(f"Total picks analyzed: {draft_patterns.get('total_picks', 0)}")
        print(f"Rounds drafted: {draft_patterns.get('rounds_drafted', 0)}")
        print(f"Positions drafted: {draft_patterns.get('positions_drafted', 0)}")

        print("\nPosition distribution:")
        pos_dist = draft_patterns.get("position_distribution", {})
        for position, count in sorted(
            pos_dist.items(), key=lambda x: x[1], reverse=True
        )[:5]:
            print(f"  {position}: {count} picks")

        print("\nAverage draft position by position:")
        avg_picks = draft_patterns.get("average_pick_by_position", {})
        for position, avg in sorted(avg_picks.items(), key=lambda x: x[1])[:5]:
            print(f"  {position}: {avg:.1f}")

        # Team strategy analysis
        print_subsection_header("Team Strategy Analysis")
        team_strategies = report.get("team_strategies", {})
        print(f"Teams analyzed: {team_strategies.get('teams_analyzed', 0)}")
        print(
            f"Average picks per team: {team_strategies.get('avg_picks_per_team', 0):.1f}"
        )

        if "most_picks" in team_strategies and team_strategies["most_picks"]:
            print(f"Team with most picks: {team_strategies['most_picks']}")

        # Position scarcity analysis
        print_subsection_header("Position Scarcity Analysis")
        scarcity = report.get("position_scarcity", {})

        print("Scarcity scores (higher = more scarce):")
        scarcity_scores = scarcity.get("scarcity_score", {})
        for position, score in sorted(
            scarcity_scores.items(), key=lambda x: x[1], reverse=True
        )[:5]:
            print(f"  {position}: {score:.2f}")

        print("\nDraft urgency (% drafted in first 3 rounds):")
        urgency = scarcity.get("draft_urgency", {})
        for position, pct in sorted(urgency.items(), key=lambda x: x[1], reverse=True)[
            :5
        ]:
            print(f"  {position}: {pct:.1f}%")

        # Optional analyses
        if "keeper_analysis" in report:
            print_subsection_header("Keeper Analysis")
            keeper_data = report["keeper_analysis"]
            print(f"Total keeper picks: {keeper_data.get('total_keepers', 0)}")

            keeper_positions = keeper_data.get("keeper_positions", {})
            if keeper_positions:
                print("Keeper positions:")
                for pos, count in keeper_positions.items():
                    print(f"  {pos}: {count}")

        if "auto_draft_analysis" in report:
            print_subsection_header("Auto-Draft Analysis")
            auto_data = report["auto_draft_analysis"]
            if auto_data:
                manual_picks = sum(
                    1 for pick in auto_data if pick.get("draft_type") == "Manual"
                )
                auto_picks = sum(
                    1 for pick in auto_data if pick.get("draft_type") == "Auto"
                )
                print(f"Manual picks: {manual_picks}")
                print(f"Auto-draft picks: {auto_picks}")

        return report

    except Exception as e:
        logger.error(f"Analysis failed: {e}")
        print(f"Error during analysis: {e}")
        return {}


def demo_file_outputs(analysis: FantasyAnalysis) -> None:
    """Demonstrate generated files and outputs."""
    print_section_header("GENERATED FILES")

    output_dir = analysis.output_dir

    if output_dir.exists():
        print(f"Output directory: {output_dir}")

        files = list(output_dir.glob("*"))
        if files:
            print("Generated files:")
            for file in files:
                size_kb = file.stat().st_size / 1024
                print(f"  {file.name} ({size_kb:.1f} KB)")
        else:
            print("No output files found")
    else:
        print("Output directory not found")

    # Database file
    db_path = Path("demo_fantasy_football.db")
    if db_path.exists():
        size_kb = db_path.stat().st_size / 1024
        print(f"\nDatabase file: {db_path.name} ({size_kb:.1f} KB)")


def demo_performance_metrics() -> None:
    """Demonstrate system performance metrics."""
    print_section_header("PERFORMANCE METRICS")

    # Query performance test
    queries = get_queries("demo_fantasy_football.db")

    print("Query performance test:")

    start_time = time.time()
    summary = queries.get_database_summary()
    summary_time = time.time() - start_time
    print(f"  Database summary: {summary_time:.3f} seconds")
    print(f"  Records: {summary['table_counts']} total")

    start_time = time.time()
    all_picks = queries.get_draft_picks_by_round()
    picks_time = time.time() - start_time
    print(f"  All draft picks: {picks_time:.3f} seconds ({len(all_picks)} records)")

    start_time = time.time()
    team_summary = queries.get_team_draft_summary()
    team_time = time.time() - start_time
    print(f"  Team summary: {team_time:.3f} seconds ({len(team_summary)} teams)")

    start_time = time.time()
    position_trends = queries.get_position_draft_trends()
    trends_time = time.time() - start_time
    print(
        f"  Position trends: {trends_time:.3f} seconds ({len(position_trends)} records)"
    )


def main():
    """Run the complete demo workflow."""
    print_section_header("FANTASY FOOTBALL ANALYSIS TOOL DEMO")
    print(
        "This demo showcases the complete workflow of the Fantasy Football analysis tool."
    )
    print("It will demonstrate database setup, data ingestion, queries, and analysis.")

    try:
        # Step 1: Database setup
        db = demo_database_setup()

        # Step 2: Data ingestion
        ingestion_stats = demo_data_ingestion(db)

        if not ingestion_stats:
            print(
                "Demo cannot continue without data. Please ensure teams.json and draft_history.json exist."
            )
            return

        # Step 3: Basic queries
        queries = get_queries("demo_fantasy_football.db")
        demo_basic_queries(queries)

        # Step 4: Advanced analysis
        analysis = get_analysis("demo_fantasy_football.db")
        analysis_report = demo_advanced_analysis(analysis)

        # Step 5: Show generated files
        demo_file_outputs(analysis)

        # Step 6: Performance metrics
        demo_performance_metrics()

        # Summary
        print_section_header("DEMO SUMMARY")
        print("Demo completed successfully!")
        print(f"Database records: {sum(ingestion_stats.values())}")
        print(f"Analysis components: {len(analysis_report)}")
        print("\nNext steps:")
        print(
            "1. Explore the generated visualizations in the 'analysis_output' directory"
        )
        print("2. Review the analysis summary text file")
        print("3. Examine the SQLite database with your preferred database tool")
        print("4. Run individual analysis functions for specific insights")
        print("5. Modify the code to add your own analysis functions")

    except KeyboardInterrupt:
        print("\nDemo interrupted by user")
    except Exception as e:
        logger.error(f"Demo failed: {e}")
        print(f"Demo failed with error: {e}")
        print("Please check the log messages above for more details")


if __name__ == "__main__":
    main()
