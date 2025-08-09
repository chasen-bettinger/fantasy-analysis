"""
Analysis module for Fantasy Football analysis tool.

Provides statistical analysis and visualization functions for draft and player data.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

from queries import FantasyQueries, QueryError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set style for plots
plt.style.use("default")
sns.set_palette("husl")


class AnalysisError(Exception):
    """Custom exception for analysis operations."""

    pass


class FantasyAnalysis:
    """Statistical analysis and visualization for fantasy football data."""

    def __init__(self, queries: FantasyQueries):
        """
        Initialize analysis with queries instance.

        Args:
            queries: FantasyQueries instance
        """
        self.queries = queries
        self.output_dir = Path("analysis_output")
        self.output_dir.mkdir(exist_ok=True)

    def analyze_draft_patterns(self, save_plots: bool = True) -> Dict[str, Any]:
        """
        Analyze draft patterns and trends.

        Args:
            save_plots: Whether to save generated plots

        Returns:
            Dictionary with analysis results
        """
        try:
            logger.info("Analyzing draft patterns...")

            # Get data
            draft_picks = self.queries.get_draft_picks_by_round()
            position_trends = self.queries.get_position_draft_trends()

            if draft_picks.empty:
                raise AnalysisError("No draft pick data available for analysis")

            # Calculate statistics
            analysis_results = {
                "total_picks": len(draft_picks),
                "rounds_drafted": draft_picks["round_id"].nunique(),
                "positions_drafted": draft_picks["position"].nunique(),
                "position_distribution": draft_picks["position"]
                .value_counts()
                .to_dict(),
                "average_pick_by_position": draft_picks.groupby("position")[
                    "overall_pick_number"
                ]
                .mean()
                .to_dict(),
                "earliest_pick_by_position": draft_picks.groupby("position")[
                    "overall_pick_number"
                ]
                .min()
                .to_dict(),
            }

            if save_plots:
                self._plot_draft_patterns(draft_picks, position_trends)

            logger.info("Draft pattern analysis completed")
            return analysis_results

        except QueryError as e:
            raise AnalysisError(f"Failed to analyze draft patterns: {e}")
        except Exception as e:
            raise AnalysisError(f"Unexpected error in draft pattern analysis: {e}")

    def _plot_draft_patterns(
        self, draft_picks: pd.DataFrame, position_trends: pd.DataFrame
    ) -> None:
        """Create visualizations for draft patterns."""

        # Create figure with subplots
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle("Draft Pattern Analysis", fontsize=16, fontweight="bold")

        # 1. Position distribution pie chart
        position_counts = draft_picks["position"].value_counts()
        axes[0, 0].pie(
            position_counts.values, labels=position_counts.index, autopct="%1.1f%%"
        )
        axes[0, 0].set_title("Position Distribution in Draft")

        # 2. Draft picks by round heatmap
        round_position = (
            draft_picks.groupby(["round_id", "position"]).size().unstack(fill_value=0)
        )
        if not round_position.empty:
            sns.heatmap(
                round_position, annot=True, fmt="d", cmap="YlOrRd", ax=axes[0, 1]
            )
            axes[0, 1].set_title("Picks by Round and Position")
            axes[0, 1].set_xlabel("Position")
            axes[0, 1].set_ylabel("Round")

        # 3. Average draft position by position
        avg_adp = (
            draft_picks.groupby("position")["overall_pick_number"].mean().sort_values()
        )
        axes[1, 0].bar(avg_adp.index, avg_adp.values)
        axes[1, 0].set_title("Average Draft Position by Position")
        axes[1, 0].set_xlabel("Position")
        axes[1, 0].set_ylabel("Average Overall Pick")
        axes[1, 0].tick_params(axis="x", rotation=45)

        # 4. Draft timeline
        if (
            "round_id" in draft_picks.columns
            and "overall_pick_number" in draft_picks.columns
        ):
            for position in draft_picks["position"].unique():
                if pd.notna(position):
                    pos_data = draft_picks[draft_picks["position"] == position]
                    axes[1, 1].scatter(
                        pos_data["overall_pick_number"],
                        pos_data["round_id"],
                        label=position,
                        alpha=0.7,
                    )

        axes[1, 1].set_title("Draft Picks by Overall Position and Round")
        axes[1, 1].set_xlabel("Overall Pick Number")
        axes[1, 1].set_ylabel("Round")
        axes[1, 1].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        plt.tight_layout()
        plt.savefig(
            self.output_dir / "draft_patterns.png", dpi=300, bbox_inches="tight"
        )
        plt.close()

        logger.info(f"Draft pattern plots saved to {self.output_dir}")

    def analyze_team_strategies(self, save_plots: bool = True) -> Dict[str, Any]:
        """
        Analyze fantasy team drafting strategies.

        Args:
            save_plots: Whether to save generated plots

        Returns:
            Dictionary with team strategy analysis
        """
        try:
            logger.info("Analyzing team strategies...")

            team_summary = self.queries.get_team_draft_summary()

            if team_summary.empty:
                raise AnalysisError("No team data available for strategy analysis")

            # Calculate strategy metrics
            strategy_analysis = {
                "teams_analyzed": len(team_summary),
                "position_strategies": self._analyze_position_strategies(team_summary),
            }

            if save_plots:
                self._plot_team_strategies(team_summary)

            logger.info("Team strategy analysis completed")
            return strategy_analysis

        except QueryError as e:
            raise AnalysisError(f"Failed to analyze team strategies: {e}")
        except Exception as e:
            raise AnalysisError(f"Unexpected error in team strategy analysis: {e}")

    def _analyze_position_strategies(
        self, team_summary: pd.DataFrame
    ) -> Dict[str, Any]:
        """Analyze position-based drafting strategies."""
        position_cols = [
            "qb_picks",
            "rb_picks",
            "wr_picks",
            "te_picks",
            "k_picks",
            "dst_picks",
        ]

        strategies = {}
        for col in position_cols:
            if col in team_summary.columns:
                position = col.replace("_picks", "").upper()
                strategies[position] = {
                    "avg_picks": team_summary[col].mean(),
                    "max_picks": team_summary[col].max(),
                    "teams_with_none": (team_summary[col] == 0).sum(),
                }

        return strategies

    def _plot_team_strategies(self, team_summary: pd.DataFrame) -> None:
        """Create visualizations for team strategies."""

        fig, axes = plt.subplots(1, 2, figsize=(15, 6))
        fig.suptitle("Fantasy Team Draft Strategies", fontsize=16, fontweight="bold")

        # 1. Position distribution stacked bar
        position_cols = [
            "qb_picks",
            "rb_picks",
            "wr_picks",
            "te_picks",
            "k_picks",
            "dst_picks",
        ]
        available_cols = [col for col in position_cols if col in team_summary.columns]

        if available_cols:
            team_summary[available_cols].plot(kind="bar", stacked=True, ax=axes[0])
            axes[0].set_title("Position Distribution by Team")
            axes[0].set_xlabel("Team Index")
            axes[0].set_ylabel("Number of Picks")
            axes[0].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        # 2. Pick range analysis
        if all(col in team_summary.columns for col in ["earliest_pick", "latest_pick"]):
            pick_range = team_summary["latest_pick"] - team_summary["earliest_pick"]
            axes[1].bar(range(len(team_summary)), pick_range)
            axes[1].set_title("Draft Pick Range by Team")
            axes[1].set_xlabel("Team Index")
            axes[1].set_ylabel("Pick Range (Latest - Earliest)")

        plt.tight_layout()
        plt.savefig(
            self.output_dir / "team_strategies.png", dpi=300, bbox_inches="tight"
        )
        plt.close()

        logger.info(f"Team strategy plots saved to {self.output_dir}")

    def analyze_position_scarcity(self, save_plots: bool = True) -> Dict[str, Any]:
        """
        Analyze position scarcity and draft timing.

        Args:
            save_plots: Whether to save generated plots

        Returns:
            Dictionary with position scarcity analysis
        """
        try:
            logger.info("Analyzing position scarcity...")

            picks_by_position = self.queries.get_picks_by_position()
            position_trends = self.queries.get_position_draft_trends()

            if picks_by_position.empty:
                raise AnalysisError("No position data available for scarcity analysis")

            # Calculate scarcity metrics
            scarcity_analysis = {
                "position_counts": picks_by_position["position"]
                .value_counts()
                .to_dict(),
                "scarcity_score": self._calculate_scarcity_score(picks_by_position),
                "draft_urgency": self._calculate_draft_urgency(picks_by_position),
                "position_depth": self._analyze_position_depth(position_trends),
            }

            if save_plots:
                self._plot_position_scarcity(picks_by_position, position_trends)

            logger.info("Position scarcity analysis completed")
            return scarcity_analysis

        except QueryError as e:
            raise AnalysisError(f"Failed to analyze position scarcity: {e}")
        except Exception as e:
            raise AnalysisError(f"Unexpected error in position scarcity analysis: {e}")

    def _calculate_scarcity_score(self, picks_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate scarcity score based on draft timing."""
        scarcity_scores = {}

        for position in picks_data["position"].unique():
            if pd.notna(position):
                pos_picks = picks_data[picks_data["position"] == position][
                    "overall_pick_number"
                ]
                if not pos_picks.empty:
                    # Lower average pick number = higher scarcity
                    avg_pick = pos_picks.mean()
                    scarcity_scores[position] = 100.0 / avg_pick if avg_pick > 0 else 0

        return scarcity_scores

    def _calculate_draft_urgency(self, picks_data: pd.DataFrame) -> Dict[str, float]:
        """Calculate draft urgency based on early-round selections."""
        urgency_scores = {}

        for position in picks_data["position"].unique():
            if pd.notna(position):
                pos_picks = picks_data[picks_data["position"] == position]
                early_picks = pos_picks[pos_picks["round_id"] <= 3]  # First 3 rounds

                if not pos_picks.empty:
                    urgency_scores[position] = len(early_picks) / len(pos_picks) * 100

        return urgency_scores

    def _analyze_position_depth(self, position_trends: pd.DataFrame) -> Dict[str, Any]:
        """Analyze position depth across rounds."""
        depth_analysis = {}

        if not position_trends.empty:
            for position in position_trends["position"].unique():
                if pd.notna(position):
                    pos_data = position_trends[position_trends["position"] == position]
                    depth_analysis[position] = {
                        "rounds_drafted": len(pos_data),
                        "total_picks": pos_data["picks_count"].sum(),
                        "avg_picks_per_round": pos_data["picks_count"].mean(),
                        "peak_round": (
                            pos_data.loc[pos_data["picks_count"].idxmax(), "round_id"]
                            if len(pos_data) > 0
                            else None
                        ),
                    }

        return depth_analysis

    def _plot_position_scarcity(
        self, picks_data: pd.DataFrame, trends_data: pd.DataFrame
    ) -> None:
        """Create visualizations for position scarcity."""

        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle("Position Scarcity Analysis", fontsize=16, fontweight="bold")

        # 1. Position count
        position_counts = picks_data["position"].value_counts()
        axes[0, 0].bar(position_counts.index, position_counts.values)
        axes[0, 0].set_title("Total Picks by Position")
        axes[0, 0].set_xlabel("Position")
        axes[0, 0].set_ylabel("Number of Picks")

        # 2. Average draft position by position
        avg_adp = (
            picks_data.groupby("position")["overall_pick_number"].mean().sort_values()
        )
        axes[0, 1].bar(avg_adp.index, avg_adp.values, color="orange")
        axes[0, 1].set_title("Average Draft Position by Position")
        axes[0, 1].set_xlabel("Position")
        axes[0, 1].set_ylabel("Average Overall Pick")

        # 3. Draft distribution by round
        if not trends_data.empty:
            pivot_data = trends_data.pivot(
                index="round_id", columns="position", values="picks_count"
            ).fillna(0)
            pivot_data.plot(kind="bar", ax=axes[1, 0])
            axes[1, 0].set_title("Position Picks by Round")
            axes[1, 0].set_xlabel("Round")
            axes[1, 0].set_ylabel("Number of Picks")
            axes[1, 0].legend(bbox_to_anchor=(1.05, 1), loc="upper left")

        # 4. Early round focus (first 3 rounds)
        early_rounds = picks_data[picks_data["round_id"] <= 3]
        early_counts = early_rounds["position"].value_counts()
        axes[1, 1].pie(
            early_counts.values, labels=early_counts.index, autopct="%1.1f%%"
        )
        axes[1, 1].set_title("Early Round Position Focus (Rounds 1-3)")

        plt.tight_layout()
        plt.savefig(
            self.output_dir / "position_scarcity.png", dpi=300, bbox_inches="tight"
        )
        plt.close()

        logger.info(f"Position scarcity plots saved to {self.output_dir}")

    def generate_comprehensive_report(self, save_plots: bool = True) -> Dict[str, Any]:
        """
        Generate a comprehensive analysis report.

        Args:
            save_plots: Whether to save generated plots

        Returns:
            Dictionary with comprehensive analysis results
        """
        try:
            logger.info("Generating comprehensive analysis report...")

            report = {
                "database_summary": self.queries.get_database_summary(),
                "draft_patterns": self.analyze_draft_patterns(save_plots),
                "team_strategies": self.analyze_team_strategies(save_plots),
                "position_scarcity": self.analyze_position_scarcity(save_plots),
            }

            # Add additional analyses
            try:
                keeper_data = self.queries.get_keeper_analysis()
                if not keeper_data.empty:
                    report["keeper_analysis"] = {
                        "total_keepers": len(keeper_data),
                        "keeper_positions": keeper_data["position"]
                        .value_counts()
                        .to_dict(),
                        "keeper_rounds": keeper_data["round_id"]
                        .value_counts()
                        .to_dict(),
                    }
            except QueryError:
                logger.warning("Keeper analysis failed - no keeper data available")

            try:
                auto_draft_data = self.queries.get_auto_draft_analysis()
                if not auto_draft_data.empty:
                    report["auto_draft_analysis"] = auto_draft_data.to_dict("records")
            except QueryError:
                logger.warning("Auto-draft analysis failed")

            # Save report summary
            if save_plots:
                self._save_report_summary(report)

            logger.info("Comprehensive analysis report completed")
            return report

        except Exception as e:
            raise AnalysisError(f"Failed to generate comprehensive report: {e}")

    def _save_report_summary(self, report: Dict[str, Any]) -> None:
        """Save a text summary of the analysis report."""
        summary_file = self.output_dir / "analysis_summary.txt"

        with open(summary_file, "w") as f:
            f.write("Fantasy Football Draft Analysis Report\n")
            f.write("=" * 50 + "\n\n")

            # Database summary
            db_summary = report.get("database_summary", {})
            f.write("Database Summary:\n")
            for table, count in db_summary.get("table_counts", {}).items():
                f.write(f"  {table}: {count} records\n")
            f.write("\n")

            # Draft patterns
            draft_patterns = report.get("draft_patterns", {})
            f.write("Draft Patterns:\n")
            f.write(f"  Total picks: {draft_patterns.get('total_picks', 0)}\n")
            f.write(f"  Rounds: {draft_patterns.get('rounds_drafted', 0)}\n")
            f.write(f"  Positions: {draft_patterns.get('positions_drafted', 0)}\n")
            f.write("\n")

            # Position distribution
            f.write("Position Distribution:\n")
            for pos, count in draft_patterns.get("position_distribution", {}).items():
                f.write(f"  {pos}: {count} picks\n")
            f.write("\n")

            # Team strategies
            team_strategies = report.get("team_strategies", {})
            f.write("Team Strategies:\n")
            f.write(f"  Teams analyzed: {team_strategies.get('teams_analyzed', 0)}\n")
            f.write(
                f"  Average picks per team: {team_strategies.get('avg_picks_per_team', 0):.1f}\n"
            )
            f.write("\n")

            # Position scarcity
            scarcity = report.get("position_scarcity", {})
            f.write("Position Scarcity Scores:\n")
            for pos, score in scarcity.get("scarcity_score", {}).items():
                f.write(f"  {pos}: {score:.2f}\n")
            f.write("\n")

            f.write(f"Report generated on: {pd.Timestamp.now()}\n")

        logger.info(f"Analysis summary saved to {summary_file}")


def get_analysis(db_path: str = "fantasy_football.db") -> FantasyAnalysis:
    """
    Convenience function to get a FantasyAnalysis instance.

    Args:
        db_path: Path to database file

    Returns:
        FantasyAnalysis instance
    """
    from database import FantasyDatabase
    from queries import FantasyQueries

    db = FantasyDatabase(db_path)
    queries = FantasyQueries(db)
    return FantasyAnalysis(queries)


if __name__ == "__main__":
    # Quick test of analysis functionality
    try:
        analysis = get_analysis()
        report = analysis.generate_comprehensive_report()

        print("Analysis completed successfully!")
        print(
            f"Total picks analyzed: {report.get('draft_patterns', {}).get('total_picks', 0)}"
        )
        print(
            f"Teams analyzed: {report.get('team_strategies', {}).get('teams_analyzed', 0)}"
        )

    except Exception as e:
        print(f"Analysis failed: {e}")
