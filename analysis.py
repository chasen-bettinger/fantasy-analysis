"""
Analysis module for Fantasy Football analysis tool.

Provides statistical analysis and visualization functions for draft and player data.
"""

import logging
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

from config import config
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
        self.output_dir = Path(config.OUTPUT_DIR)
        self.output_dir.mkdir(exist_ok=True)

    def analyze_draft_patterns(self, save_plots: bool = True, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Analyze draft patterns and trends.

        Args:
            save_plots: Whether to save generated plots
            season: Specific season to analyze (None for all seasons)

        Returns:
            Dictionary with analysis results
        """
        try:
            logger.info("Analyzing draft patterns...")

            # Get data
            draft_picks = self.queries.get_draft_picks_by_round(season=season)
            position_trends = self.queries.get_position_draft_trends(season=season)

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

    def analyze_team_strategies(self, save_plots: bool = True, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Analyze fantasy team drafting strategies.

        Args:
            save_plots: Whether to save generated plots
            season: Specific season to analyze (None for all seasons)

        Returns:
            Dictionary with team strategy analysis
        """
        try:
            logger.info("Analyzing team strategies...")

            team_summary = self.queries.get_team_draft_summary(season=season)

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

    def analyze_position_scarcity(self, save_plots: bool = True, season: Optional[int] = None) -> Dict[str, Any]:
        """
        Analyze position scarcity and draft timing.

        Args:
            save_plots: Whether to save generated plots
            season: Specific season to analyze (None for all seasons)

        Returns:
            Dictionary with position scarcity analysis
        """
        try:
            logger.info("Analyzing position scarcity...")

            picks_by_position = self.queries.get_picks_by_position(season=season)
            position_trends = self.queries.get_position_draft_trends(season=season)

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

    def analyze_team_performance_vs_scores(
        self, save_plots: bool = True, only_starters: bool = True, season: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Analyze the relationship between player fantasy scores and team final standings.

        This analysis examines how total team fantasy scores correlate with final position,
        which positions contribute most to successful teams, and other performance insights.

        Args:
            save_plots: Whether to save generated plots
            only_starters: Whether to include only starting lineup players
            season: Specific season to analyze (None for all seasons)

        Returns:
            Dictionary with team performance vs scores analysis
        """
        try:
            logger.info("Analyzing team performance vs fantasy scores...")

            # Get team performance data by joining fantasy_teams, draft_picks, and players
            team_scores_query = """
            SELECT 
                ft.name as fantasy_team_name,
                ft.final_position,
                ft.wins,
                ft.losses,
                ft.ties,
                ft.points_for,
                ft.points_against,
                SUM(p.fantasy_score) as total_fantasy_score,
                AVG(p.fantasy_score) as avg_fantasy_score,
                SUM(CASE WHEN p.position = 'QB' THEN p.fantasy_score ELSE 0 END) as qb_score,
                SUM(CASE WHEN p.position = 'RB' THEN p.fantasy_score ELSE 0 END) as rb_score,
                SUM(CASE WHEN p.position = 'WR' THEN p.fantasy_score ELSE 0 END) as wr_score,
                SUM(CASE WHEN p.position = 'TE' THEN p.fantasy_score ELSE 0 END) as te_score,
                SUM(CASE WHEN p.position = 'K' THEN p.fantasy_score ELSE 0 END) as k_score,
                SUM(CASE WHEN p.position = 'DST' THEN p.fantasy_score ELSE 0 END) as dst_score,
                COUNT(CASE WHEN p.position = 'QB' THEN 1 END) as qb_picks,
                COUNT(CASE WHEN p.position = 'RB' THEN 1 END) as rb_picks,
                COUNT(CASE WHEN p.position = 'WR' THEN 1 END) as wr_picks,
                COUNT(CASE WHEN p.position = 'TE' THEN 1 END) as te_picks,
                COUNT(CASE WHEN p.position = 'K' THEN 1 END) as k_picks,
                COUNT(CASE WHEN p.position = 'DST' THEN 1 END) as dst_picks
            FROM rosters r 
            LEFT JOIN fantasy_teams ft on ft.id=r.team_id 
            LEFT JOIN players p on p.id=r.player_id
            WHERE ft.final_position IS NOT NULL
            """

            if only_starters:
                team_scores_query += (
                    " AND (r.lineup_slot_id <= 17 or r.lineup_slot_id == 23)"
                )

            team_scores_query += " GROUP BY ft.id, ft.name, ft.final_position, ft.wins, ft.losses, ft.ties, ft.points_for, ft.points_against"
            team_scores_query += " ORDER BY ft.final_position"

            results = self.queries.db.execute_query(team_scores_query)
            team_performance_df = pd.DataFrame([dict(row) for row in results])

            if team_performance_df.empty:
                raise AnalysisError("No team performance data available for analysis")

            # Calculate additional metrics
            team_performance_df["win_percentage"] = (
                team_performance_df["wins"]
                / (
                    team_performance_df["wins"]
                    + team_performance_df["losses"]
                    + team_performance_df["ties"]
                )
            ).fillna(0)

            # Calculate position averages per pick
            for pos in ["qb", "rb", "wr", "te", "k", "dst"]:
                picks_col = f"{pos}_picks"
                score_col = f"{pos}_score"
                avg_col = f"{pos}_avg_score"
                team_performance_df[avg_col] = (
                    team_performance_df[score_col] / team_performance_df[picks_col]
                ).fillna(0)

            # Statistical analysis
            analysis_results = self._calculate_performance_statistics(
                team_performance_df
            )

            # Position contribution analysis
            position_contributions = self._analyze_position_contributions(
                team_performance_df
            )
            analysis_results["position_contributions"] = position_contributions

            # Correlation analysis
            correlations = self._calculate_performance_correlations(team_performance_df)
            analysis_results["correlations"] = correlations

            if save_plots:
                self._plot_team_performance_analysis(team_performance_df)

            logger.info("Team performance vs scores analysis completed")
            return analysis_results

        except Exception as e:
            raise AnalysisError(f"Failed to analyze team performance vs scores: {e}")

    def _calculate_performance_statistics(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate basic statistics for team performance analysis."""
        stats = {
            "teams_analyzed": len(df),
            "avg_total_score": df["total_fantasy_score"].mean(),
            "std_total_score": df["total_fantasy_score"].std(),
            "score_range": {
                "min": df["total_fantasy_score"].min(),
                "max": df["total_fantasy_score"].max(),
            },
            "by_final_position": {},
        }

        # Analysis by final position
        for position in sorted(df["final_position"].unique()):
            pos_data = df[df["final_position"] == position]
            stats["by_final_position"][position] = {
                "team_count": len(pos_data),
                "avg_total_score": pos_data["total_fantasy_score"].mean(),
                "avg_win_pct": pos_data["win_percentage"].mean(),
                "avg_points_for": pos_data["points_for"].mean(),
            }

        return stats

    def _analyze_position_contributions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze how different positions contribute to team success."""
        positions = ["qb", "rb", "wr", "te", "k", "dst"]
        contributions = {}

        # Split teams into top and bottom performers
        median_position = df["final_position"].median()
        top_teams = df[df["final_position"] <= median_position]
        bottom_teams = df[df["final_position"] > median_position]

        for pos in positions:
            score_col = f"{pos}_score"
            avg_col = f"{pos}_avg_score"

            contributions[pos.upper()] = {
                "top_teams_total": top_teams[score_col].mean(),
                "bottom_teams_total": bottom_teams[score_col].mean(),
                "top_teams_avg_per_pick": top_teams[avg_col].mean(),
                "bottom_teams_avg_per_pick": bottom_teams[avg_col].mean(),
                "difference_total": top_teams[score_col].mean()
                - bottom_teams[score_col].mean(),
                "difference_per_pick": top_teams[avg_col].mean()
                - bottom_teams[avg_col].mean(),
            }

        return contributions

    def _calculate_performance_correlations(self, df: pd.DataFrame) -> Dict[str, float]:
        """Calculate correlations between various metrics and final position."""
        # Note: Lower final position = better performance, so negative correlations are good
        correlations = {}

        def safe_corr(x, y):
            """Calculate correlation safely, handling NaN values."""
            try:
                corr = x.corr(y)
                return corr if pd.notna(corr) else 0.0
            except (ValueError, TypeError):
                return 0.0

        # Overall performance correlations
        correlations["total_score_vs_position"] = safe_corr(
            df["total_fantasy_score"], df["final_position"]
        )
        correlations["avg_score_vs_position"] = safe_corr(
            df["avg_fantasy_score"], df["final_position"]
        )
        correlations["win_pct_vs_position"] = safe_corr(
            df["win_percentage"], df["final_position"]
        )
        correlations["points_for_vs_position"] = safe_corr(
            df["points_for"], df["final_position"]
        )

        # Position-specific correlations
        positions = ["qb", "rb", "wr", "te", "k", "dst"]
        for pos in positions:
            score_col = f"{pos}_score"
            avg_col = f"{pos}_avg_score"
            correlations[f"{pos}_total_vs_position"] = safe_corr(
                df[score_col], df["final_position"]
            )
            correlations[f"{pos}_avg_vs_position"] = safe_corr(
                df[avg_col], df["final_position"]
            )

        return correlations

    def _plot_team_performance_analysis(self, df: pd.DataFrame) -> None:
        """Create visualizations for team performance analysis."""
        fig, axes = plt.subplots(3, 2, figsize=(16, 18))
        fig.suptitle(
            "Team Performance vs Fantasy Scores Analysis",
            fontsize=16,
            fontweight="bold",
        )

        # 1. Total fantasy score vs final position
        axes[0, 0].scatter(
            df["total_fantasy_score"], df["final_position"], alpha=0.7, s=80
        )
        axes[0, 0].set_xlabel("Total Fantasy Score")
        axes[0, 0].set_ylabel("Final Position (1 = Best)")
        axes[0, 0].set_title("Total Fantasy Score vs Final Position")
        axes[0, 0].invert_yaxis()  # Lower position numbers at top

        # Add trendline if data is suitable
        if (
            len(df) > 1
            and df["total_fantasy_score"].std() > 0
            and df["final_position"].std() > 0
        ):
            try:
                z = np.polyfit(df["total_fantasy_score"], df["final_position"], 1)
                p = np.poly1d(z)
                axes[0, 0].plot(
                    df["total_fantasy_score"],
                    p(df["total_fantasy_score"]),
                    "r--",
                    alpha=0.8,
                )
            except (np.linalg.LinAlgError, ValueError):
                pass  # Skip trendline if calculation fails

        # 2. Average fantasy scores by final position
        position_avgs = df.groupby("final_position")["total_fantasy_score"].mean()
        axes[0, 1].bar(position_avgs.index, position_avgs.values, alpha=0.7)
        axes[0, 1].set_xlabel("Final Position")
        axes[0, 1].set_ylabel("Average Total Fantasy Score")
        axes[0, 1].set_title("Average Fantasy Score by Final Position")

        # 3. Position contribution comparison (top vs bottom teams)
        positions = ["QB", "RB", "WR", "TE", "K", "DST"]
        median_pos = df["final_position"].median()
        top_teams = df[df["final_position"] <= median_pos]
        bottom_teams = df[df["final_position"] > median_pos]

        top_scores = [top_teams[f"{pos.lower()}_score"].mean() for pos in positions]
        bottom_scores = [
            bottom_teams[f"{pos.lower()}_score"].mean() for pos in positions
        ]

        x = range(len(positions))
        width = 0.35
        axes[1, 0].bar(
            [i - width / 2 for i in x],
            top_scores,
            width,
            label="Top Half Teams",
            alpha=0.7,
        )
        axes[1, 0].bar(
            [i + width / 2 for i in x],
            bottom_scores,
            width,
            label="Bottom Half Teams",
            alpha=0.7,
        )
        axes[1, 0].set_xlabel("Position")
        axes[1, 0].set_ylabel("Average Fantasy Score")
        axes[1, 0].set_title("Position Scores: Top vs Bottom Teams")
        axes[1, 0].set_xticks(x)
        axes[1, 0].set_xticklabels(positions)
        axes[1, 0].legend()

        # 4. Win percentage vs total fantasy score
        axes[1, 1].scatter(
            df["total_fantasy_score"], df["win_percentage"], alpha=0.7, s=80
        )
        axes[1, 1].set_xlabel("Total Fantasy Score")
        axes[1, 1].set_ylabel("Win Percentage")
        axes[1, 1].set_title("Win Percentage vs Total Fantasy Score")

        # Add trendline if data is suitable
        if (
            len(df) > 1
            and df["total_fantasy_score"].std() > 0
            and df["win_percentage"].std() > 0
        ):
            try:
                z = np.polyfit(df["total_fantasy_score"], df["win_percentage"], 1)
                p = np.poly1d(z)
                axes[1, 1].plot(
                    df["total_fantasy_score"],
                    p(df["total_fantasy_score"]),
                    "r--",
                    alpha=0.8,
                )
            except (np.linalg.LinAlgError, ValueError):
                pass  # Skip trendline if calculation fails

        # 5. Points for vs fantasy score
        axes[2, 0].scatter(df["total_fantasy_score"], df["points_for"], alpha=0.7, s=80)
        axes[2, 0].set_xlabel("Total Fantasy Score")
        axes[2, 0].set_ylabel("Points For")
        axes[2, 0].set_title("Points For vs Total Fantasy Score")

        # Add trendline if data is suitable
        if (
            len(df) > 1
            and df["total_fantasy_score"].std() > 0
            and df["points_for"].std() > 0
        ):
            try:
                z = np.polyfit(df["total_fantasy_score"], df["points_for"], 1)
                p = np.poly1d(z)
                axes[2, 0].plot(
                    df["total_fantasy_score"],
                    p(df["total_fantasy_score"]),
                    "r--",
                    alpha=0.8,
                )
            except (np.linalg.LinAlgError, ValueError):
                pass  # Skip trendline if calculation fails

        # 6. Correlation heatmap of key metrics
        try:
            corr_columns = [
                "final_position",
                "total_fantasy_score",
                "win_percentage",
                "points_for",
                "qb_score",
                "rb_score",
                "wr_score",
                "te_score",
            ]
            available_columns = [col for col in corr_columns if col in df.columns]
            corr_data = df[available_columns].corr()

            im = axes[2, 1].imshow(corr_data.values, cmap="RdBu_r", vmin=-1, vmax=1)
            axes[2, 1].set_xticks(range(len(corr_data.columns)))
            axes[2, 1].set_yticks(range(len(corr_data.columns)))
            axes[2, 1].set_xticklabels(corr_data.columns, rotation=45, ha="right")
            axes[2, 1].set_yticklabels(corr_data.columns)
            axes[2, 1].set_title("Correlation Matrix: Key Performance Metrics")

            # Add correlation values as text
            for i in range(len(corr_data.columns)):
                for j in range(len(corr_data.columns)):
                    value = corr_data.iloc[i, j]
                    if pd.notna(value):
                        axes[2, 1].text(
                            j, i, f"{value:.2f}", ha="center", va="center", fontsize=8
                        )
        except Exception:
            # If correlation heatmap fails, just show a simple text
            axes[2, 1].text(
                0.5,
                0.5,
                "Correlation analysis\nnot available",
                ha="center",
                va="center",
                transform=axes[2, 1].transAxes,
            )
            axes[2, 1].set_title("Correlation Matrix: Key Performance Metrics")

        plt.tight_layout()
        plt.savefig(
            self.output_dir / "team_performance_vs_scores.png",
            dpi=300,
            bbox_inches="tight",
        )
        plt.close()

        logger.info(f"Team performance analysis plots saved to {self.output_dir}")

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
                "team_performance_vs_scores": self.analyze_team_performance_vs_scores(
                    save_plots
                ),
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


def get_analysis(db_path: str = None) -> FantasyAnalysis:
    """
    Convenience function to get a FantasyAnalysis instance.

    Args:
        db_path: Path to database file (defaults to config.DATABASE_PATH)

    Returns:
        FantasyAnalysis instance
    """
    from database import FantasyDatabase
    from queries import FantasyQueries

    if db_path is None:
        db_path = config.DATABASE_PATH

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
