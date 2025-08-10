#!/usr/bin/env python3
"""
Test script for the new team performance vs scores analysis function.

This script tests the analyze_team_performance_vs_scores() method to ensure
it works correctly with the existing database.
"""

import sys
import json
import numpy as np
import pandas as pd
from pathlib import Path

# Add the project directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent))

from analysis import get_analysis
from config import config


def main():
    """Test the team performance vs scores analysis."""

    print("Testing Team Performance vs Scores Analysis")
    print("=" * 50)

    try:
        # Get analysis instance
        analysis = get_analysis()

        # Run the team performance analysis
        print("Running team performance vs scores analysis...")
        results = analysis.analyze_team_performance_vs_scores(save_plots=True)

        # Print results summary
        print("\nAnalysis Results:")
        print(f"Teams analyzed: {results.get('teams_analyzed', 0)}")
        print(f"Average total fantasy score: {results.get('avg_total_score', 0):.2f}")
        print(f"Score standard deviation: {results.get('std_total_score', 0):.2f}")

        # Print score range
        score_range = results.get("score_range", {})
        print(
            f"Score range: {score_range.get('min', 0):.1f} - {score_range.get('max', 0):.1f}"
        )

        # Print correlations
        print("\nKey Correlations with Final Position:")
        correlations = results.get("correlations", {})
        for metric, corr in correlations.items():
            if "vs_position" in metric and "total" in metric:
                print(f"  {metric}: {corr:.3f}")

        # Print position contributions
        print("\nPosition Contributions (Top vs Bottom Teams):")
        contributions = results.get("position_contributions", {})
        for pos, data in contributions.items():
            diff = data.get("difference_total", 0)
            print(f"  {pos}: {diff:+.1f} point difference")

        # Print final position breakdown
        print("\nBreakdown by Final Position:")
        by_position = results.get("by_final_position", {})
        for position in sorted(by_position.keys()):
            data = by_position[position]
            print(
                f"  Position {position}: {data.get('avg_total_score', 0):.1f} avg score, "
                f"{data.get('avg_win_pct', 0):.3f} win rate"
            )

        # Check if plots were saved
        output_dir = Path(config.OUTPUT_DIR)
        plot_file = output_dir / "team_performance_vs_scores.png"
        if plot_file.exists():
            print(f"\nPlots saved successfully to: {plot_file}")
        else:
            print(f"\nWarning: Plot file not found at {plot_file}")

        print("\n" + "=" * 50)
        print("Analysis completed successfully!")

        # Optionally save results to JSON for inspection
        output_file = f"{output_dir}/team_performance_results.json"
        try:
            def convert_numpy_types(obj):
                """
                Recursively convert numpy types to native Python types.
                This handles both dictionary keys and values.
                """
                if isinstance(obj, np.integer):
                    return int(obj)
                elif isinstance(obj, np.floating):
                    return float(obj)
                elif isinstance(obj, np.ndarray):
                    return obj.tolist()
                elif isinstance(obj, dict):
                    # Convert both keys and values
                    return {
                        convert_numpy_types(k): convert_numpy_types(v) 
                        for k, v in obj.items()
                    }
                elif isinstance(obj, (list, tuple)):
                    # Convert list/tuple elements
                    return [convert_numpy_types(item) for item in obj]
                else:
                    return obj

            # Convert all numpy types to native Python types
            clean_results = convert_numpy_types(results)
            
            with open(output_file, "w") as f:
                json.dump(clean_results, f, indent=2)
            print(f"Results saved to: {output_file}")
        except Exception as e:
            print(f"Warning: Could not save JSON results: {e}")

    except Exception as e:
        print(f"Error during analysis: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
