#!/usr/bin/env python3
"""
Test script for ESPN API client.

Allows direct testing of ESPNClient methods without going through the full application.
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any

from espn import ESPNClient
from config import config


def setup_logging(verbose: bool = False) -> None:
    """Configure logging for the test script."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )


def print_config_summary() -> None:
    """Print current configuration summary."""
    print("ESPN API Configuration:")
    print(f"  League ID: {config.ESPN_LEAGUE_ID}")
    print(f"  Season: {config.ESPN_SEASON}")
    print(f"  Team ID: {config.ESPN_TEAM_ID}")
    print(f"  Has Authentication: {bool(config.ESPN_SWID and config.ESPN_S2)}")
    print(f"  Rate Limit Delay: {config.API_RATE_LIMIT_DELAY}s")
    print(f"  API Timeout: {config.API_TIMEOUT}s")
    print()


def analyze_data(data: Dict[str, Any], data_type: str) -> None:
    """Analyze and print summary of the fetched data."""
    print(f"{data_type.title()} Data Summary:")
    print(f"  Type: {type(data)}")
    
    if isinstance(data, dict):
        print(f"  Top-level keys: {list(data.keys())}")
        
        if data_type == "draft_history":
            if isinstance(data, list) and data:
                first_item = data[0]
                if "draftDetail" in first_item:
                    draft_detail = first_item["draftDetail"]
                    picks = draft_detail.get("picks", [])
                    print(f"  Draft picks found: {len(picks)}")
                    if picks:
                        print(f"  Sample pick keys: {list(picks[0].keys())}")
                
                if "teams" in first_item:
                    teams = first_item["teams"]
                    print(f"  Teams found: {len(teams)}")
            
        elif data_type == "players":
            if isinstance(data, list):
                print(f"  Total players: {len(data)}")
                if data:
                    sample_player = data[0]
                    print(f"  Sample player keys: {list(sample_player.keys())}")
                    
                    # Count positions
                    positions = {}
                    for player in data[:100]:  # Sample first 100
                        slots = player.get("eligibleSlots", [])
                        if slots:
                            # Map slot IDs to positions
                            slot_map = {0: "QB", 2: "RB", 4: "WR", 6: "TE", 17: "K", 16: "DST"}
                            for slot in slots:
                                if slot in slot_map:
                                    pos = slot_map[slot]
                                    positions[pos] = positions.get(pos, 0) + 1
                                    break
                    
                    print(f"  Position sample: {positions}")
    
    # Calculate data size
    data_str = json.dumps(data)
    size_kb = len(data_str.encode('utf-8')) / 1024
    print(f"  Data size: {size_kb:.1f} KB")
    print()


def save_data(data: Dict[str, Any], data_type: str) -> None:
    """Save data to a JSON file."""
    filename = f"espn_{data_type}_{config.ESPN_SEASON}_{config.ESPN_LEAGUE_ID}.json"
    filepath = Path(filename)
    
    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Data saved to: {filepath}")
        print(f"File size: {filepath.stat().st_size / 1024:.1f} KB")
        print()
    except Exception as e:
        print(f"Failed to save data to {filepath}: {e}")


def test_draft_history(client: ESPNClient, save_output: bool = False) -> bool:
    """Test the draft history endpoint."""
    print("Testing Draft History Endpoint...")
    print("-" * 40)
    
    try:
        data = client.get_draft_history()
        print("✅ Draft history fetch successful!")
        analyze_data(data, "draft_history")
        
        if save_output:
            save_data(data, "draft_history")
        
        return True
        
    except Exception as e:
        print(f"❌ Draft history fetch failed: {e}")
        return False


def test_players(client: ESPNClient, save_output: bool = False) -> bool:
    """Test the players endpoint."""
    print("Testing Players Endpoint...")
    print("-" * 40)
    
    try:
        data = client.get_players()
        print("✅ Players fetch successful!")
        analyze_data(data, "players")
        
        if save_output:
            save_data(data, "players")
        
        return True
        
    except Exception as e:
        print(f"❌ Players fetch failed: {e}")
        return False


def main():
    """Main test function with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Test ESPN API client functionality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python test_espn.py --test-draft --verbose
  python test_espn.py --test-players --save-output  
  python test_espn.py --test-both --verbose --save-output
        """
    )
    
    parser.add_argument(
        '--test-draft',
        action='store_true',
        help='Test draft history endpoint'
    )
    
    parser.add_argument(
        '--test-players', 
        action='store_true',
        help='Test players endpoint'
    )
    
    parser.add_argument(
        '--test-both',
        action='store_true',
        help='Test both endpoints'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose logging'
    )
    
    parser.add_argument(
        '--save-output',
        action='store_true',
        help='Save raw JSON responses to files'
    )
    
    args = parser.parse_args()
    
    # Set default if no specific test requested
    if not (args.test_draft or args.test_players or args.test_both):
        args.test_both = True
    
    # Setup logging
    setup_logging(args.verbose)
    
    # Print configuration
    print_config_summary()
    
    # Create ESPN client
    try:
        client = ESPNClient()
        print("✅ ESPN client created successfully")
        print()
    except Exception as e:
        print(f"❌ Failed to create ESPN client: {e}")
        sys.exit(1)
    
    # Run tests
    success_count = 0
    total_tests = 0
    
    if args.test_draft or args.test_both:
        total_tests += 1
        if test_draft_history(client, args.save_output):
            success_count += 1
    
    if args.test_players or args.test_both:
        total_tests += 1
        if test_players(client, args.save_output):
            success_count += 1
    
    # Print summary
    print("Test Summary:")
    print(f"  Tests run: {total_tests}")
    print(f"  Successful: {success_count}")
    print(f"  Failed: {total_tests - success_count}")
    
    if success_count == total_tests:
        print("🎉 All tests passed!")
        sys.exit(0)
    else:
        print("⚠️  Some tests failed")
        sys.exit(1)


if __name__ == "__main__":
    main()