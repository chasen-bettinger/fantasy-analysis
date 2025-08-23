#!/usr/bin/env python3
"""
Test script to verify _reconcile_player_ranks method functionality.
"""

import tempfile
import os
from database import FantasyDatabase
from ingestion import DataIngestion

def test_player_ranks():
    """Test the _reconcile_player_ranks method with sample data."""
    # Create a temporary database for testing
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as temp_db:
        temp_db_path = temp_db.name
    
    try:
        # Initialize database and ingestion
        db = FantasyDatabase(temp_db_path)
        ingestion = DataIngestion(db)
        
        # Insert sample player data
        sample_players = [
            (1, "Player A", "QB", 1, "ACTIVE", True, 250.5, None, None, 2024),
            (2, "Player B", "QB", 2, "ACTIVE", True, 300.0, None, None, 2024),
            (3, "Player C", "RB", 3, "ACTIVE", True, 220.0, None, None, 2024),
            (4, "Player D", "RB", 4, "ACTIVE", True, 280.0, None, None, 2024),
            (5, "Player E", "WR", 5, "ACTIVE", True, 180.5, None, None, 2024),
            (6, "Player F", "WR", 6, "ACTIVE", True, 0.0, None, None, 2024),  # 0 score player
        ]
        
        query = """
        INSERT INTO players 
        (espn_player_id, name, position, nfl_team_id, eligibility_status, is_active, 
         fantasy_score, position_rank, overall_rank, season)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        
        db.execute_many(query, sample_players)
        
        # Test the _reconcile_player_ranks method
        print("Testing _reconcile_player_ranks method...")
        ingestion._reconcile_player_ranks(2024)
        
        # Query and verify results
        verification_query = """
        SELECT name, position, fantasy_score, position_rank, overall_rank
        FROM players 
        WHERE season = 2024
        ORDER BY overall_rank ASC NULLS LAST
        """
        
        results = db.execute_query(verification_query)
        
        print("\nRanking Results:")
        print("Overall | Position | Player Name | Position | Score")
        print("-" * 50)
        
        for player in results:
            overall = player['overall_rank'] if player['overall_rank'] else 'NULL'
            position = player['position_rank'] if player['position_rank'] else 'NULL'
            print(f"{overall:7} | {position:8} | {player['name']:11} | {player['position']:8} | {player['fantasy_score']:5.1f}")
        
        # Verify expected rankings
        expected_overall_order = ["Player B", "Player D", "Player A", "Player C", "Player E"]
        
        ranked_players = [p for p in results if p['overall_rank'] is not None]
        ranked_players.sort(key=lambda x: x['overall_rank'])
        
        print(f"\nExpected overall order: {expected_overall_order}")
        actual_overall_order = [p['name'] for p in ranked_players]
        print(f"Actual overall order:   {actual_overall_order}")
        
        if actual_overall_order == expected_overall_order:
            print("✓ Overall ranking test PASSED")
        else:
            print("✗ Overall ranking test FAILED")
        
        # Verify position rankings
        qb_players = [p for p in results if p['position'] == 'QB' and p['position_rank'] is not None]
        qb_players.sort(key=lambda x: x['position_rank'])
        
        expected_qb_order = ["Player B", "Player A"]
        actual_qb_order = [p['name'] for p in qb_players]
        
        print(f"\nExpected QB order: {expected_qb_order}")
        print(f"Actual QB order:   {actual_qb_order}")
        
        if actual_qb_order == expected_qb_order:
            print("✓ Position ranking test PASSED")
        else:
            print("✗ Position ranking test FAILED")
            
        # Check that 0-score player doesn't have ranks
        zero_score_player = [p for p in results if p['name'] == 'Player F'][0]
        if zero_score_player['overall_rank'] is None and zero_score_player['position_rank'] is None:
            print("✓ Zero-score player exclusion test PASSED")
        else:
            print("✗ Zero-score player exclusion test FAILED")
            
    finally:
        # Clean up temporary database
        if os.path.exists(temp_db_path):
            os.unlink(temp_db_path)

if __name__ == "__main__":
    test_player_ranks()