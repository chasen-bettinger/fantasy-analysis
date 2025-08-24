sqlite3 -header -csv demo_fantasy_football.db "select p.*, t.name from players p left join nfl_teams t on p.nfl_team_id=t.id where position = 'RB' and season = 2024 ORDER BY fantasy_score DESC;" > rbs.csv

sqlite3 -header -csv demo_fantasy_football.db "select p.name, p.fantasy_score, t.name from players p left join nfl_teams t on p.nfl_team_id=t.id where position = 'RB' and season = 2024 ORDER BY fantasy_score DESC;" > rbs.csv

sqlite3 -header -csv demo_fantasy_football.db "select p.name, p.fantasy_score, t.name from players p left join nfl_teams t on p.nfl_team_id=t.id where position = 'QB' and season = 2024 ORDER BY fantasy_score DESC;" > qbs.csv

sqlite3 -header -csv demo_fantasy_football.db "select p.name, p.fantasy_score, t.name from players p left join nfl_teams t on p.nfl_team_id=t.id where position = 'WR' and season = 2024 ORDER BY fantasy_score DESC;" > wrs.csv

sqlite3 -header -csv demo_fantasy_football.db "select p.name, p.fantasy_score, t.name, (p.fantasy_score/18) as avg from players p left join nfl_teams t on p.nfl_team_id=t.id where position = 'TE' and season = 2024 ORDER BY fantasy_score DESC;" > tes.csv

select dp.overall_pick_number, p.name from draft_picks dp left join players p on p.id=dp.player_id where season=2024 and fantasy_team_id = 114;
