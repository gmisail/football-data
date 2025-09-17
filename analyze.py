import duckdb

def main():
    conn = duckdb.connect()
    conn.sql("IMPORT DATABASE 'data'");

    # Calculate point differentials
    conn.sql("""
        select
            m.week,
            home_team.name, home_box.actual_score,
            away_team.name, away_box.actual_score,
            abs(home_box.actual_score - away_box.actual_score) as differential
        from
            match m join match_team home_box on m.week = home_box.week and m.home_team_id = home_box.team_id
                    join match_team away_box on m.week = away_box.week and m.away_team_id = away_box.team_id
                    join team home_team on home_team.id = home_box.team_id
                    join team away_team on away_team.id = away_box.team_id
        where
            home_box.actual_score > 0.0 and away_box.actual_score > 0.0
        order by
            differential desc
    """).show()

if __name__ == "__main__":
    main()
