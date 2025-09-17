import duckdb

def main():
    conn = duckdb.connect()
    conn.sql("IMPORT DATABASE 'data'");

    conn.sql("""
        select
            *
        from
            player_performance pp join player p on pp.player_id = p.id
        where
            week = 2 and benched = true
        order by
            pp.actual_points desc
    """).show()

if __name__ == "__main__":
    main()
