import duckdb

def main():
    conn = duckdb.connect()
    conn.sql("IMPORT DATABASE 'data'")

    conn.sql("""
        create table proj_point_differential as (
            select
                box.week,
                box.team_id,
                box.actual_score - box.projected_score as differential
            from
                match_team box
            where
                box.actual_score > 0.0 and box.projected_score > 0.0
            order by
                differential desc
        )
    """)

    #
    # Best performance
    #
    conn.sql("""
        select
            t.name, pj.week, pj.differential
        from
            proj_point_differential pj join team t on pj.team_id = t.id
        order by
            pj.differential desc
        limit 1
    """).show()

    #
    # Worst performance
    #
    conn.sql("""
        select
            t.name, pj.week, pj.differential
        from
            proj_point_differential pj join team t on pj.team_id = t.id
        order by
            pj.differential asc
        limit 1
    """).show()

    #
    # Most points for
    #
    conn.sql("""
        with team_score as (
            select
                mt.team_id,
                sum(mt.actual_score) as points
            from
                match_team mt
            group by
                mt.team_id
        )
        select
            t.name,
            ts.points
        from
            team t join team_score ts on t.id = ts.team_id
        order by
            points desc
    """).show()

    #
    # Most points against
    #
    conn.sql("""
        select
            t.name,
            sum(
                case
                    when t.id = m.home_team_id
                    then away.actual_score
                    else home.actual_score
                end
            ) as points_against
        from
            team t
                join match m on (t.id = m.home_team_id or t.id = m.away_team_id)
                join match_team home on m.week = home.week and m.home_team_id = home.team_id
                join match_team away on m.week = away.week and m.away_team_id = away.team_id
        group by
            t.name
        order by
            points_against desc
    """).show()

    #
    # Calculate point differentials for each match
    #
    conn.sql("""
        create table point_differential as (
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
        )
    """)

    #
    # Closest game
    #
    conn.sql("""
        select
            pd.*
        from
            point_differential pd
        order by
            pd.differential asc
        limit 1;
    """).show()

    #
    # Biggest blowout
    #
    conn.sql("""
        select
            pd.*
        from
            point_differential pd
        order by
            pd.differential desc
        limit 1;
    """).show()

    #
    # Find lucky winners, unlucky losers. This is defined as the following:
    #
    #   Lucky winner  = a winner whose score is beneath the median for that week. In other words, they would have lost to
    #                   most other teams in the league that week.
    #
    #   Unlucky loser = a loser whose score is _above_ the median for that week. Unlike the above, they would have _won_ against
    #                   most other teams that week.
    #
    conn.sql("""
        with
            weekly_median as (
                select
                    m.week,
                    median(mt.actual_score) as median_score,
                from
                    match m join match_team mt on m.week = mt.week
                where
                    mt.actual_score > 0.0
                group by
                    m.week
            ),
            box_diff_from_median as (
                select
                    mt.week,
                    mt.team_id,
                    mt.actual_score - wm.median_score as median_diff
                from
                    weekly_median wm join match_team mt on mt.week = wm.week
                where
                    mt.actual_score > 0.0
            ),
            box_score as (
                select
                    m.week,

                    home.actual_score as home_actual_score,
                    m.home_team_id as home_team_id,

                    away.actual_score as away_actual_score,
                    m.away_team_id as away_team_id,
                from
                    match m join match_team home on m.week = home.week and home.team_id = m.home_team_id
                            join match_team away on m.week = away.week and away.team_id = m.away_team_id
                where
                    home.actual_score != away.actual_score
            ),
            winner as (
                select
                    week,
                    case
                        when home_actual_score > away_actual_score
                        then home_team_id

                        when home_actual_score < away_actual_score
                        then away_team_id
                    end as team_id
                from
                    box_score
            ),
            loser as (
                select
                    week,
                    case
                        when home_actual_score < away_actual_score
                        then home_team_id

                        when home_actual_score > away_actual_score
                        then away_team_id
                    end as team_id
                from
                    box_score
            ),
            lucky_winner as (
                select
                    'WIN' as status, w.*, t.*, bdm.*
                from
                    winner w join box_diff_from_median bdm on w.team_id = bdm.team_id and w.week = bdm.week
                             join team t on t.id = w.team_id
                where
                    bdm.median_diff < 0
                order by
                    t.name
            ),
            unlucky_loser as (
                select
                    'LOSS' as status, w.*, t.*, bdm.*
                from
                    loser w join box_diff_from_median bdm on w.team_id = bdm.team_id and w.week = bdm.week
                             join team t on t.id = w.team_id
                where
                    bdm.median_diff > 0
                order by
                    t.name
            ),
            result as (
                select * from lucky_winner
                union all
                select * from unlucky_loser
            )
        select
            *
        from
            result w join team t on w.team_id = t.id
        order by
            w.status
    """).show()

    #
    # Best bench performance
    #
    conn.sql("""
        select
            t.name,
            p.name,
            pp.actual_points
        from
            player_performance pp
                join player p on pp.player_id = p.id
                join team t on pp.team_id = t.id
        where
            pp.benched = true
        order by
            pp.actual_points desc
    """).show()

    #
    # Highest scoring bench
    #
    conn.sql("""
        with bench_perf as (
            select
                t.id as team_id,
                sum(pp.actual_points) as points
            from
                team t join player_performance pp on t.id = pp.team_id
            where
                pp.benched = true
            group by
                t.id
        )
        select
            t.*, bp.points
        from
            team t join bench_perf bp on t.id = bp.team_id
        order by
            bp.points
    """).show()

    #
    # Most efficient GM
    #

    #
    # Least efficient GM
    #

    #
    # Number of moves by team
    #
    conn.sql("""
        select
            t.name,
            count(a.team_id) as num_moves
        from
            team t left join activity a on a.team_id = t.id
        group by
            t.name
        order by
            num_moves desc
    """).show()

if __name__ == "__main__":
    main()
