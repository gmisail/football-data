import os
import duckdb
from dotenv import load_dotenv

from espn_api.football import League
from espn_api.football.activity import Activity


def load_activity(league: League, page: int = 0) -> list[Activity]:
    currentOffset = page * 100
    activity = league.recent_activity(size=100, offset=currentOffset)

    if len(activity) == 0:
        return []

    activity.extend(load_activity(league, page + 1))

    return activity

def ingest_player_performance(conn, week, team, lineup):
    for player in lineup:
        conn.execute(
            """
                insert into player
                values (?, ?)
                on conflict (id) do nothing;
            """,
            [player.playerId, player.name]
        )

        conn.execute(
            """
                insert into player_performance
                values (?, ?, ?, ?, ?, ?, ?);
            """,
            [
                week,
                player.playerId,
                team.team_id,
                player.position,
                player.lineupSlot == "BE",
                player.points,
                player.projected_points
            ]
        )

def main():
    load_dotenv()

    espn_s2 = os.environ.get("ESPN_S2")
    swid = os.environ.get("ESPN_SWID")

    league_id = os.environ.get("LEAGUE_ID") if "LEAGUE_ID" in os.environ else ""
    league_year = os.environ.get("LEAGUE_YEAR") if "LEAGUE_YEAR" in os.environ else ""

    league = League(league_id=int(league_id or ""), year=int(league_year or ""), swid=swid, espn_s2=espn_s2)

    conn = duckdb.connect()

    conn.sql("""
        create table team (
            id text primary key,
            name text not null
        );

        create table activity (
            team_id text not null,
            action text not null,
            player_id text not null
        );

        create table match (
            week integer not null,
            home_team_id text not null,
            away_team_id text not null
        );

        create table match_team (
            week integer not null,
            team_id text not null,
            actual_score float not null,
            projected_score float not null,

            primary key (week, team_id)
        );

        create table player (
            id text not null primary key,
            name text not null
        );

        create table player_performance (
            week             integer not null,
            player_id        text not null,

            team_id          text not null,
            position         text not null,
            benched          boolean not null,
            actual_points    float not null,
            projected_points float not null,

            primary key (week, player_id)
        );
    """)

    print("loading teams...")
    for team in league.teams:
        conn.execute(
            """
                insert into team values (
                    ?, ?
                );
            """,
            [team.team_id, team.team_name]
        )

    print("loading activity...")
    activity = load_activity(league)
    for page in activity:
        for action in page.actions:
            conn.execute(
                """
                    insert into activity values (
                        ?, ?, ?
                    );
                """,
                [action[0].team_id, action[1], action[2].playerId]
            )

    print("loading matches...")
    for week in range(1, 3):
        for box_score in league.box_scores(week):
            conn.execute(
                """
                    insert into match values (
                        ?, ?, ?
                    );
                """,
                [week, box_score.home_team.team_id, box_score.away_team.team_id]
            )

            conn.execute(
                """
                    insert into match_team
                    values (?, ?, ?, ?),
                           (?, ?, ?, ?);
                """,
                [
                    week, box_score.home_team.team_id, box_score.home_score, box_score.home_projected,
                    week, box_score.away_team.team_id, box_score.away_score, box_score.away_projected,
                ]
            )

            ingest_player_performance(conn, week, box_score.home_team, box_score.home_lineup)
            ingest_player_performance(conn, week, box_score.away_team, box_score.away_lineup)

    conn.execute("""
        EXPORT DATABASE 'data' (FORMAT csv, DELIMITER '|');
    """)

if __name__ == "__main__":
    main()
