# football-data

A data analysis project which extracts interesting data points from ESPN Fantasy Football leagues.

## Setup
This project requires a `.env` with the following values.
```.env
ESPN_S2=
ESPN_SWID=

LEAGUE_ID=
LEAGUE_YEAR=2025
```
The ESPN-related values can be pulled from your browser's cookies once you login to ESPN Fantasy. The remaining league details can be found in the URL bar. It should be formatted as follows: `https://fantasy.espn.com/football/team?leagueId=<LEAGUE_ID>&...&seasonId=<LEAGUE YEAR>`

## Running
The project is split into two components, `extract` and `analyze`. The `extract` script is responsible for extracting data from ESPN's un-official API, transforming it into a sane format, and dumping it to CSV via DuckDB. You should only need to extract the data once a week after all games have completed. The latter script ingests the CSV files via DuckDB and executes a variety of "fun" queries on it.

```
uv run extract.py   # Extracts data from ESPN Fantasy
uv run analyze.py   # Run data analysis
```
