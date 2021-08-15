"""DAG demonstrating the umbrella use case with dummy operators."""
import logging
import requests
import collections
import pandas as pd
from sqlalchemy import create_engine, types

import airflow.utils.dates
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

dag = DAG(
    dag_id="red_soviz",
    description="Red Sox 40 man STAT data.",
    start_date=airflow.utils.dates.days_ago(5),
    schedule_interval="@weekly",
)

def get_player():
    # get team_id
    logging.info("Fetching team information...")
    url_team = "http://lookup-service-prod.mlb.com/json/named.team_all_season.bam?sport_code='mlb'&all_star_sw='N'&sort_order=name_asc&season='{}'".format(2020)
    response = requests.request("GET", url_team)
    get_team_id = None
    for row in response.json()['team_all_season']['queryResults']['row']:  
        if row['name_display_brief'] == 'Red Sox':
            get_team_id = row['team_id']
    url_40men = "http://lookup-service-prod.mlb.com/json/named.roster_40.bam?team_id='{}'".format(get_team_id)
    response_40men = requests.request('GET', url_40men)
    get_40men = response_40men.json()['roster_40']['queryResults']['row']
    # output file
    logging.info("Retrieved 40 man data!")
    get_40men = pd.DataFrame(get_40men)
    get_40men.to_csv(path_or_buf="/Users/jim/Github/MLB_Data_Engineering/data/men40.csv", index=False)

def get_hitting():
    # players info (position_txt, name_display_first_last, team_name, team_abbrev)
    df = collections.defaultdict(list)
    players = pd.read_csv("/Users/jim/Github/MLB_Data_Engineering/data/men40.csv")
    logging.info("Retrieved 40 man data!")
    for row in range(players.shape[0]):
        id = players.iloc[row, players.columns.get_loc('player_id')]
        # get player data
        try:
            url_hitting = "http://lookup-service-prod.mlb.com/json/named.sport_career_hitting.bam?league_list_id='mlb'&game_type='R'&player_id='{}'".format(id)
            get_data = requests.request('GET', url_hitting).json()['sport_career_hitting']["queryResults"]["row"]
            df['position_txt'].append(players.iloc[row, players.columns.get_loc('position_txt')])
            df['name'].append(players.iloc[row, players.columns.get_loc('name_display_first_last')])
            df['team'].append(players.iloc[row, players.columns.get_loc('team_name')])
            df['team_abbrev'].append(players.iloc[row, players.columns.get_loc('team_abbrev')])
            df['player_id'].append(players.iloc[row, players.columns.get_loc('player_id')])
            for key, val in get_data.items():
                if key != "player_id":
                    df[key].append(val)
        except KeyError:
            continue
    logging.info("Retrieved 40 man STAT Hitting data!")
    get_40men_hitting = pd.DataFrame(df)
    get_40men_hitting.to_csv(path_or_buf="/Users/jim/Github/MLB_Data_Engineering/data/get_40men_hitting.csv", index=False)        


# def get_pitching():
def get_pitching():
    # players info (position_txt, name_display_first_last, team_name, team_abbrev)
    df = collections.defaultdict(list)
    players = pd.read_csv("/Users/jim/Github/MLB_Data_Engineering/data/men40.csv")

    # get player data
    logging.info("Retrieved 40 man data!")
    for row in range(players.shape[0]):
        id = players.iloc[row, players.columns.get_loc('player_id')]
        try:
            url_pitching = "http://lookup-service-prod.mlb.com/json/named.sport_career_pitching.bam?league_list_id='mlb'&game_type='R'&player_id='{}'".format(id)
            get_data = requests.request('GET', url_pitching).json()['sport_career_pitching']["queryResults"]["row"]
            df['position_txt'].append(players.iloc[row, players.columns.get_loc('position_txt')])
            df['name'].append(players.iloc[row, players.columns.get_loc('name_display_first_last')])
            df['team'].append(players.iloc[row, players.columns.get_loc('team_name')])
            df['team_abbrev'].append(players.iloc[row, players.columns.get_loc('team_abbrev')])
            df['player_id'].append(players.iloc[row, players.columns.get_loc('player_id')])
            print(get_data)
            for key, val in get_data.items():
                if key != "player_id":
                    df[key].append(val)
        except KeyError:
            continue
    logging.info("Retrieved 40 man STAT Pitching data!")
    get_40men_pitching = pd.DataFrame(df)
    get_40men_pitching.to_csv(path_or_buf="/Users/jim/Github/MLB_Data_Engineering/data/get_40men_pitching.csv", index=False)        

def load_mysql(input_csv):
    df = pd.read_csv(input_csv)
    logging.info("Retrieved data!")


get_40men = PythonOperator(
    task_id="get_40men",
    python_callable=get_player,
    dag=dag
)
get_40men_hitting = PythonOperator(
    task_id="get_40men_hitting",
    python_callable=get_hitting,
    dag=dag
)
get_40men_pitching = PythonOperator(
    task_id="get_40men_pitching",
    python_callable=get_pitching,
    dag=dag
)
load_mysql_hitting = DummyOperator(
    task_id='load_hitting',
    dag=dag
)
load_mysql_pitching = DummyOperator(
    task_id='load_pitching',
    dag=dag
)
# load_mysql_hitting = PythonOperator(
#     task_id='load_hitting',
#     python_callable=load_mysql,
#     op_kwargs={"input_csv": "/Users/jim/Github/MLB_Data_Engineering/data/get_40men_hitting.csv"},
#     dag=dag
# )
# load_mysql_hitting = PythonOperator(
#     task_id='load_hitting',
#     python_callable=load_mysql,
#     op_kwargs={"input_csv": "/Users/jim/Github/MLB_Data_Engineering/data/get_40men_pitching.csv"},
#     dag=dag
# )

# Set dependencies between all tasks
get_40men >> [get_40men_hitting, get_40men_pitching]
get_40men_hitting >> load_mysql_hitting
get_40men_pitching >> load_mysql_pitching