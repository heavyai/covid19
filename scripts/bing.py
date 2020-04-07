#!/home/venkat/miniconda3/bin/python

## ^^^ change to your system python3 path, ideally 3.7+
##Deps needed - pymapd, prefect, parsel (conda isntalling these will install other needed deps)

import pandas as pd
import prefect
from prefect import task, Flow
from os import path
from pymapd import connect
from typing import Dict,List
from datetime import timedelta
from prefect.schedules import IntervalSchedule
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
from prefect.engine.result_handlers import LocalResultHandler
from parsel import Selector
import requests
from io import StringIO
from datetime import datetime
import sys
import os
import json
import jmespath

DATA_BING_URL = 'https://bing.com/covid/data'
TIME_INTERVAL = 10

@task
def extract_bing() -> Dict[str, pd.DataFrame]:
    logger = prefect.context.get("logger")

    dfs={}
    try:

        with requests.get(DATA_BING_URL) as response:
            if(response.status_code == requests.codes.ok and 'application/json' in response.headers["Content-Type"]):
                raw_json = json.loads(response.text)
                df = pd.json_normalize(json.loads(response.text)["areas"], meta=["id", "displayName"], meta_prefix="world", record_path=["areas"])
                full_df = df.explode('areas')
                temp_df = full_df.loc[full_df['areas'] != '']
                rest_df = full_df.loc[full_df['areas'] == '']
                temp_df.to_csv('bing2.csv', index=False)

        return { 'bing_data': df }

    except:
        logger.error(f'Failed to download bing data')
        raise

@task(cache_for=timedelta(minutes=1), cache_key='prev_result', cache_validator=prefect.engine.cache_validators.all_inputs)
def transform_worldometer_country_stats( today_data: pd.DataFrame, yesterday_data: pd.DataFrame ) -> Dict[str, pd.DataFrame]:
    pass

@task
def load_omnisci(tables_to_load: [Dict[str, pd.DataFrame]]) -> None:
    """Load the data into OmniSci, truncating existing data"""
    logger = prefect.context.get("logger")

    for t in tables_to_load:
        logger.info(f"{t['table_name']} -> {t['table_data'].info()}")

    try:

        logger.info(f"Connecting to OmniSciDB")

        #Connect to the OmniSci instance
        #TODO hide these criteria in a params file
        with connect(user="venkat", password="HyperInteractive", host="bewdy.mapd.com", dbname="mapd", port="6274", protocol="binary") as con:
            for tbl in tables_to_load:
                logger.info(f"Proceeding to load {len(tbl['table_data'].index)} rows into {tbl['table_name']} table")
                con.load_table(tbl['table_name'], tbl['table_data'], create='infer', preserve_index=False)
                logger.info('Table loaded successfully, proceeding to cleanup files')
    
    except Exception as ex:
        template = "An exception of type {0} occurred. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.error_msg)
        logger.error(f'Failed to load data into OmniSciDB: {message}')
    finally:
        con.close()

#run every 10 mins
min_schedule = IntervalSchedule(
    start_date=datetime.utcnow() + timedelta(seconds=1),
    interval=timedelta(minutes=1),
)

prev_run_output = None

with Flow('COVID 19 flow', schedule=min_schedule) as flow:
    #extract tasks
    bing_data_raw = extract_bing()
    #data tables

    #passing an array of dicts so that we can add more transform functions later, and collect them in a list for the loader


with prefect.context():
    state=flow.run()