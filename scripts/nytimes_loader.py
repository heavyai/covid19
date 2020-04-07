import pandas as pd
import prefect
from prefect import task, Flow, unmapped
from prefect.schedules import CronSchedule
import requests
from io import StringIO
from datetime import datetime, timedelta
import os
from os import path
from typing import Dict, List, Any
from prefect.tasks.aws.s3 import S3Upload
import omniscitasks
from parsel import Selector
import sys
import sqlite3
from resulthandlers import SQLiteResultHandler

SOURCE='nytimes'

@task
def extract_nytimes_data() -> Dict[str, pd.DataFrame]:
    """
    Download the states data from  NY Times github
    """

    locations=['states','counties']
    url = prefect.config.sources[SOURCE]['url']

    logger = prefect.context.get("logger")
    dfs={}

    for loc in locations:
        fn = f'us-{loc}.csv'
        file_url = f'{url}{fn}'
        logger.info(f'Downloading {fn} for {loc} from {file_url}')
        with requests.get(file_url) as response:
            if(response.status_code == requests.codes.ok and 'text/plain' in response.headers["Content-Type"]):
                df = pd.read_csv(StringIO(response.text), dtype={'fips':'object'})
                dfs[loc] = df
    
    return dfs

@task 
def transform_nytimes_data(daily_nytimes_data: Dict[str, pd.DataFrame]) -> [Dict]:
    output_dfs = []
    output_filenames=[]

    logger = prefect.context.get("logger")

    for key in daily_nytimes_data:
        logger.info(f"Processing NYTimes data for {key}")
        raw_counts_df = daily_nytimes_data[key]
        groupby_key = ['county','state'] if 'county' in raw_counts_df.columns else ['state']

        #Fix missing and broken fips codes
        if ('county' in raw_counts_df.columns):
            raw_counts_df.loc[raw_counts_df['county'].str.contains('New York City'),'fips'] = '36061' 
            indexNames = raw_counts_df[raw_counts_df['county'].str.contains('Dukes and Nantucket|Kansas City')].index
            raw_counts_df.drop(indexNames , inplace=True)

        raw_counts_df['dt'] = raw_counts_df['date'].astype('str').apply(lambda dt_str: datetime.strptime(dt_str, '%Y-%m-%d'))
        raw_counts_df.drop(['date'], axis=1, inplace=True)
        raw_counts_df.sort_values(by='dt', inplace=True)

        raw_counts_df.drop
        prev_day_st = raw_counts_df.groupby(groupby_key).shift(1)
        pos_diff_df = raw_counts_df['cases'].sub(prev_day_st['cases'], fill_value=0).rename('confirmed_diff')
        deaths_diff_df = raw_counts_df['deaths'].sub(prev_day_st['deaths'], fill_value=0).rename('deaths_diff')
        full_diff_df = pd.concat([raw_counts_df, pos_diff_df, deaths_diff_df], axis=1, sort=False)


        full_diff_df.rename(columns={"state":"province_state",\
                                                "cases":"confirmed"}, inplace=True)    
        
        full_diff_df = full_diff_df.astype({  'confirmed':'int32',\
                                    'confirmed_diff':'int32',\
                                    'deaths':'int32',\
                                    'deaths_diff':'int32'})

        row_count = len(full_diff_df.index)

        if (row_count > 0):
            file_prefix = f"{prefect.config.sources[SOURCE]['file_prefix']}-{key}"
            table_name = f"{prefect.config.sources[SOURCE]['result_table']}_{key}"
            flowrun_suffix = datetime.now().strftime('%Y%m%d-%H%M%S')
            output_dir = prefect.config['output-directory']
            if(not os.path.exists(output_dir)):
                os.makedirs(output_dir)
            
            current_output_filename = f"{file_prefix}-{flowrun_suffix}.csv"
            output_filepath = os.path.join(output_dir, current_output_filename)
            logger.info(f'Writing output file, {row_count} rows to:{output_filepath}')

            full_diff_df.to_csv(output_filepath, index=False)
            output_filenames.append(output_filepath)
            output_dfs.append({'table_name':f'nytimes_{key}', 'table_data': full_diff_df })
        
    df_dict={\
            'file_names':output_filenames,\
            'data':output_dfs
            }
    return df_dict

with Flow('COVID 19 Worldometer Data Pipeline') as flow:
    #extract params from config file
    logger = prefect.utilities.logging.get_logger()

    s3_bucket = prefect.context.secrets['s3_bucket_name']

    #extract
    raw_data = extract_nytimes_data()

    #transform
    transformed_data = transform_nytimes_data( raw_data )
    destinations = prefect.config['destinations']['omnisci']

    #load - both to s3 and omnisci db destinations specified in config
    s3_upload = S3Upload(bucket=s3_bucket)
    s3_upload.map(data=transformed_data['file_names'])

    for dest in destinations:
        load_task = omniscitasks.OmniSciLoadTableTask(dbname = dest['dbname'],\
                            user = dest['user'],\
                            password = dest['password'],\
                            host = dest['host'],\
                            port = 6274 if dest['port'] is None else dest['port'])

        load_task.map(data = transformed_data['data'], drop_existing = unmapped(True))

with prefect.context() as c:
    if(prefect.config['runmode'] == 'schedule'):
        schedule = CronSchedule(cron = prefect.config.sources[SOURCE]['cron_string'], start_date=datetime.now())    
        flow.schedule = schedule

    state=flow.run()