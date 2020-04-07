import pandas as pd
import prefect
from prefect import task, Flow, unmapped
from prefect.schedules import Schedule, CronSchedule
from prefect.schedules.clocks import CronClock
import requests
from io import StringIO
from datetime import datetime, timedelta
import os
from os import path
from typing import Dict
from prefect.tasks.aws.s3 import S3Upload
import omniscitasks

#Must match the name in config.toml
SOURCE='covid19trackingproject'

@task
def extract_covid19_tracking_data() -> Dict[str, pd.DataFrame]:
    """Download the states data from the covid19 tracking project"""

    logger = prefect.context.get("logger")

    config = prefect.config.sources[SOURCE]
    url = config['url']

    dfs={}

    with requests.get(url) as response:
        if(response.status_code == requests.codes.ok and 'application/json' in response.headers["Content-Type"]):
            df = pd.read_json(response.text).fillna(0)

    return { 'c19tp_data': df }

@task 
def transform_covid19_tracking_data(daily_c19tp_reports: Dict) -> [Dict]:
    output_dfs = []

    logger = prefect.context.get("logger")
    
    state_counts_df = daily_c19tp_reports['c19tp_data']

    state_counts_df['dt'] = state_counts_df['date'].astype('str').apply(lambda dt_str: datetime.strptime(dt_str, '%Y%m%d'))
    state_counts_df.drop(['date', 'dateChecked'], axis=1, inplace=True)
    state_counts_df.sort_values(by='dt', inplace=True)

    prev_day_st = state_counts_df.groupby('state').shift(1)
    pos_diff_df = state_counts_df['positive'].sub(prev_day_st['positive'], fill_value=0).rename('confirmed_diff')
    neg_diff_df = state_counts_df['negative'].sub(prev_day_st['negative'], fill_value=0).rename('negative_diff')
    pend_diff_df = state_counts_df['pending'].sub(prev_day_st['pending'], fill_value=0).rename('pending_diff')
    death_diff_df = state_counts_df['death'].sub(prev_day_st['death'], fill_value=0).rename('deaths_diff')
    hosp_diff = state_counts_df['hospitalized'].sub(prev_day_st['hospitalized'], fill_value=0).rename('hosp_diff')
    tot_diff_df = state_counts_df['total'].sub(prev_day_st['total'], fill_value=0).rename('total_diff')
    full_diff_df = pd.concat([state_counts_df, pos_diff_df, neg_diff_df, pend_diff_df, death_diff_df, hosp_diff, tot_diff_df], axis=1, sort=False)

    full_diff_df.rename(columns={"state":"province_state",\
                                            "positive":"confirmed",\
                                            "death":"deaths"}, inplace=True)    
    
    full_diff_df = full_diff_df.astype({  'confirmed':'int32',\
                                'confirmed_diff':'int32',\
                                'deaths':'int32',\
                                'deaths_diff':'int32',\
                                'negative':'int32',\
                                'negative_diff':'int32',\
                                'pending':'int32',\
                                'pending_diff':'int32',\
                                'hospitalized':'int32',\
                                'hosp_diff':'int32',\
                                'total':'int32',\
                                'total_diff':'int32'})

    row_count = len(full_diff_df.index)
    if (row_count > 0):
        file_prefix = prefect.config.sources[SOURCE]['file_prefix']
        table_name = prefect.config.sources[SOURCE]['result_table']
        flowrun_suffix = datetime.now().strftime('%Y%m%d-%H%M%S')

        output_dir = prefect.config['output-directory']
        if(not os.path.exists(output_dir)):
            os.makedirs(output_dir)
        
        current_output_filename = f"{file_prefix}-{flowrun_suffix}.csv"
        output_filepath = os.path.join(output_dir, current_output_filename)
        logger.info(f'Writing output file, {row_count} rows to:{output_filepath}')
        full_diff_df.to_csv(output_filepath, index=False)

        df_dict={\
                'file_names':[current_output_filename],\
                'data':{'table_name':table_name, 'table_data': full_diff_df}\
                }
        return df_dict 
        
    return None

with Flow('COVID 19 Tracking Project Data Pipeline') as flow:
    logger = prefect.utilities.logging.get_logger()

    config = prefect.config.sources[SOURCE]
    destinations = prefect.config['destinations']['omnisci']
    s3_bucket = prefect.context.secrets['s3_bucket_name']

    #extract tasks
    raw_data = extract_covid19_tracking_data()

    #transform tasks
    transformed_data = transform_covid19_tracking_data(raw_data)

    # load tasks
    #upload files to S3
    s3_upload = S3Upload(bucket=s3_bucket)
    s3_upload.map(data=transformed_data['file_names'])
    # Fetch omniscidb destinations using a task - unify those in the config file, and any e.g. that are fetched dynamically

    #load the results to the final destinations
    for dest in destinations:

          load_task = omniscitasks.OmniSciLoadTableTask(dbname = dest['dbname'],\
                              user = dest['user'],\
                              password = dest['password'],\
                              host = dest['host'],\
                              port = 6274 if dest['port'] is None else dest['port'])

          #We need to wrap even a singleton output in a list, which will allow us to upload multiple tables to each dest
          load_task.map(data = [transformed_data['data']], drop_existing = unmapped(True))
    
with prefect.context() as c:
    if(prefect.config['runmode'] == 'schedule'):
        schedule = CronSchedule(cron = prefect.config.sources[SOURCE]['cron_string'], start_date=datetime.now())    
        flow.schedule = schedule

    state=flow.run()