##Deps needed - pymapd, prefect, parsel (conda isntalling these will install other needed deps)

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

SOURCE='worldometer'

prefect.config.flows.checkpointing = True

@task
def extract_wm_prev_run(result_db_path: str):
    logger = prefect.context.get("logger")
    logger.info(f"Looking for previous run results at-:{result_db_path}")

    if (os.path.exists(result_db_path)):
        logger.info(f"Found previous run results at-:{result_db_path}")
        with sqlite3.connect(result_db_path) as conn:
            try:
                df = pd.read_sql(f'select * from worldometer',conn)
                if (df is not None):
                    return df
            except:
                return None

    return None

@task
def extract_worldometer() -> Dict[str, pd.DataFrame]:
    logger = prefect.context.get("logger")
    tbl_suffixes = ['today', 'yesterday']
    dfs = {}

    url = prefect.config.sources[SOURCE]['url']
    try:
        f = requests.get(url)
        #use parsel to create the dataframe for the transform function
        page_raw = Selector(text=f.text)
        #Extract tables into dataframes, and produce a dict
        #There are 2 tables in the main page - yesterday's and current data
        #We'll always diff the 'yesterday table' against todays to get a current count of new cases, while retaining the cumulative totals
        dfs['tables'] = {}
        for suffix in tbl_suffixes:
            tbl_hdr_xpath=f'//table[@id="main_table_countries_{suffix}"]/thead/tr/th'
            tbl_hdr_raw_html=page_raw.xpath(tbl_hdr_xpath).getall()
            tbl_id_xpath=f'//table[@id="main_table_countries_{suffix}"]/tbody/tr'
            tbl_raw_html_rows=page_raw.xpath(tbl_id_xpath).getall()
            df = process_worldometer_table(tbl_hdr_raw_html, tbl_raw_html_rows)
            dfs['tables'][suffix] = df    
        
        #TODO Extract news articles here with parsel

        return dfs

    except:
        logger.error(f'Failed to download time series files - exception occurred: {sys.exc_info()[0]}')

def process_worldometer_table( worldometer_table_hdr_html: List[str], worldometer_table_raw_html: List[str]) -> pd.DataFrame:
    """Process the worldometer raw data into dataframes"""

    logger = prefect.context.get("logger")
    
    hdr_cols=[]
    for hdrow in worldometer_table_hdr_html:
        sel = Selector(text=hdrow)
        colname = '_'.join(sel.xpath('//th/text()').getall())\
                                                    .replace('/','')\
                                                    .replace(',','')\
                                                    .replace(' ','_')\
                                                    .replace('\xa0','_')\
                                                    .replace('\n','').lower()
        hdr_cols.append(colname)
    
    counts_dataset = [','.join(hdr_cols)]
    try:
        for row in worldometer_table_raw_html[1:]:

            sel=Selector(text=row)
            if sel.xpath('//td/a') != []:
                nm=sel.xpath('//td/a/text()').get()
            elif sel.xpath('//td/span') != []:
                nm=sel.xpath('//td/span/text()').get()
            else:
                nm=sel.xpath('//td/text()').get()

            #Basic data cleaning steps because of garbage in the country field
            nm = 'United States' if nm == "USA" else nm
            nm = 'United Kingdom' if nm == "UK" else nm
            nm = 'South Korea' if nm == "S. Korea" else nm
            nm = 'Russian Federation' if nm == "Russia" else nm
            nm = 'Czech Republic' if nm == "Czechia" else nm
            nm = 'Brunei Darussalam' if nm == "Brunei" else nm
            nm = 'Macedonia' if nm == "North Macedonia" else nm
            nm = "Côte d'Ivoire" if nm == "Ivory Coast" else nm
            nm = "Democratic Republic of the Congo" if nm == "DRC" else nm
            nm = "Republic of the Congo" if nm == "Congo" else nm
            nm = "Saint-Martin" if nm == "Saint Martin" else nm
            nm = "Central African Republic" if nm == "CAR" else nm
            nm = "Vatican" if nm == "Vatican City" else nm
            nm = "Saint Vincent and the Grenadines" if nm == "St. Vincent Grenadines" else nm
            nm = "Republic of Cabo Verde" if nm == "Cabo Verde" else nm
            nm = "United States Virgin Islands" if nm == "U.S. Virgin Islands" else nm
            nm = "Saint-Barthélemy" if nm == "St. Barth" else nm
            nm = "eSwatini" if nm == "Eswatini" else nm

            rest = ['""' if td.xpath('text()') == [] else f"\"{td.xpath('text()').get().strip().replace(',','').replace('+','')}\"" for td in sel.xpath('//td')]

            rest[0] = f'"{nm}"'
            counts_row = ','.join(rest)

            #write each row to a list
            counts_dataset.append(counts_row)

        counts_dataset_str = StringIO('\n'.join(counts_dataset[:-1]))
        raw_df = pd.read_csv(counts_dataset_str).fillna(0)

        #reorder columns

        raw_df.rename(columns={  'country_other':'country_region',\
                                'total_cases':'confirmed',\
                                'new_cases':'confirmed_diff',\
                                'total_deaths':'deaths',\
                                'new_deaths':'deaths_diff',\
                                'total_recovered':'recovered',\
                                'active_cases':'active',\
                                'serious_critical':'critical',\
                                'total_tests':'tests',\
                                'tot_cases_1m_pop':'cases_1m_pop',\
                                'tests__':'tests_1m_pop'}, inplace=True)

        raw_df = raw_df[['country_region',\
                        'confirmed',\
                        'deaths',\
                        'recovered',\
                        'active',\
                        'critical',\
                        'tests',\
                        'cases_1m_pop',\
                        'deaths_1m_pop',\
                        'tests_1m_pop',\
                        'confirmed_diff',\
                        'deaths_diff']]
        return raw_df

    except:
        logger.error(f'Failed to process worldometer html - exception occurred: {sys.exc_info()[0]}')

@task(result_handler=SQLiteResultHandler(result_tbl_name='worldometer'))
def transform_worldometer_country_stats( today_data: pd.DataFrame, yesterday_data: pd.DataFrame, prev_run_df: pd.DataFrame ) -> Dict[str, pd.DataFrame]:
    """Transform the worldometer data outputs into a final dataframe. We have 2 tables (for yesterday and today, compute the diffs"""

    logger = prefect.context.get("logger")

    if prev_run_df is None:
        logger.info('No previous result found, doing first load')
        prev_run_data = yesterday_data
    else:
        logger.info('Found data from previous run')
        prev_run_data = prev_run_df
    
    #First, get the 2 tables to diff by their key (country)
    
    today_data.set_index('country_region', inplace=True)
    prev_run_data.set_index('country_region', inplace=True)

    #Column-wise subtraction on the diffs columns, using the country_region as key
    today_data['confirmed_diff'] = today_data['confirmed'].sub(prev_run_data['confirmed'], fill_value=0, level='country_region')
    today_data['deaths_diff'] = today_data['deaths'].sub(prev_run_data['deaths'],  fill_value=0, level='country_region')
    today_data['recovered_diff'] = today_data['recovered'].sub(prev_run_data['recovered'], fill_value=0, level='country_region')
    today_data['active_diff'] = today_data['active'].sub(prev_run_data['active'], fill_value=0, level='country_region')
    today_data['critical_diff'] = today_data['critical'].sub(prev_run_data['critical'], fill_value=0, level='country_region')
    today_data['tests_diff'] = today_data['tests'].sub(prev_run_data['tests'], fill_value=0, level='country_region')

    today_data.reset_index(inplace=True)
    wm_df = today_data

    ts = pd.to_datetime(pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    wm_df['ts'] = ts

    wm_df_out = wm_df.astype({ 'confirmed':'int32',\
                                'confirmed_diff':'int32',\
                                'deaths_diff':'int32',\
                                'deaths':'int32',\
                                'recovered':'int32',\
                                'active':'int32',\
                                'critical':'int32',\
                                'recovered_diff':'int32',\
                                'active_diff':'int32',\
                                'critical_diff':'int32',\
                                'cases_1m_pop':'int32',\
                                'tests_diff':'int32'})

    row_count = len(wm_df_out.index)
    #must refactor out below - common code for all loaders
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

        wm_df_out.to_csv(output_filepath, index=False)

        df_dict={\
                'file_names':[current_output_filename],\
                'data':{'table_name':table_name, 'table_data': wm_df_out}\
                }

        return df_dict 

    return None

with Flow('COVID 19 Worldometer Data Pipeline') as flow:
    #extract params from config file
    logger = prefect.utilities.logging.get_logger()

    results_dir = prefect.config.results['result_dir']
    result_db = prefect.config.results['result_db']
    s3_bucket = prefect.context.secrets['s3_bucket_name']

    #extract
    raw_data = extract_worldometer()
    result_db_path = f'{results_dir}/{result_db}'
    prev_run_data = extract_wm_prev_run(result_db_path=result_db_path)
    today_data, yesterday_data = raw_data['tables']['today'], raw_data['tables']['yesterday']

    #transform
    transformed_data = transform_worldometer_country_stats(today_data, yesterday_data, prev_run_data)
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

        load_task.map(data = [transformed_data['data']], drop_existing = unmapped(True))

with prefect.context() as c:
    if(prefect.config['runmode'] == 'schedule'):
        schedule = CronSchedule(cron = prefect.config.sources[SOURCE]['cron_string'], start_date=datetime.now())    
        flow.schedule = schedule

    state=flow.run()