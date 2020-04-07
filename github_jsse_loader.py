#!/home/ubuntu/miniconda3/bin/python

import pandas as pd
import wget
import prefect
from prefect import task, Flow
from os import path
from pymapd import connect
from typing import Dict
from datetime import timedelta
from prefect.schedules import IntervalSchedule
from prefect.schedules import Schedule
from prefect.schedules.clocks import CronClock
import requests
from io import StringIO
from datetime import datetime
import sys
import os
import boto3
from botocore.exceptions import ClientError

import csv
DATASOURCE_JHU_GITHUB_FILENAME='time_series_covid19'
DATASOURCE_JHU_GITHUB_URL = 'https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/'
DATASOURCE_COVID19TRACKING_URL = 'https://covidtracking.com/api/states/daily'
S3_BUCKET_NAME='mapd-data-uswest2'
S3_OBJECT_NAME='covid19/latest/covid_daily_global-data.csv'
AUTOSCALING_GROUPS = [{'asg_name': 'demo-covid19','asg_region': 'us-west-2'},{'asg_name': 'demo-covid19','asg_region': 'eu-central-1'}]

@task
def extract_covid19_tracking_data() -> Dict[str, pd.DataFrame]:
    """Download the states data from the covid19 tracking project"""

    logger = prefect.context.get("logger")
    dfs={}

    with requests.get(DATASOURCE_COVID19TRACKING_URL) as response:
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

    output_file_name=f'covid_19_tracking_project-{datetime.now().strftime("%Y%m%d-%H%M%S")}.csv'  
    full_diff_df.to_csv(output_file_name, index=False)

    df_dict={\
            'file_names':[output_file_name],\
            'data':{'table_name':'daily_covid_us_states', 'table_data': full_diff_df}\
            }
    return df_dict 
                                              
@task
def extract_gh_global_covid_ts_data() -> Dict[str, pd.DataFrame]:
    """Download the time series csv files into dataframes"""

    logger = prefect.context.get("logger")

    dfs={}

    locations = ['global', 'US']
    file_types = ['testing','confirmed','deaths']
    
    for loc in locations:
        loc_dict = {}
        for file_type in file_types:
            file_handle=f'{DATASOURCE_JHU_GITHUB_FILENAME}_{file_type}_{loc}.csv'
            file_url=f'{DATASOURCE_JHU_GITHUB_URL}{file_handle}'

            #Download each file and then read into a pandas dataframe for post-processing.
            #As of 3/23 we have to handle this separately for global and us locations

            with requests.get(file_url) as response:
                if(response.status_code == requests.codes.ok and 'text/plain' in response.headers['Content-Type']):
                    logger.info(f'Downloaded: {file_handle}')
                    loc_dict[file_type] = pd.read_csv(StringIO(response.text))
            
        if loc_dict!= {}:
            dfs[loc] = loc_dict

    return dfs

@task
def transform_daily_covid_data(daily_report_dfs: Dict) -> [Dict]:
    """Consolidate the time series files into one for global and US, for each location"""

    logger = prefect.context.get("logger")

    output_dfs = []
    output_filenames = []
    for loc in daily_report_dfs:
        logger.info(f'Processing time series data for:{loc}')
        confirmed_df_ts = daily_report_dfs[loc].get('confirmed')
        deaths_df_ts = daily_report_dfs[loc].get('deaths')
        testing_df_ts = daily_report_dfs[loc].get('testing', None)
        filename, consolidated_df = process_daily_covid_data( confirmed_df_ts, deaths_df_ts, testing_df_ts)

        #get a single consolidated dataframe for each location (global, US)
        if (consolidated_df is not None):
            table_key = f'covid_daily_{loc.lower()}'
            output_dfs.append({ 'table_name' : table_key, 'table_data': consolidated_df })
            output_filenames.append(filename)

    return {'file_names':output_filenames, 'data':output_dfs}

def process_daily_covid_data( confirmed_df_ts: pd.DataFrame, deaths_df_ts: pd.DataFrame, testing_df_ts: pd.DataFrame)-> pd.DataFrame:

    logger = prefect.context.get("logger")

    """Process the 3 time series dataframes into a single one with diffs for each day, and filter out empty rows"""

    #date columns start at column 5, pivot into long form
    dates = confirmed_df_ts.columns[4:]

    conf_df_long = confirmed_df_ts.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], value_vars=dates, var_name='Date', value_name='Confirmed')
    deaths_df_long = deaths_df_ts.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], value_vars=dates, var_name='Date', value_name='Deaths')
    #TODO uncomment below when we start seeing the testing time series data
    #testing_df_long = testing_df_long.melt(id_vars=['Province/State', 'Country/Region', 'Lat', 'Long'], value_vars=dates, var_name='Date', value_name='Testing')

    #munge these melted dfs together

    full_table = pd.concat([conf_df_long, deaths_df_long['Deaths']], axis=1, sort=False)

    #TODO uncomment below, and remove above line when we start seeing the testing time series data
    #full_table = pd.concat([conf_df_long, deaths_df_long['Deaths'], testing_df_long['Testing']], axis=1, sort=False)

    #subtle bug - df.shift doesnt work if you groupby on nulls in the province field, so have to use the lat/long and country fields to group by
    prev_day = full_table.groupby(['Country/Region', 'Lat', 'Long']).shift(1)
    confirmed_diff = full_table['Confirmed'].sub(prev_day['Confirmed'],fill_value=0).rename('confirmed_diff')
    deaths_diff = full_table['Deaths'].sub(prev_day['Deaths'],fill_value=0).rename('deaths_diff')

    #TODO uncomment this when we start seeing the testing time series data
    #testing_diff = full_table['Testing'].sub(prev_day['Testing'],fill_value=0).rename('testing_diff')

    full_table_diff = pd.concat([full_table, confirmed_diff, deaths_diff], axis=1, sort=False)
    
    #TODO uncomment below, and remove above line when we start seeing the testing time series data
    #full_table_diff = pd.concat([full_table, confirmed_diff, deaths_diff, testing_diff], axis=1, sort=False)

    full_table_diff.fillna(0)
    full_table_diff.rename(columns={"Province/State": "province_state",\
                                    "Country/Region": "country_region",\
                                    "Lat": "lat",\
                                    "Long":"lon",\
                                    "Date":"dt",\
                                    "Confirmed":"confirmed",\
                                    "Deaths":"deaths"}, inplace=True)

    #select only rows with non-zero counts
    filt_df = full_table_diff[full_table_diff.iloc[:,5:].sum(axis=1) != 0]

    #Basic data cleaning steps because of garbage in the country field
    filt_df.loc[filt_df['country_region'] == 'US','country_region'] = 'United States'
    filt_df.loc[filt_df['country_region'] == 'Korea, South','country_region'] = 'Korea'
    filt_df.loc[filt_df['country_region'] == 'Taiwan*','country_region'] = 'Taiwan'
    filt_df.loc[filt_df['country_region'] == 'Russia','country_region'] = 'Russian Federation'
    filt_df.loc[filt_df['country_region'] == 'Czechia','country_region'] = 'Czech Republic'
    filt_df.loc[filt_df['country_region'] == 'Brunei','country_region'] = 'Brunei Darussalam'
    filt_df.loc[filt_df['country_region'] == 'North Macedonia','country_region'] = 'Macedonia'
    filt_df.loc[filt_df['country_region'] == 'Congo (Kinshasa)','country_region'] = 'Democratic Republic of the Congo'
    filt_df.loc[filt_df['country_region'] == 'Congo (Brazzaville)','country_region'] = 'Republic of the Congo'
    filt_df.loc[filt_df['country_region'] == "Cote d'Ivoire",'country_region'] = "Côte d'Ivoire"
    filt_df.loc[filt_df['country_region'] == 'Eswatini','country_region'] = 'eSwatini'
    filt_df.loc[filt_df['country_region'] == 'Bahamas, The','country_region'] = 'Bahamas'
    filt_df.loc[filt_df['country_region'] == 'Gambia, The','country_region'] = 'The Gambia'
    filt_df.loc[filt_df['country_region'] == 'Cabo Verde','country_region'] = 'Republic of Cabo Verde'
    filt_df.loc[filt_df['country_region'] == 'U.S. Virgin Islands','country_region'] = 'United States Virgin Islands'
    filt_df.loc[filt_df['country_region'] == 'Holy See','country_region'] = 'Vatican'
    filt_df.loc[filt_df['country_region'] == 'Martinique','country_region'] = 'France'

    #TODO - fix the copy warning here
    dateify_func = lambda dt_str: datetime.strptime(dt_str, '%m/%d/%y')
    filt_df['dt'] = filt_df['dt'].apply(dateify_func)

    #write this out
    logger.info(f"Transformed report files. Record count:{len(filt_df.index)}. Writing to clean file")

    filt_df = filt_df.astype({  'lat':'float32',\
                                'lon':'float32',\
                                'confirmed':'int32',\
                                'deaths':'int32',\
                                'deaths_diff':'int32',\
                                'confirmed_diff':'int32'})

    #TODO parametrize this
    clean_file_name=f'covid_19_clean_complete-{datetime.now().strftime("%Y%m%d-%H%M%S")}.csv'
    filt_df.to_csv(clean_file_name, index=False, date_format='%s')

    return clean_file_name, filt_df

@task
def upload_file_s3(file_names, bucket, object_name=None):
    """
    Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """
    logger = prefect.context.get("logger")

    # If S3 object_name was not specified, use file_name
    for file_name in file_names:
        if object_name is None:
            object_name = file_name

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logger.error(e)
            return False
        logger.info(f"Wrote to s3 " + file_name + bucket + object_name)
        return True
@task
def fetch_asg_instance_ips(autoscaling_groups):
    logger = prefect.context.get("logger")

    asg_instance_ips = []

    for asg in autoscaling_groups:

        asg_name = asg['asg_name']
        asg_region = asg['asg_region']

        asg_client = boto3.client('autoscaling',asg_region)
        asg_instance_ids = []
        asg_group_info = asg_client.describe_auto_scaling_groups(
          AutoScalingGroupNames=[asg_name]
        )

        for instance in asg_group_info['AutoScalingGroups'][0]['Instances']:
            asg_instance_ids.append(instance['InstanceId'])

        ec2_client = boto3.client('ec2',asg_region)
        for instance_id in asg_instance_ids:
            asg_instance_ips.append(
              ec2_client.describe_instances(
                InstanceIds=[instance_id]
              )['Reservations'][0]['Instances'][0]['PrivateIpAddress']
            )    

    logger.info('Found ASG instances with IPs' + str(asg_instance_ips))

    return asg_instance_ips

@task
def load_omnisci(instance_ips, tables_to_load: [Dict]) -> None:
    """Load the data into OmniSci, truncating existing data"""
    logger = prefect.context.get("logger")
    logger.info(tables_to_load)
    for instance_ip in instance_ips:
        try:
            logger.info('Connecting to OmniSciDB at IP ' + instance_ip)

            #Connect to the OmniSci instance
            #TODO hide these criteria in a params file
            with connect(user="admin", password="HyperInteractive", host=instance_ip, dbname="omnisci", port="6274", protocol="binary") as con:
                for tbl in tables_to_load:
                    logger.info(f"Proceeding to drop {tbl['table_name']} table")
                    con.execute(f"DROP TABLE IF EXISTS {tbl['table_name']}")
                    logger.info(f"Proceeding to load {len(tbl['table_data'].index)} rows into {tbl['table_name']} table")
                    con.load_table(tbl['table_name'], tbl['table_data'], create=True, preserve_index=False)
                    con.execute(f"grant select on table {tbl['table_name']} to demouser;")
                    logger.info('Table loaded successfully, proceeding to cleanup files')

        except Exception as ex:
            template = "An exception of type {0} occurred. Arguments:\n{1!r}"
            message = template.format(type(ex).__name__, ex.error_msg)
            logger.error(f'Failed to load data into OmniSciDB: {message}')
        finally:
            con.close()

#run 4pm and 5pm daily PST (I think the clock uses GMT so the cron schedule is 23/24 rather than 16/17)
daily_schedule = Schedule(clocks=[CronClock("30 0,1 * * *")])

#with Flow('COVID 19 flow', schedule=daily_schedule) as flow:
with Flow('COVID 19 flow') as flow:

    #extract tasks
    daily_covid_us_states_data_raw = extract_covid19_tracking_data()
    daily_jhu_github_data_raw = extract_gh_global_covid_ts_data()
    
    #transform
    daily_covid_data = transform_daily_covid_data(daily_jhu_github_data_raw)
    daily_us_states_data = transform_covid19_tracking_data(daily_covid_us_states_data_raw)
    #upload all files from each transform to s3
    upload_file_s3(daily_covid_data['file_names'], S3_BUCKET_NAME, S3_OBJECT_NAME)
    upload_file_s3(daily_us_states_data['file_names'], S3_BUCKET_NAME, S3_OBJECT_NAME)

    # Fetch list of db's to load
    instance_ips = fetch_asg_instance_ips(AUTOSCALING_GROUPS)

    #load db
    load_omnisci(instance_ips, daily_covid_data['data'])
    load_omnisci(instance_ips, [daily_us_states_data['data']])

flow.run()

