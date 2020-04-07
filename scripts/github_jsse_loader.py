import pandas as pd
import prefect
from prefect import task, Flow, unmapped
from prefect.schedules import CronSchedule
import requests
from io import StringIO
from datetime import datetime, timedelta
import os
from os import path
from typing import Dict
from prefect.tasks.aws.s3 import S3Upload
import omniscitasks

SOURCE = 'jhu'

@task
def extract_gh_global_covid_ts_data() -> Dict[str, pd.DataFrame]:
    """Download the time series csv files into dataframes"""

    logger = prefect.context.get("logger")

    config = prefect.config.sources[SOURCE]
    locations = config['locations']
    file_types = config['file_types']
    source_file_name = config['source_file_name']
    url = config['url']

    dfs={}

    for loc in locations:
        loc_dict = {}
        for file_type in file_types:
            file_handle=f'{source_file_name}_{file_type}_{loc}.csv'
            file_url=f'{url}{file_handle}'

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
        filename, consolidated_df = process_daily_covid_data( confirmed_df_ts, deaths_df_ts, testing_df_ts )

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
    filt_df.loc[filt_df['country_region'] == 'Korea, South','country_region'] = 'South Korea'
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
    logger.info(f"Record count:{len(filt_df.index)}. Writing to output file")

    filt_df = filt_df.astype({  'lat':'float32',\
                                'lon':'float32',\
                                'confirmed':'int32',\
                                'deaths':'int32',\
                                'deaths_diff':'int32',\
                                'confirmed_diff':'int32'})

    #TODO parametrize this
    row_count = len(filt_df.index)

    if (row_count > 0):
        file_prefix = prefect.config.sources[SOURCE]['file_prefix']
        flowrun_suffix = datetime.now().strftime('%Y%m%d-%H%M%S')

        output_dir = prefect.config['output-directory']
        if(not os.path.exists(output_dir)):
            os.makedirs(output_dir)
        
        current_output_filename = f"{file_prefix}-{flowrun_suffix}.csv"
        output_filepath = os.path.join(output_dir, current_output_filename)
        logger.info(f'Writing output file, {row_count} rows to:{output_filepath}')
        filt_df.to_csv(output_filepath, index=False)
        
        return output_filepath, filt_df
    
    return None

with Flow('COVID 19 JHU Data Pipeline') as flow:
    logger = prefect.utilities.logging.get_logger()

    config = prefect.config.sources[SOURCE]
    destinations = prefect.config['destinations']['omnisci']
    s3_bucket = prefect.context.secrets['s3_bucket_name']

    #extract tasks
    raw_data = extract_gh_global_covid_ts_data()

    #transform tasks
    transformed_data = transform_daily_covid_data(raw_data)

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

         load_task.map(data = transformed_data['data'], drop_existing = unmapped(True))
    
with prefect.context() as c:
    if(prefect.config['runmode'] == 'schedule'):
        schedule = CronSchedule(cron = prefect.config.sources[SOURCE]['cron_string'], start_date=datetime.now())    
        flow.schedule = schedule

    state=flow.run()