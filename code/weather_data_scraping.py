import pandas as pd 
import numpy as np 

import requests
import sqlalchemy as sqla
from sqlalchemy import create_engine

def js_to_df(date):
    """
    Get weather data for a single day from the weather.com api, return a pandas df
    Inputs:
        date (str): date string
    Returns:
        df (pandas df): df for SF single day weather
    """
    url = "https://api.weather.com/v1/location/KSFO:9:US/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=m&startDate=" + date
    req = requests.get(url, verify=False)
    js = req.json()
    df = pd.DataFrame.from_dict(pd.json_normalize(js['observations']), orient='columns')
    df['date']=pd.to_datetime(date)
    df=df[['date','temp','precip_hrly']]
    df.precip_hrly = np.where(df.precip_hrly.isnull(), 0, df.precip_hrly).astype('int')
    df = df.groupby(['date'], as_index=False).agg({'temp':'mean', 'precip_hrly':'sum'})
    return df

def scrap_date_range(start, end, outpath):
    """
    Get weather data for a single day from the weather.com api, write the output
        dataframe to a specified path
    Inputs:
        start (str): start date string
        end (str): end date string
        outpath (str): output path to write the .csv file
    Returns:
        None
    """

    date_range = pd.date_range(start=start, end=end)
    date_str = list(date_range.strftime("%Y%m%d"))

    sqlite_file = 'weather.sqlite'
    engine = sqla.create_engine('sqlite:///' + sqlite_file)

    def get_data(date_str):
        year_ind = 2002
        i=0
        for dt in date_str:
            if dt[:4] != str(year_ind):
                year_ind += 1
                print(f"\tNow scaping SF weather data for year {year_ind}...")
            df = js_to_df(date=dt)
            if i == 0:
                df.to_sql('weather', con=engine, if_exists='replace')
            else:
                df.to_sql('weather', con=engine, if_exists='append')
            i += 1

    get_data(date_str)

    sql = """
    select date, temp, precip_hrly as precip from weather
    """

    df_SF_weather = pd.read_sql_query(sql, engine, parse_dates='date')
    n, d = df_SF_weather.shape
    print(f"Writing {n} rows and {d} columns to {outpath}...")
    df_SF_weather.to_csv(outpath, index=False)
