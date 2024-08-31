import psycopg2
import pandas as pd
# import optparse
# import numpy as np
# import petl as etl
# import pandas_gbq
import datetime as dt
# import os
# import pytz
# import sys

import gspread
from pathlib import Path
from datetime import datetime
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
# from dependencies import db_config

from google.auth.transport.requests import AuthorizedSession
from google.oauth2 import service_account

from gspread.exceptions import WorksheetNotFound

def read_td(table_name, sheet):
    worksheet = sheet.worksheet('CoIMS_master_stg')
    list_of_lists = worksheet.get_all_values()

    # df = pd.DataFrame()
    df = pd.DataFrame(list_of_lists)
    
    # df = pd.concat([df,list_of_lists])
    df.columns = df.iloc[0]
    df = df.reindex(df.index.drop([0]))
    df = df.loc[df['Important Tables'] == '{}'.format(table_name)]

    if df.empty:
        table_description = ''
    else:
        table_description = df.iloc[0]['Description']
    print(table_description)
    return table_description


def read_cd(dataset, table_name, sheet):
    worksheet = sheet.worksheet(table_name)
    list_of_lists = worksheet.get_all_values()
    # df = pd.DataFrame()
    df = pd.DataFrame(list_of_lists)
    # df = pd.concat([df, list_of_lists])
    
    # ambil nama kolom
    df.columns = df.iloc[0]
    # drop baris yang menjadi kolom
    df = df.reindex(df.index.drop([0]))

    print(df)
    return df

def main(table_name, query):
    # Tabulate
    pd.options.display.max_colwidth = 100000

    # Attach credential file from developer google (API)
    scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
    credentials = service_account.Credentials.from_service_account_file('/Users/arifahfariza/Documents/repo_alami_env/service_account_alami.json', scopes=scope)
    gc = gspread.Client(auth=credentials)
    gc.session = AuthorizedSession(credentials)

    # Target dataset
    dataset = 'temp_7_days'

    # https://docs.google.com/spreadsheets/d/1gx4ptk_gSBAxs7DDsV_tV75EoWhudrMOLmH3YfQPcjg/edit?usp=sharing
    # Create the pandas DataFrame
    google_sheet_id = '1gx4ptk_gSBAxs7DDsV_tV75EoWhudrMOLmH3YfQPcjg'
    sheet = gc.open_by_key(google_sheet_id)

    # df = read_cd(dataset, table_name, sheet)
    # print('run function read_cd() 1 is success')
    # print(df.head)
        
    try:
        # table_description = read_td(table_name, sheet)
        # print('run function read_td() is success')
        # print(table_description)
        df = read_cd(dataset, table_name, sheet)
        print('run function read_cd() is success')
        print(df.head)

    except WorksheetNotFound as e:
        print(e)
        print('gspread.exceptions.WorksheetNotFound: ' + table_name)
    except Exception as e:
        raise e
    print('done -----')
    # return 'SUCCESS'
    
if __name__ == "__main__":
    main("loan_application","")