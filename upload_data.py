# This is simple example of my code.
# On previous job position I used to write scripts like this and dealt with this type of tasks.
import os
from os.path import join, dirname
from dotenv import load_dotenv

import pandas as pd
from sqlalchemy import create_engine


def download_csv(file_url, file_name):
    try:
        os.system(f"wget {file_url} -O {file_name}")
    except:
        print(
            'Error during the downlodaing process. Pls check url and internet connection.')
    else:
        print(f'File {file_name} sucessfully downloaded.')


def upload_to_pg(engine, file_name_athlets, file_name_regions, table_name_w, table_name_s, table_name_r):

    df_regions = pd.read_csv(file_name_regions)
    df_regions = df_regions.loc[:, ['NOC', 'region']]

    df_regions.head(n=0).to_sql(name=table_name_r, con=engine,
                                if_exists='replace', index=False)
    df_regions.to_sql(name=table_name_r, con=engine,
                      if_exists='append', index=False)

    print("Uploading process finished for regions data.")

    df_iter = pd.read_csv(file_name_athlets, iterator=True, chunksize=100000)

    df = next(df_iter)

    df_winter = df[df.Season == 'Winter']
    df_summer = df[df.Season == 'Summer']

    df_winter.head(n=0).to_sql(name=table_name_w, con=engine,
                               if_exists='replace', index=False)
    df_summer.head(n=0).to_sql(name=table_name_s, con=engine,
                               if_exists='replace', index=False)

    df_winter.to_sql(name=table_name_w, con=engine,
                     if_exists='append', index=False)
    df_summer.to_sql(name=table_name_s, con=engine,
                     if_exists='append', index=False)

    print("One chunk of athlets data has been uploaded into postgres.")

    while True:
        try:
            df = next(df_iter)

            df_winter = df[df.Season == 'Winter']
            df_summer = df[df.Season == 'Summer']

            df_winter.to_sql(name=table_name_w, con=engine,
                             if_exists='append', index=False)
            df_summer.to_sql(name=table_name_s, con=engine,
                             if_exists='append', index=False)

            print("One chunk of athlets data has been uploaded into postgres.")
        except StopIteration:
            print("Uploading process finished for athlets data.")
            break


def simple_validation(engine, file_name, table_name):

    query_sql = f"SELECT count(1) FROM {table_name};"

    try:
        df = pd.read_csv(file_name)

        input_file_rows = df.shape[0]

        df_from_pg = pd.read_sql(query_sql, con=engine)

        output_rows = df_from_pg.loc[:, 'count'].item()
    except: 
        print("Smth went wrong during validation process.")
    else:
        print(f"CSV file {file_name} contains {input_file_rows} rows.")
        print(f"Uploaded to postgres {output_rows} rows into {table_name}.")

if __name__ == '__main__':

    dotenv_path = join(dirname(__file__), '.env')
    load_dotenv(dotenv_path)

    FILE_URL_athlets = os.getenv("FILE_URL_athlets")
    FILE_URL_regions = os.getenv("FILE_URL_regions")

    FILE_NAME_athlets = os.getenv("FILE_NAME_athlets")
    FILE_NAME_regions = os.getenv("FILE_NAME_regions")

    USER = os.getenv("USER")
    PASSWORD = os.getenv("PASSWORD")
    HOST = os.getenv("HOST")
    PORT = os.getenv("PORT")
    DB = os.getenv("DB")

    TABLE_NAME_WINTER = os.getenv("TABLE_NAME_WINTER")
    TABLE_NAME_SUMMER = os.getenv("TABLE_NAME_SUMMER")
    TABLE_NAME_REGIONS = os.getenv("TABLE_NAME_REGIONS")

    try:
        download_csv(FILE_URL_athlets, FILE_NAME_athlets)
        download_csv(FILE_URL_regions, FILE_NAME_regions)

        try:
            engine = create_engine(
                f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')
            engine.connect()
        except:
            print(
                "Cannot connect to postgres. Pls check that docker container has been runned succesfully.")

        upload_to_pg(engine, FILE_NAME_athlets, FILE_NAME_regions,
                     TABLE_NAME_WINTER, TABLE_NAME_SUMMER, TABLE_NAME_REGIONS)

        simple_validation(engine, FILE_NAME_regions, TABLE_NAME_REGIONS)
        simple_validation(engine, FILE_NAME_athlets, TABLE_NAME_WINTER)
        simple_validation(engine, FILE_NAME_athlets, TABLE_NAME_SUMMER)
    except:
        print("Smth went wrong, sorry:( ")
    else:
        print("This simple script finished sucessfully. Have a nice day:)")
