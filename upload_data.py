# This is simple example of my code.
# On previous job position I used to write scripts like this and dealt with this type of tasks.
import os

import pandas as pd
from sqlalchemy import create_engine

def download_csv(file_url, file_name):
    try:
        os.system(f"wget {file_url} -O {file_name}")
    except:
        print('Error during the downlodaing process. Pls check url and internet connection.')
    else:
        print(f'File {file_name} sucessfully downloaded.')

def upload_to_pg(engine, file_name, table_name_w, table_name_s):

    df_iter = pd.read_csv(file_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df_winter = df[df.Season=='Winter']
    df_summer = df[df.Season=='Summer']

    df_winter.head(n=0).to_sql(name=table_name_w, con=engine, if_exists='replace')
    df_summer.head(n=0).to_sql(name=table_name_s, con=engine, if_exists='replace')

    df_winter.to_sql(name=table_name_w, con=engine, if_exists='append')
    df_summer.to_sql(name=table_name_s, con=engine, if_exists='append')

    print("One chunk of data has been uploaded.")

    while True: 
        try:
            df = next(df_iter)

            df_winter = df[df.Season=='Winter']
            df_summer = df[df.Season=='Summer']

            df_winter.to_sql(name=table_name_w, con=engine, if_exists='append')
            df_summer.to_sql(name=table_name_s, con=engine, if_exists='append')

            print("One chunk of data has been uploaded.")
        except StopIteration:
            print("Uploading process finished.")
            break

def simple_validation(engine, file_name):

    df = pd.read_csv(file_name)
    input_file_rows = df.shape[0]

    query_w =  f"SELECT count(1) FROM {table_name_w}"
    query_s =  f"SELECT count(1) FROM {table_name_s}"

    df_w = pd.read_sql(query_w, con=engine)
    df_s = pd.read_sql(query_s, con=engine)

    output_winter_rows = df_w.loc[:, 'count'].item()
    output_summer_rows = df_s.loc[:, 'count'].item()

    print(f"CSV file contains {input_file_rows}.") 
    print(f"Uploaded to postgres {output_winter_rows} for winter and {output_summer_rows} for summer, at all: {output_winter_rows + output_summer_rows}.")


if __name__ == '__main__':

    FILE_URL = "https://10d9b011-755b-4b59-9f64-dbfb2ae95d87.filesusr.com/ugd/b84e29_0668e8b9f89b455aafe12f024d788b71.csv?dn=Olympic%20Athletes%20and%20Events.csv"
    FILE_NAME = "athlets_and_events.csv"
    #file_url = "https://10d9b011-755b-4b59-9f64-dbfb2ae95d87.filesusr.com/ugd/b84e29_aab029d948a64ad3803f945d49071b30.csv?dn=Olympic%20Nations%20and%20Region%20Look-Up.csv"

    USER = 'root'
    PASSWORD = 'root'
    HOST = 'pgdatabase'
    PORT ='5432'
    DB ='olympic_database'

    table_name_w = "athlets_and_events_winter"
    table_name_s = "athlets_and_events_summer"

    try:
        download_csv(FILE_URL, FILE_NAME)

        engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}')

        upload_to_pg(engine, FILE_NAME, table_name_w, table_name_s)

        simple_validation(engine, FILE_NAME)
    except:
        pass
    else:
        print("This simple script finished sucessfully. Have a nice day:)")