# This is simple example of my code.
# On previous job position I used to write scripts like this and dealt with this type of tasks.

import pandas as pd
# from sqlalchemy import create_engine
import os


file_url = "https://10d9b011-755b-4b59-9f64-dbfb2ae95d87.filesusr.com/ugd/b84e29_0668e8b9f89b455aafe12f024d788b71.csv?dn=Olympic%20Athletes%20and%20Events.csv"
#file_url = "https://10d9b011-755b-4b59-9f64-dbfb2ae95d87.filesusr.com/ugd/b84e29_aab029d948a64ad3803f945d49071b30.csv?dn=Olympic%20Nations%20and%20Region%20Look-Up.csv"

user = 'root'
password = 'root'
host = 'localhost'
port='5432'
db='olympic_database'

file_name = "athlets_and_events.csv"

def download_csv(file_url, file_name):
    try:
        os.system(f"wget {file_url} -O {file_name}")
    except:
        print('ups, error')
    finally:
        print('sucessfully downloaded')

def upload_to_pg():

    df_iter = pd.read_csv("data.csv", iterator=True, chunksize=100000)

    df = next(df_iter)

    df_winter = df[df.Season=='Winter']
    df_summer = df[df.Season=='Summer']

    table_name_w = "athlets_and_events_winter"
    table_name_s = "athlets_and_events_summer"

    df.head(n=0).to_sql(name=table_name_w, con=engine, if_exists='replace')
    df.head(n=0).to_sql(name=table_name_s, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True: 
        try:
            t_start = time()
                
            df = next(df_iter)

            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break


if __name__ == '__main__':
    download_csv(file_url, file_name)

    df = pd.read_csv("athlets_and_events.csv")
    print(df.head())
