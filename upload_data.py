# This is simple example of my code.
# On previous job position I used to write scripts like this and dealt with this type of tasks.

import pandas as pd
# from sqlalchemy import create_engine
import os


file_url = "https://10d9b011-755b-4b59-9f64-dbfb2ae95d87.filesusr.com/ugd/b84e29_0668e8b9f89b455aafe12f024d788b71.csv?dn=Olympic%20Athletes%20and%20Events.csv"

def download_csv(file_url):
    try:
        os.system(f"wget {file_url}")
    except:
        print('ups, error')
    finally:
        print('sucessfully downloaded')

if __name__ == '__main__':
    download_csv(file_url)