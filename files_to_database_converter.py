import re
import json
import glob
import pandas as pd
import os
import sys
import sqlalchemy
import dotenv
import logging

# Load environment variables from .env file
dotenv.load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_columns_details(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda con: con[sorting_key])
    return [col['column_name'] for col in columns]

#This function reads a CSV file using pandas read_csv with a specified chunk size and returns a DataFrame "reader"
def read_csv(files, schemas):
    file_path_list = re.split('[/\\\]', files)
    ds_name = file_path_list[-2]
    columns = get_columns_details(schemas, ds_name)
    df_reader = pd.read_csv(files, names=columns, chunksize=10000)
    return df_reader

def to_sql(df, db_conn_uri, ds_name):
    with sqlalchemy.create_engine(db_conn_uri).connect() as connection:
        df.to_sql(ds_name, connection, if_exists='append', index=False)

def db_loader(scr_base_dir, db_conn_uri, ds_name):
    schemas = json.loads(open(f'{scr_base_dir}/schemas.json').read())
    files = glob.glob(f'{scr_base_dir}/{ds_name}/part-*')

    if not files:
        raise NameError(f'No files found in {ds_name}')

    truncate_tables = ['orders']
    with sqlalchemy.create_engine(db_conn_uri).connect() as engine:
        for table in truncate_tables:
            engine.execute(f'TRUNCATE TABLE {table};')

        for file in files:
            try:
                df_reader = read_csv(file, schemas)
                for idx, df in enumerate(df_reader):
                    print(f'Populating chunk {idx} of {df.shape}')
                    to_sql(df, db_conn_uri, ds_name)
            except Exception as e:
                logger.error(f'Error processing chunk {idx}: {e}')
                
#for all the dataset
def process_files(ds_names=None):
    scr_base_dir = os.environ.get('SRC_BASE_DIR')
    db_host = os.environ.get('DB_HOST')
    db_port = os.environ.get('DB_PORT')
    db_name = os.environ.get('DB_NAME')
    db_user = os.environ.get('DB_USER')
    db_pass = os.environ.get('DB_PASS')
    db_conn_uri = f'postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}'

    schemas = json.loads(open(f'{scr_base_dir}/schemas.json').read())
    # for all
    if not ds_names:
        ds_names = schemas.keys()
    # for specific
    for ds_name in ds_names:
        try:
            logger.info(f'Processing {ds_name}')
            db_loader(scr_base_dir, db_conn_uri, ds_name)
        except NameError as ne:
            logger.error(ne)
            pass
        except Exception as e:
            logger.error(e)
            pass
        finally:
            logger.info(f'Finished processing {ds_name}')

if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = sys.argv[1]
        process_files([ds_names])
    else:
        process_files()

