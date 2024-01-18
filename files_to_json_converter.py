import re
import json 
import glob
import pandas as pd 
import os
import sys

def get_columns_details(schemas, ds_name, sorting_key='column_position'):
    column_details = schemas[ds_name]
    columns = sorted(column_details, key=lambda con: con[sorting_key])
    return [col['column_name'] for col in columns]

def read_csv(file, schemas, ds_name):
    columns = get_columns_details(schemas, ds_name)
    df = pd.read_csv(file, names=columns)   
    return df

def to_json(df, tgt_base_dr, ds_name, file_name):
    json_file_path = f'{tgt_base_dr}/{ds_name}/{file_name}'
    os.makedirs(f'{tgt_base_dr}/{ds_name}', exist_ok=True)
    df.to_json(json_file_path, orient='records', lines=True)

def file_converter(scr_base_dir, tg_base_dir, ds_name):
    schemas = json.load(open(f'{scr_base_dir}/schemas.json'))
    files = glob.glob(f'{scr_base_dir}/{ds_name}/part-*')
    if len(files) == 0:
        raise NameError(f'No files found for {ds_name}')
    for file in files:
        df = read_csv(file, schemas, ds_name)
        file_name = re.split('[/\\\]', file)[-1]
        to_json(df, tg_base_dir, ds_name, file_name)
        
def process_file(ds_names=None):
    scr_base_dir = os.environ.get('SRC_BASE_DIR')  # $Env:SRC_BASE_DIR = "data/retail_db"
    tg_base_dir = os.environ.get('TG_BASE_DIR')    # $Env:TG_BASE_DIR  = "data/retail_db_json1"
    schemas = json.load(open(f'{scr_base_dir}/schemas.json'))
    
    if not ds_names:
        ds_names = list(schemas.keys())
    else:
        try:
            ds_names = json.loads(ds_names)
        except json.decoder.JSONDecodeError:
            ds_names = [ds_names]

    for ds_name in ds_names:
        try:
            print(f'Processing {ds_name}')
            file_converter(scr_base_dir, tg_base_dir, ds_name)
        except NameError as ne:
            print(f'Error proccessing {ds_name}')
            pass
        
if __name__ == '__main__':
    if len(sys.argv) == 2:
        ds_names = sys.argv[1]
        process_file(ds_names)
    else:
        process_file()
