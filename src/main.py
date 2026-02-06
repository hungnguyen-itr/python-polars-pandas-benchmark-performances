from copy import deepcopy

import pandas as pd
import polars as pl
import numpy as np
import time
from prettytable import PrettyTable


def _create_data_test(
        data: pl.DataFrame,
):
    all_data = deepcopy(data)
    for i in range(2):
        max_file_index  = data['FILE_INDEX'].max() + 1
        max_epoch       = data['EPOCH'].max()
        
        epoch_diff = np.diff(data['EPOCH'].to_numpy())
        epoch_diff = np.insert(epoch_diff, 0, 0)
        epoch_diff = 100 + max_epoch + epoch_diff
        
        data = (
            data
            .with_columns(
                    [
                        (pl.col('FILE_INDEX') + max_file_index)
                        .alias('FILE_INDEX'),
                        
                        pl.Series('EPOCH', epoch_diff).alias('EPOCH')
                    ]
            )
        )
        pass
        all_data = pl.concat([all_data, data])
        
    print(all_data)
    
    all_data.write_parquet(POLARS_PATH)
    all_data.to_pandas().to_parquet(PANDAS_PATH, engine='fastparquet')
    np.save(NUMPY_PATH, all_data.to_numpy())
    
    return data


def _summary(
        action_type: str = 'read',
) -> None:
    time_process = dict()
    time_process['polars']  = list()
    time_process['pandas']  = list()
    time_process['numpy']   = list()
    
    for i in range(N_LOOP):
        start_time = time.time()
        if action_type == 'read':
            start_time = time.time()
            pl.read_parquet(POLARS_PATH)
        elif action_type == 'write':
            df = pl.read_parquet(POLARS_PATH)
            start_time = time.time()
            df.write_parquet(POLARS_PATH.replace('.parquet', f'-{action_type}.parquet'))
        elif action_type == 'sort':
            df = pl.read_parquet(POLARS_PATH)
            start_time = time.time()
            df.sort('EPOCH')
        elif action_type == 'filter':
            df = pl.read_parquet(POLARS_PATH)
            start_time = time.time()
            df.with_columns((pl.col("EPOCH").diff()).alias("RR_interval")).filter(pl.col("RR_interval") > 400)
            
        elif action_type == 'query':
            df = pl.read_parquet(POLARS_PATH)
            start_time = time.time()
            df.filter(pl.col("BEAT_TYPE") == 60)
            
        elif action_type == 'update':
            df = pl.read_parquet(POLARS_PATH)
            start_time = time.time()
            df.with_columns(pl.when(pl.col("BEAT_TYPE") == 60).then(1).otherwise(pl.col("BEAT_TYPE")).alias("BEAT_TYPE"))
        time_process['polars'].append(time.time() - start_time)
        
        start_time = time.time()
        if action_type == 'read':
            start_time = time.time()
            pd.read_parquet(PANDAS_PATH)
        elif action_type == 'write':
            df = pd.read_parquet(PANDAS_PATH)
            start_time = time.time()
            df.to_parquet(PANDAS_PATH.replace('.parquet', f'-{action_type}.parquet'))
        elif action_type == 'sort':
            df = pd.read_parquet(PANDAS_PATH)
            start_time = time.time()
            df.sort_values(by=['EPOCH'], inplace=True)
        elif action_type == 'filter':
            df = pd.read_parquet(PANDAS_PATH)
            start_time = time.time()
            df["RR_interval"] = df["EPOCH"].diff()
            filtered = df[df["RR_interval"] > 400]
        elif action_type == 'query':
            df = pd.read_parquet(PANDAS_PATH)
            start_time = time.time()
            df["RR_interval"] = df["EPOCH"].diff()
            filtered = df[df["BEAT_TYPE"] == 60]
        elif action_type == 'update':
            df = pd.read_parquet(PANDAS_PATH)
            start_time = time.time()
            df["BEAT_TYPE"] = df["BEAT_TYPE"].apply(lambda x: 1 if x == 60 else x)
        time_process['pandas'].append(time.time() - start_time)
        
        start_time = time.time()
        if action_type == 'read':
            start_time = time.time()
            np.load(NUMPY_PATH)
        elif action_type == 'write':
            arr = np.load(NUMPY_PATH)
            start_time = time.time()
            np.save(NUMPY_PATH.replace('.npy', f'-{action_type}.npy'), arr)
        elif action_type == 'sort':
            arr = np.load(NUMPY_PATH)
            start_time = time.time()
            arr.sort(axis=0)
        elif action_type == 'filter':
            arr = np.load(NUMPY_PATH)
            start_time = time.time()
            rr_intervals = np.insert(np.diff(arr[:, 0]), 0, 0)
            filtered = arr[rr_intervals > 400]
        elif action_type == 'query':
            arr = np.load(NUMPY_PATH)
            start_time = time.time()
            filtered = arr[np.flatnonzero(arr[:, 3] == 60)]
        elif action_type == 'update':
            arr = np.load(NUMPY_PATH)
            start_time = time.time()
            beat_type = arr[:, 3]
            beat_type = np.where(beat_type == 60, 1, beat_type)
        time_process['numpy'].append(time.time() - start_time)
    
    tables = PrettyTable(['packages', 'minTime (s)', 'avgTime (s)', 'maxTime (s)'])
    tables.title = action_type.upper()
    for key, value in time_process.items():
        min_t = round(np.min(value), 6)
        max_t = round(np.max(value), 6)
        avg_t = round(np.mean(value), 6)
        
        tables.add_row([key, min_t, avg_t, max_t])
    
    list(map(
            lambda x: print('\t' + x),
            tables.get_string().splitlines()
    ))
    pass


def _test_load_file() -> None:
    _summary(action_type='read')
    
    
def _test_write_file() -> None:
    _summary(action_type='write')
    

def _test_sort_file() -> None:
    _summary(action_type='sort')


def _test_filter_file() -> None:
    _summary(action_type='filter')
    
    
def _test_query_file() -> None:
    _summary(action_type='query')
    
    
def _test_update_file() -> None:
    _summary(action_type='update')
    

def main():
    # _create_data_test(data=pl.read_parquet(DATA_PATH))
    
    # _test_load_file()
    # _test_write_file()
    # _test_sort_file()
    # _test_filter_file()
    # _test_query_file()
    _test_update_file()
    

if __name__ == '__main__':
    DATA_PATH = './data/data-test.parquet'
    
    NUMPY_PATH  = './data/data-test-numpy.npy'
    POLARS_PATH = './data/data-test-polars.parquet'
    PANDAS_PATH = './data/data-test-pandas.parquet'
    
    N_LOOP = 10
    
    main()
