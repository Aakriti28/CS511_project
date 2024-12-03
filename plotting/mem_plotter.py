import numpy as np, pandas as pd
import matplotlib.pyplot as plt
import itertools
import glob
import os

import logging
logger = logging.getLogger(__name__)
formatter = logging.Formatter('[%(levelname)s]: %(message)s')
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
file_handler = logging.FileHandler('./plotter.log')
file_handler.setFormatter(formatter)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)
logger.setLevel(logging.DEBUG)

# eda_fields = ['load_dataset', 'to_csv', 'get_columns', 'locate_null_values', 'sort']
eda_fields = ['get_columns', 'locate_null_values', 'sort']
dt_fields = ['join', 'delete_columns', 'rename_columns', 'calc_column', 'cast_columns_types', 'replace']
dc_fields = ['groupby', 'drop_duplicates', 'fill_nan']

stage_to_name = {
    'get_columns': 'Exploratory Data Analysis',
    'join': 'Data Transformation',
    'groupby': 'Data Cleaning'
}
algo_to_name_map = {
    'pandas': 'Pandas',
    'modin_dask': 'Modin Dask',
    'pyspark_pandas': 'PySpark Pandas',
    'rapids': 'Rapids',
    'datatable': 'Datatable',
    'spark': 'Spark',
    'polars': 'Polars'
}

algorithms = ['pandas', 'modin_dask', 'pyspark_pandas', 'rapids', 'datatable', 'spark', 'polars']
datasets = ['athlete', 'loan']

if __name__ == '__main__':
    # gather data .csv files from ../results/{athlete/loan}/{algorithm_name}_mem*_cpu*/<most recent file>.csv
    data_dump = {}
    for (dataset, algorithm) in itertools.product(datasets, algorithms):
        logger.info(f'Processing {dataset} dataset with {algorithm} algorithm')
        path = f'../results/{dataset}/{algorithm}_mem*_cpu*/'
        available_files = glob.glob(path)
        if len(available_files) > 1:
            logger.warning(f'Multiple files found in {path}. Using the most recent one.')
        if len(available_files) == 0:
            logger.critical(f'No folders found in {path}')
            continue
            
        # sort folders by creation date
        available_files.sort(key=os.path.getmtime)
        path = available_files[-1]
        
        # get the most recent file
        files = glob.glob(f'{path}/*.csv')
        if len(files) > 1:
            logger.warning(f'Multiple files found in {path}. Using the most recent one.')
        if len(files) == 0:
            logger.error(f'No .csv files found in {path}')
            continue
        files.sort(key=os.path.getmtime)
        path = files[-1]
        
        data = pd.read_csv(path)
        # path = ../results/athlete/pandas_mem27_cpu1/pandas_run_2024_11_30_02_41_34.csv
        mem_s = path.split('mem')[1].split('_')[0]
        cpu_s = path.split('cpu')[1].split('/')[0]
        data_dump[(dataset, algorithm, mem_s, cpu_s)] = data
    
    # save data_dump dict to a file
    with open('data_dump.json', 'w') as f:
        import json
        json.dump({str(k): v.to_dict() for k, v in data_dump.items()}, f, indent=4)
    
    # figure 3, plot per dataset, technique data load/saving time
    # 2 rows, 2 col: first row is read, second row is write, first col is athlete, second col is loan
    # entries are algorithms
    
    fig, axs = plt.subplots(2, 2, figsize=(15, 15))

    for i, dataset in enumerate(datasets):
        for j, method in enumerate(['load_dataset', 'to_csv']):
            algorithm_memorys = {}
            for algorithm in algorithms:
                flag_found = False
                for (d, t, mem, cpu), data in data_dump.items():
                    if d == dataset and t == algorithm:
                        flag_found = True
                        break
                if not flag_found:
                    logger.error(f'No data found for {dataset} dataset with {algorithm} algorithm')
                    continue

                if not data[data['method'] == method].empty:
                    memory = data[data['method'] == method]['memory'].mean()
                    algorithm_memorys[algorithm] = memory // 1e9
                else:
                    logger.error(f'No data found for {method} in {dataset} dataset with {algorithm} algorithm')

            colors = plt.cm.get_cmap('viridis', len(algorithm_memorys))
            axs[j, i].bar(algorithm_memorys.keys(), algorithm_memorys.values(), color=[colors(k) for k in range(len(algorithm_memorys))])
            axs[j, i].set_title(f'{dataset.capitalize()} - {"Read" if method == "load_dataset" else "Write"}', fontsize=16)
            axs[j, i].set_ylabel('Memory (GB)', fontsize=14)
            axs[j, i].set_xlabel('Algorithm', fontsize=14)
            axs[j, i].set_xticklabels([algo_to_name_map[label] for label in algorithm_memorys.keys()], rotation=30)
            axs[j, i].tick_params(axis='both', which='major', labelsize=14)
            
    fig.suptitle('Data Loading and Saving Memory Across Frameworks', fontsize=20, fontweight='bold')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('mem_figure3.png', dpi=300)
