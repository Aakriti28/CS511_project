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
    
    # figure 1: Average runtime for each stage of the data preparation pipeline (EDA, DT, DC) on different datasets.
    # a 3rowx2 col grid
    # col 1 is athlete, col 2 is loan
    # row 1 is EDA, row 2 is DT, row 3 is DC
    # use eda_fields, dt_fields, dc_fields to extract the data, take mean of algorithms in eda as the value for eda and so on
    
    fig, axs = plt.subplots(3, 2, figsize=(15, 15))
    for i, dataset in enumerate(datasets):
        for j, stage in enumerate([eda_fields, dt_fields, dc_fields]):
            algorithm_times = {}
            for algorithm in algorithms:
                flag_found = False
                for (d, t, mem, cpu), data in data_dump.items():
                    if d == dataset and t == algorithm:
                        flag_found = True
                        break
                if not flag_found:
                    logger.error(f'No data found for {dataset} dataset with {algorithm} algorithm')
                    continue

                times = []
                for field in stage:
                    if not data[data['method'] == field].empty:
                        times.append(data[data['method'] == field]['time'].sum())
                if times:
                    time = sum(times) / len(times)
                else:
                    time = 0
                    
                algorithm_times[algorithm] = time
            colors = plt.cm.get_cmap('viridis', len(algorithm_times))
            axs[j, i].bar(algorithm_times.keys(), algorithm_times.values(), color=[colors(k) for k in range(len(algorithm_times))])
            axs[j, i].set_title(f'{dataset.capitalize()} - {stage_to_name[stage[0]]}', fontsize=16)
            axs[j, i].set_ylabel('Time (s)', fontsize=14)
            axs[j, i].set_xlabel('Algorithm', fontsize=14)
            axs[j, i].set_xticklabels([label.upper() for label in algorithm_times.keys()])
            axs[j, i].tick_params(axis='both', which='major', labelsize=14)
            # xticks tilt 30 degrees
            xticks = axs[j, i].get_xticklabels()
            axs[j, i].set_xticklabels(xticks, rotation=30)
            
    fig.suptitle('Average Runtime for Each Stage of the Pipeline on Different Datasets', fontsize=20, fontweight='bold')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('figure1.png', dpi=300)
    
    
    # figure 2: scatterplot of runtime of approaches
    # 2 rows, multiple col: rows are athlete and loan, cols are fields in eda, dt, dc
    
    fig, axs = plt.subplots(2, 1, figsize=(20, 10))
    markers = ['o', 's', '^', 'P', 'X', 'D', 'v']
    for i, dataset in enumerate(datasets):
        for algorithm in algorithms:
            field_times = {}
            for j, field in enumerate(eda_fields + dt_fields + dc_fields):
                flag_found = False
                for (d, t, mem, cpu), data in data_dump.items():
                    if d == dataset and t == algorithm:
                        flag_found = True
                        break
                if not flag_found:
                    logger.error(f'No data found for {dataset} dataset with {algorithm} algorithm')
                    continue

                if not data[data['method'] == field].empty:
                    time = data[data['method'] == field]['time'].mean()
                    
                    # hardcode
                    if (dataset == 'loan' and field == 'drop_duplicates' and algorithm == 'datatable'):
                        logger.info(f"dropping {algorithm} for {field} in {dataset}")
                        continue
                    
                    field_times[field] = time
                    
                else:
                    logger.error(f'No data found for {field} in {dataset} dataset with {algorithm} algorithm')
            
            axs[i].scatter(field_times.keys(), field_times.values(), label=algorithm, marker=markers[algorithms.index(algorithm)], s=100)
        axs[i].set_title(f'{dataset.capitalize()}', fontsize=16, pad=30)
        axs[i].set_ylabel('Time (s)', fontsize=14)
        axs[i].tick_params(axis='both', which='major', labelsize=14)
        # incline xticks by 30 degrees
        xticks = axs[i].get_xticklabels()
        axs[i].set_xticklabels(xticks, rotation=30)
            
        # make vertical lines at len(eda_fields), len(eda_fields) + len(dt_fields), len(eda_fields) + len(dt_fields) + len(dc_fields)
        # this makes 3 sections eda, dt, dc
        axs[i].axvline(x=len(eda_fields) - 0.5, color='black')
        axs[i].axvline(x=len(eda_fields) + len(dt_fields) - 0.5, color='black')
            
        # add heading at the top of the three sections, eda, dt, dc
        # use axs[i] bound of the figure to get the x and y coordinates
        axs[i].text((len(eda_fields) - 0.5)/2, axs[i].get_ylim()[1], 'Exploratory Data Analysis', ha='center', va='bottom', fontsize=14)
        axs[i].text(len(eda_fields) + len(dt_fields)/ 2 - 0.5, axs[i].get_ylim()[1], 'Data Transformation', ha='center', va='bottom', fontsize=14)
        axs[i].text(len(eda_fields) + len(dt_fields) + len(dc_fields) / 2 - 0.5, axs[i].get_ylim()[1], 'Data Cleaning', ha='center', va='bottom', fontsize=14)
        
        axs[i].grid()
        handles, labels = axs[i].get_legend_handles_labels()
        labels = [algo_to_name_map[label] for label in labels]
        axs[i].legend(handles, labels, fontsize=15, loc='upper left', bbox_to_anchor=(1, 1))
        
    fig.suptitle('Runtime of Frameworks for Different Stages of the Processing Pipeline',fontsize=20, fontweight='bold')
    plt.tight_layout()
    plt.savefig('figure2.png', dpi=300)

    
    # figure 3, plot per dataset, technique data load/saving time
    # 2 rows, 2 col: first row is read, second row is write, first col is athlete, second col is loan
    # entries are algorithms
    
    fig, axs = plt.subplots(2, 2, figsize=(15, 15))

    
    for i, dataset in enumerate(datasets):
        for j, method in enumerate(['load_dataset', 'to_csv']):
            algorithm_times = {}
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
                    time = data[data['method'] == method]['time'].mean()
                    algorithm_times[algorithm] = time
                else:
                    logger.error(f'No data found for {method} in {dataset} dataset with {algorithm} algorithm')

            colors = plt.cm.get_cmap('viridis', len(algorithm_times))
            axs[j, i].bar(algorithm_times.keys(), algorithm_times.values(), color=[colors(k) for k in range(len(algorithm_times))])
            axs[j, i].set_title(f'{dataset.capitalize()} - {"Read" if method == "load_dataset" else "Write"}', fontsize=16)
            axs[j, i].set_ylabel('Time (s)', fontsize=14)
            axs[j, i].set_xlabel('Algorithm', fontsize=14)
            axs[j, i].set_xticklabels([algo_to_name_map[label] for label in algorithm_times.keys()], rotation=30)
            axs[j, i].tick_params(axis='both', which='major', labelsize=14)
            
    fig.suptitle('Data Loading and Saving Time Across Frameworks', fontsize=20, fontweight='bold')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('figure3.png', dpi=300)
