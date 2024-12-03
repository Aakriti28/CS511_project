import numpy as np, matplotlib.pyplot as plt
import pandas as pd, json, os, sys
import glob
import logging
import pprint
logger = logging.getLogger(__name__)
# set format including level
logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.INFO)

operation_to_name = {
    'load_dataset': 'Load Dataset',
    'locate_null_values': 'Locate Null Values',
    'drop_duplicates': 'Drop Duplicates',
    'join': 'Join'
}

if __name__ == '__main__':
    num_cpus = [1,2,4,8]
    required_methods = ['load_dataset', 'locate_null_values', 'drop_duplicates', 'join']
    
    cpu_to_df = {}
    for num_cpu in num_cpus:
        csv_files = glob.glob(f'../results_cpu/modin_dask_mem80_cpu{num_cpu}/*.csv')
        if len(csv_files) != 1:
            logger.critical(f'Expected 1 csv file, but got {len(csv_files)}')
        data_frames = pd.read_csv(csv_files[0])
        # if multiple data_frames['method'] are the same then keep the first one
        data_frames = data_frames.drop_duplicates(subset='method')
        
        # groupby method and take the mean of time
        # data_frames = data_frames.groupby('method').mean().reset_index()
        
        cpu_to_df[num_cpu] = data_frames
        
    num_operations = len(cpu_to_df[1])
    logger.info(f"{num_operations = }")
    
    # plot only the required methods in a 2x2 figure
    fig, axs = plt.subplots(2, 2, figsize=(12, 10))
    fig.suptitle('Time Taken For Select DF Operations On Multiple CPUs On The Loan Dataset', fontsize=16, fontweight='bold')
    for i, operation in enumerate(required_methods):
        ax = axs[i // 2, i % 2]
        ax.set_title(operation_to_name.get(operation, operation))
        ax.set_xlabel('Number of CPUs')
        ax.set_ylabel('Time (s)')
        ax.grid(True)
        times = [cpu_to_df[num_cpu][cpu_to_df[num_cpu]['method'] == operation]['time'].values[0] for num_cpu in num_cpus]
        ax.bar(range(len(num_cpus)), times)
        ax.set_xticks(range(len(num_cpus)))
        ax.set_xticklabels(num_cpus)
        
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig('./plots/cpu_plots_selected_methods.png', dpi=300)
