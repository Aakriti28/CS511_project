import numpy as np, matplotlib.pyplot as plt
import pandas as pd, json, os, sys
import glob
import logging
import pprint
logger = logging.getLogger(__name__)
# set format including level
logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.INFO)

if __name__ == '__main__':
    num_cpus = [1,2,4,8]
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
    
    # plot 14 operations
    # each plot has time for that op for 1,2,4,8 cpus
    fig, axs = plt.subplots(2, 7, figsize=(20, 10))
    fig.suptitle('Time Taken for each DF Operation for Multiple CPUs', fontsize=16, fontweight='bold')
    sorted_methods = sorted(cpu_to_df[1]['method'])
    for i, operation in enumerate(sorted_methods):
        axs[i//7, i%7].set_title(operation)
        axs[i//7, i%7].set_xlabel('Number of CPUs')
        axs[i//7, i%7].set_ylabel('Time (s)')
        axs[i//7, i%7].grid(True)
        times = [cpu_to_df[num_cpu][cpu_to_df[num_cpu]['method'] == operation]['time'].values[0] for num_cpu in num_cpus]
        axs[i//7, i%7].bar(num_cpus, times)
        axs[i//7, i%7].set_xticks(num_cpus)
        
    plt.tight_layout()
    plt.savefig('./plots/cpu_plots.png', dpi=300)
