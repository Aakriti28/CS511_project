import pandas as pd, numpy as np
import os, sys, json
import logging
from tqdm import tqdm
logger = logging.getLogger(__name__)
logging.basicConfig(format='[%(levelname)s]: %(message)s', level=logging.INFO)


def modify_dataset(data, columns, sparsity_level:float):
    np.random.seed(0)
    
    modified_data = data.copy()
    for col in columns:
        total_values = len(modified_data[col])
        existing_nulls = modified_data[col].isnull().sum()
        target_nulls = int(total_values * sparsity_level)
        additional_nulls_needed = target_nulls - existing_nulls

        if additional_nulls_needed > 0:
            non_null_indices = modified_data[col].dropna().index
            indices_to_nullify = np.random.choice(non_null_indices, size=additional_nulls_needed, replace=False)
            modified_data.loc[indices_to_nullify, col] = np.nan
        else:
            logger.critical(f"Sparsity level is too low for column {col}, existing sparsity is {existing_nulls/total_values} and target sparsity is {sparsity_level}")

    return modified_data

if __name__ == "__main__":
    dataset_path = '../datasets/'
    # dataset_name = 'loan'
    # column_to_be_modified = ['loan_status', 'addr_state']
    dataset_name = 'athlete'
    column_to_be_modified = ['NOC', 'Medal']
    
    logger.info(f"Reading data")
    # data = pd.read_csv(dataset_path + "loan_data/loan_data.csv")
    data = pd.read_csv(dataset_path + "athlete/athlete_events.csv")
    
    sparsity_levels = [0.1, 0.2, 0.3, 0.4, 0.5]
    
    for sparsity_level in tqdm(sparsity_levels, desc="Creating new datasets"):
        modified_data = modify_dataset(data, column_to_be_modified, sparsity_level)
        # modified_data.to_csv(f"./data/loan_data_{sparsity_level}.csv", index=False)
        modified_data.to_csv(f"./data/athlete_events_{sparsity_level}.csv", index=False)
    
    logger.info("Done!")
