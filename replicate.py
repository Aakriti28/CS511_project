import pandas as pd 
df = pd.read_csv("/shared/data/aa117/bento/datasets/loan_data/loan_data.csv")

df_new = pd.concat([df] * 5, ignore_index=True)

df_new.to_csv("/shared/data/aa117/bento/datasets/loan_data/loan_data_5.csv")