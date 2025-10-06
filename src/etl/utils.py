import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.cluster import AgglomerativeClustering
import yaml
import requests
from pathlib import Path  


def cluster_text_values(values: pd.DataFrame, column_val: str):
    model = SentenceTransformer("all-MiniLM-L6-v2")

    unique_values = values[column_val].dropna().unique()
    encoding = model.encode(unique_values)

    clustering = AgglomerativeClustering(n_clusters=None, distance_threshold=1.2)
    labels = clustering.fit_predict(encoding)
                                
    clustered = pd.DataFrame({
    column_val: unique_values,
    "cluster": labels
    })

    cluster_names = {}
    for cluster_id, group in clustered.groupby("cluster"):
        most_frequent_value = values[values[column_val].isin(group[column_val].unique())].value_counts().idxmax()[0]
        cluster_names[cluster_id] = most_frequent_value

    mapping = pd.DataFrame.from_dict(cluster_names, orient='index', columns=['group_name'])
    mapping.index.name = 'group_id'

    group_mapping = mapping.merge(clustered, left_on='group_id', right_on='cluster')[['group_name'] + [column_val]]

    groups_dict = {}
    for group, group_df in group_mapping.groupby("group_name"):
        groups_dict[group] = sorted(group_df[column_val].unique().tolist())

    return groups_dict

def save_yaml(yaml_data: dict, file_path: str, yaml_title: str = None):
    if yaml_title:
        yaml_data = {yaml_title: yaml_data}

    cwd = Path.cwd().parent 
    file_path = str(cwd) + "/" + file_path
    
    with open(file_path, 'w') as file:
        yaml.dump(yaml_data, file)


def get_borough_population_data():
    pop_url = "https://data.cityofnewyork.us/resource/ph5g-sr3v.json"
    resp = requests.get(pop_url)
    data = resp.json()
    borough_populations = pd.DataFrame(data)
    borough_populations = borough_populations[borough_populations.age_group == 'Total']

    borough_populations = borough_populations.rename(columns={'_1':'2000','_2':'2010','_3':'2020','_4':'2030'})

    pop_cols = ['2000','2010','2020','2030']
    df_subset = borough_populations[["borough"] + pop_cols]

    df_long = df_subset.melt(id_vars="borough", value_vars=pop_cols,var_name="year", value_name="population")

    df_long["year"] = df_long["year"].astype(int)
    df_long["population"] = df_long["population"].astype(int)
    df_long['borough'] = df_long['borough'].str.upper().str.strip()
    return df_long



