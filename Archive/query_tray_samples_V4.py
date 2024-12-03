# %%
###### Adjustments made for case senstivity in sample names ########
from qsdc.client import Client
import met_client as app
import pandas as pd

def get_sample_tray(sample_names_group):
    # Convert sample names to lowercase for case-insensitive matching
    sample_names_group = [name.lower() for name in sample_names_group]

    # Extract the first 6 characters and get unique values
    unique_prefixes = list(set([name[:6] for name in sample_names_group]))

    # Create a regular expression pattern from the unique prefixes
    regexp_pattern = "|".join(unique_prefixes)

    # Query to get relevant data from the database
    query = f"""
        SELECT *
        FROM production_2011_beta_1.tray_links
        WHERE LOWER(sample_name) REGEXP '{regexp_pattern}'
    """
    
    qs_client = Client()
    conn = qs_client.get_mysql_engine()

    data = pd.read_sql_query(query, conn)

    # Convert sample_name column to lowercase for case-insensitive matching
    data['sample_name'] = data['sample_name'].str.lower()

    # Filter data to keep rows where 'sample_name' is in the group of sample_names
    filtered_data_group = data[data['sample_name'].isin(sample_names_group)]

    # Remove duplicates in 'sample_name' within the group, keeping the first occurrence
    filtered_data_group = filtered_data_group.drop_duplicates(subset='sample_name', keep='last')

    # Additional filtering based on 'idtray'
    filtered_data_group = filtered_data_group[filtered_data_group["idtray"].str.contains("tray_unit_cell_automation")]

    # Keep only specific columns
    keep_columns = ['barcode_data', 'sample_name', 'row_index', 'col_index', 'modified']
    newdata_group = filtered_data_group.loc[:, keep_columns]

    # Rename 'barcode_data' to 'tray_id'
    newdata_group = newdata_group.rename(columns={'barcode_data': 'tray_id'})

    newdata_group['sample_name'] = newdata_group['sample_name'].str.upper()

    # Convert 'col_index' to integers to remove the decimal point
    newdata_group['col_index'] = newdata_group['col_index'].astype(int)

    return newdata_group

# %%
sample_names_group = sample_array = [
    "APD251EA-US00-14",
    "APD251DY-US00-06",
    "APD251EE-US00-04",
    "APD251EF-US00-34",
    "APD251DU-US00-17",
    "APD251DU-US00-24",
    "APD251EE-US00-05",
    "APD251EF-US00-35",
    "APD251EF-US00-29",
    "APD251EC-US01-03",
    "APD251EA-US00-38",
    "APD251DY-US00-11",
    "APD251EB-US00-19",
    "APD251DU-US00-08",
    "APD251EC-US00-04",
    "APD251ED-US00-30",
    "APD251EE-US00-03",
    "APD251EB-US00-24",
    "APD251EE-US00-16",
    "APD251DU-US00-16"
]
df_tray = get_sample_tray(sample_names_group)
# # # %%
# %%
