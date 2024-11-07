# %% 
######### READ FILE CONTAINING MANUAL REVIEW #########
######### This script takes your list of yielded unit cells, queries their performance metrics, and tiers each one based on those metrics #########

#import modules and functions
import pandas as pd
import numpy as np
import genealogy_v2
from qsdc.client import Client
import met_client as app
import os
import unit_cell_metro_metrics_ZI
import unit_cell_electrical_yield_and_metrics_v2 as uceym
import unit_cell_electrical_yield_and_metrics_with_rel as uceym_rel
import warnings
from datetime import datetime
import rel_sim as rs
import query_tray_samples_V4 as query_tray_samples
from importlib import reload
from met_client import SearchQuery, ImageAgent
from met_client.constants import AnalysisType, ImageSize
from image_client.client import ImageClient
from image_client.manual_review import convert_manual_reviews_to_dataframe
# create the quantumscape data client
qs_client = Client()
# Query data
conn = qs_client.get_mysql_engine()

asr_limit = 3 #legacy parameter when we were strategically pairing cells within ASR ranges


#master function that will query cell metrics, tier cells, and produce spreadsheet 
def pairing_process(df_pairing):
    ##takes your unit cells and tiers them based on performance metrics
    print("Running Function")
    def process_group(df_pairing, condition, label):
        # Apply condition to filter the dataframe
        rank = df_pairing[condition]

        # User input
        current_date = datetime.now().strftime('%Y-%m-%d')
        output_name = f'{current_date}_{label}.xlsx'
        sample_col = 'Cell ID'
        ml_features = [
            sample_col,
            'MedDischargeASR_1C',
            'DischargeCapacity_Co3',
            'dVdt_delta_fastcharge',
            'Total',
        ]

        ranking_pis = [
            'min', 
            'max', 
            'min', 
            'min'
        ]
        ranking_weights = [
            0.10, 
            0.00, 
            0.00, 
            0.90
        ]
        rank.to_clipboard(index=False)
        # Drop cells that contain NaNs in ml_features
        rank = rank.dropna(subset=ml_features).copy()
        ranking_params = ml_features.copy()
        ranking_params.remove(sample_col)

        # Rank cells by performance metrics
        for i, feature in enumerate(ranking_params):
            rank[feature + '_rank'] = rank[feature] * ranking_weights[i]
        
        # Calculate total ranking score
        rank['total_rank_score'] = rank[[f + '_rank' for f in ranking_params]].sum(axis=1)

        # Sort by total rank score (ascending or descending based on criteria)
        rank = rank.sort_values(by='total_rank_score', ascending=True)
        
        # Select and reorder the specified columns
        final_columns = [
            "Cell ID", "Cell Status", "Pairing Group", 
            "Edge Wetting", "Thickness", "Alignment", 
            "Anode Tier", "MedDischargeASR_1C", 
            "DischargeCapacity_Co3", "dVdt_delta_fastcharge", 
            "total_rank_score", "tray_id", "row_index", "col_index"
        ]

        # Filter the DataFrame to only include the specified columns
        rank = rank[final_columns]

        # Save the ranked cells to an Excel file
        rank.to_excel(output_name, index=False)
        print(f"Saved ranked {label} cells to {output_name}")

        return rank
    
    #Update if yielded cell is on already in a multilayer pouch
    # Query dataframe from database
    print("Aquiring Disposition")
    gen = genealogy_v2.get_genealogy_2L('APD|ML|UC', conn) # 
    # Map the 'US_id' to '6L_cell_id' using '2L_cell_id' in the 'gen' dataframe
    us_to_ps_mapping = dict(zip(gen['2L_cell_id'], gen['6L_cell_id']))
    df_pairing['Cell Status'] = df_pairing['Cell ID'].map(us_to_ps_mapping)

    #Update if yielded cell is/was on 2L Reliability 
    #Query electrical test metrics (UCT and Reliability)
    df_electrical_yield_metrics = uceym.get_electrical_yield_and_metrics(df_pairing["Cell ID"])
    df_electrical_yield_metrics = df_electrical_yield_metrics.rename(columns={"US_id": "Cell ID"})
    # Merge the "Reliability Test Count" column to df_pairing based on "Cell ID"
    #df_merged = pd.merge(df_pairing, df_electrical_yield_metrics[['Cell ID', 'Reliability Test Count']], on='Cell ID', how='left')
    # Update "Cell Status" to "2L Reliability" where "Reliability Test Count" is 1
    #df_merged.loc[df_merged['Reliability Test Count'] == 1, 'Cell Status'] = '2L Reliability'
    # Drop the extra "Reliability Test Count" column
    #df_pairing = df_merged.drop(columns=['Reliability Test Count'])

    #Cells that are unpaired and haven't been in 2L will show "waiting" status
    df_pairing['Cell Status'] = df_pairing['Cell Status'].fillna('Waiting') 


    ###Pull metrics for each yielded cell
    ## Query HIFI CELL THICKNESS METRICS ##
    print("Aquiring Thickness Data")
    agent = app.ImageAgent()        
    # get US thickness data
    df_us_thickness = unit_cell_metro_metrics_ZI.get_thickness_metrics(
            df_pairing["Cell ID"].str.slice(stop=13).unique(), agent
        )
    # append "_US" to column names
    df_us_thickness.columns = [f"{col}_US" for col in df_us_thickness.columns]
    df_us_thickness = df_us_thickness.rename(columns={"sample_US": "Cell ID"})
    # select desired columns for analysis
    selected_columns_df_us_thickness = df_us_thickness[['Cell ID','10mm_eroded_rect_inside_mean_US','0.5mm_eroded_rect_east_mean_US','0.5mm_eroded_rect_west_mean_US','0.5mm_eroded_rect_north_mean_US','0.5mm_eroded_rect_south_mean_US','center_normalized_0.5mm_eroded_rect_outside_mean_US']]
    # MERGE US DATA
    df_processing = df_pairing.merge(selected_columns_df_us_thickness, how="left", on="Cell ID")
    # Define the conditions
    conditions = [
           df_processing['center_normalized_0.5mm_eroded_rect_outside_mean_US'] < 1.1
        ]
        # Define the corresponding values
    values = [1]
    # Assign values to the 'Thickness' column
    df_pairing['Thickness'] = np.select(conditions, values, default=3)


    ## Query Edge Wetting Metrics ##
    print("Aquiring Edge Wetting Data")
    df_edge_wetting_metrics = unit_cell_metro_metrics_ZI.get_edge_wetting_metrics(df_pairing['Cell ID'].str.slice(stop=13).unique(), agent)
    df_pairing = df_pairing.merge(df_edge_wetting_metrics[['sample','median_contour_catholyte_pct']], how="left", left_on="Cell ID", right_on = 'sample').drop(columns = 'sample')
    df_pairing['Edge Wetting'] = np.where(df_pairing['median_contour_catholyte_pct'] < 80, 3, 1)
    
    
    ## Query Anode tier for pairing 
    print("Aquiring Anode Data")
    df_anode_metrics = unit_cell_metro_metrics_ZI.get_anode_tier_A1(df_pairing['Cell ID'].str.slice(stop=13).unique(), agent)
    df_pairing['Anode Tier'] = df_pairing['Cell ID'].str.slice(start=0, stop=16).map(df_anode_metrics.set_index('sample')['A1_anode_tier'])
    ## drop this column or else it will be considered in your ranking and script will bug out 
    df_pairing = df_pairing.drop(columns = ['median_contour_catholyte_pct'])

    ## Query Cathode Misalignment (Alignment) ##
    with ImageClient(host="image-api.qscape.app") as image_client:
    #image_client = ImageClient()  # Use ImageClient as a context manager
        print("Aquiring Cathode Misalignment Data")
        for index, row in df_pairing.iterrows():
            sample_name = row['Cell ID']
            image_search = SearchQuery(
                sample_prefix=sample_name,    
                a_type=AnalysisType.CONTRAST,      
                lregex="nordson_matrix-us-stitched-corners$",   
                )
            image_agent = ImageAgent()
            image_results = image_agent.search(query=image_search)
            # Check if 'cathode_alignment_custom_model_prediction' exists in the image results
            if 'cathode_alignment_custom_model_prediction' in image_results:
                cathodemisalignment = image_results['cathode_alignment_custom_model_prediction']
                if cathodemisalignment.iloc[0] == "go":
                    df_pairing.at[index, 'Alignment'] = 1
                    print(f"{sample_name} is a 1 based on CV model")
                elif cathodemisalignment.iloc[0] == "no-go":
                    df_pairing.at[index, 'Alignment'] = 3
                    print(f"{sample_name} is a 3 based on CV model")
            #Continue with manual review
            manual_reviews = image_client.get_manual_reviews(samples=[sample_name], include_history=True)
            manualreviewCM = convert_manual_reviews_to_dataframe(manual_reviews, include_modified_date=True)
            if not manualreviewCM.empty and manualreviewCM['cathode_alignment'].notnull().any():
                df_pairing.at[index, 'Alignment'] = manualreviewCM['cathode_alignment'].iloc[0]
                print(f"Manual Review corrected {sample_name} to {manualreviewCM['cathode_alignment'].iloc[0]}")

    # Remove duplicate rows across all columns
    df_pairing = df_pairing.drop_duplicates()

    # Select all columns except 'Cell ID' and 'Cell Status'
    columns_to_consider = df_pairing.columns.difference(['Cell ID', 'Cell Status', 'Pairing Group', 'Requests', 'Cell Comments'])

    # Calculate the minimum value for each row across the selected columns
    df_pairing['Total'] = df_pairing[columns_to_consider].max(axis=1)

    ## Query electrical metrics for pairing ##
    df_electrical_yield_metrics = uceym.get_electrical_yield_and_metrics(df_pairing["Cell ID"])
    df_pairing = pd.merge(df_pairing, df_electrical_yield_metrics, left_on='Cell ID', right_on='US_id', how='left')
    # df_pairing['Capacity_Difference'] = abs(df_pairing['AMSDischargeCapacity_Co3'] - (-0.4805 * df_pairing['MedDischargeASR_1C'] + 207.0932))

    ## Query tray location ##
    sample_names_group = df_pairing["Cell ID"]
    df_tray = query_tray_samples.get_sample_tray(sample_names_group)
    df_pairing = pd.merge(df_pairing, df_tray[['sample_name', 'tray_id', 'row_index', 'col_index', 'modified']], left_on='Cell ID', right_on='sample_name', how='left')

    # Fill NaN values in 'Pairing Group' based on 'Total'
    df_pairing['Pairing Group'] = df_pairing['Pairing Group'].fillna(
        df_pairing['Total'].map({
            1: '1',
            2: '2',
            3: '3',
            # 4: '1b',
            # 5: '1b',
            # 6: '3b',
            # 7: '3b'
        })
    )

    # Process Tier 1 Cells Only
    #ranked_tier_1 = process_group(df_pairing, (df_pairing['Pairing Group'] == '1'), 'Tier 1')
    ## Process Tier 2 Cells Only
    #ranked_tier_2 = process_group(df_pairing, (df_pairing['Pairing Group'] == '2'), 'Tier 2')
    # Process Tier 3 Cells Only
    #ranked_tier_3 = process_group(df_pairing, (df_pairing['Pairing Group'] == '3'), 'Tier 3')
    #Process All Cells
    ranked = process_group(df_pairing, (df_pairing['Pairing Group'].isin(['1', '2', '3'])), 'Tiered')
    
    df_pairing.to_clipboard(index=False)

    #return ranked_tier_1, ranked_tier_2, ranked_tier_3, ranked
    return ranked

#%% Run code on spreadsheet
# Load your input spreadsheet into the dataframe
df_pairing = pd.read_excel('UnitCellTiering/Input Template.xlsx')
# Call the pairing process
pairing_process(df_pairing)
#%%