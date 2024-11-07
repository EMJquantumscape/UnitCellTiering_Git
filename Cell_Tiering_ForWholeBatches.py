# %% Import Modules
######## Import Modules
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick  # Importing matplotlib ticker module for formatting
import sys
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)
from itertools import groupby
from lifelines import KaplanMeierFitter
from statsmodels.stats.proportion import proportion_confint
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.figure_factory as ff
import plotly.io as pio
import plotly
pio.templates.default = "plotly_white"
clrs = plotly.colors.DEFAULT_PLOTLY_COLORS
from qsdc.client import Client
import met_client as app
import genealogy
import genealogy_v2
import unit_cell_electrical_yield_and_metrics_with_rel as uceym_rel
import unit_cell_metro_metrics
import cell_tiering_metro
import mass
from qsdc.client import Client
# create the quantumscape data client
qs_client = Client()
# Query data
conn = qs_client.get_mysql_engine()


batches = "UCD004AG|UCD005A[A-C]|UCD006AB|UCD006A[D-E]|UCD006AH|UCD006A[J-M]|UCD011AA" #Available UCD batches for MLD
batches = "UCB002A[U-Y]"
#batches = "UCD005AF|UCD006A[R-S]|UCD011A[B-E]|UCD013AA"
#batches = "UCD006AN"
#batches = "UCB002AX"
batches = "UCD004AF"
 




# %% Query Data
######## Query Data
# =============================================================================
# ========================         QUERY DATA          =======================
# =============================================================================

######## CELL GENEALOGY ########
# produce dataframe that lists all cells in the batches specified above 
df_sample_ids = genealogy.get_unit_cells(batches) # query ID's of unit cells that are in the batches mentioned above

# creates dataframe with the genealogy history of each unit cell
df_genealogy = genealogy.get_genealogy_unitcell(df_sample_ids["US_id"]) # queries the genealogy history of every sell in the batch
df_samples = df_sample_ids.merge(df_genealogy, on="US_id", how="left") # merges the list of unit cells with the geneology dataframe 


######## QUERY ELECTRICAL YIELD AND ELECTRICAL METRICS ########
# get yield and electrical metrics for each unit cells
df_electrical_yield_metrics = uceym_rel.get_electrical_yield_and_metrics(df_samples["US_id"])
df_testdata = df_samples.merge(df_electrical_yield_metrics, on="US_id") #merges this list with the previous data frame
df_testdata = df_testdata.sort_values('US_id')


######## METROLOGY DATA 
agent = app.ImageAgent()
# get anode metrics from hifi scds scans
anode_metrics = unit_cell_metro_metrics.get_anode_tier(
    df_testdata["US_id"].str.slice(stop=13).unique(), agent
)
anode_metrics.rename(columns={"sample": "US_id"}, inplace=True)
# get radiograph metrics from nordson matrix x-ray scans
radiograph_metrics = unit_cell_metro_metrics.get_radiograph_tier(
    df_testdata["US_id"].str.slice(stop=13).unique(), agent
)
radiograph_metrics.rename(columns={"sample": "US_id"}, inplace=True)
# get unit cell mass metrics
mass_metrics = mass.get_mass_data(df_testdata["US_id"])
# unit cell tiering (alpha 2 criteria)
df_testdata = cell_tiering_metro.cell_tiering(
    df_testdata, anode_metrics, radiograph_metrics, mass_metrics
)

# %% Plot Screen UCT Yields
######## Plot Screen UCT Yields
#  =============================================================================
# ========================      SCREEN UCT YIELDS        =======================
# =============================================================================

##Take dataframe for individual cell and group them by batch ("process")
grouping = "process"
data = df_testdata.copy() #make copy


#data = data[~data['US_id'].str.contains("APD224A|APD238AE")] #focus on subset by filtering out

#create new dataframe where cells from the same batches are grouped together
df_cyield = (
    data[[grouping]]
    .groupby(grouping)
    .first()
    .join(
        data[
            [
                grouping,
                "Build Count",
                "Formation Count",
                "C/3 Count",
                "1C Count",
                "Fast Charge Count",
                "4C Count",
                "Final 1C Count",
                "Yield Count",
                # "Anode Count",
                # "Anode Yield Count",
                # "Anode Yield Tier 1 Count",
                # "Ultrasound Count",
                # "Ultrasound Yield Count",
                # "Metrology Count",
                # "Tier 1 Count",
                # "Tier 1+2 Count",
            ]
        ]
        .groupby(grouping)
        .sum(),
        how="right",
    )
    .reset_index()
).set_index(grouping)


df_cyield[
    [
        "Cells Built",
        "Formation Yield",
        "C/3 Yield",
        "1C Yield",
        "Fast Charge Yield",
        "4C Yield",
        "Final 1C Yield",
        "Screen Yield",
    ]
] = 100 * df_cyield[
    [
        "Build Count",
        "Formation Count",
        "C/3 Count",
        "1C Count",
        "Fast Charge Count",
        "4C Count",
        "Final 1C Count",
        "Yield Count",
    ]
].div(
    df_cyield["Build Count"], axis=0
)

# df_cyield["Anode Yield Tier 1+2"] = (
#     df_cyield["Anode Yield Count"] / df_cyield["Anode Count"]
# ) * df_cyield["Screen Yield"]

# df_cyield["Anode Yield Tier 1"] = (
#     df_cyield["Anode Yield Tier 1 Count"] / df_cyield["Anode Count"]
# ) * df_cyield["Screen Yield"]

# df_cyield["Tier 1+2 Yield"] = (
#     df_cyield["Tier 1+2 Count"] / df_cyield["Metrology Count"]
# ) * df_cyield["Screen Yield"]

# df_cyield["Tier 1 Yield"] = (
#     df_cyield["Tier 1 Count"] / df_cyield["Metrology Count"]
# ) * df_cyield["Screen Yield"]


df_cyield = df_cyield.sort_values(grouping)

fig = go.Figure(
    data=[
        go.Bar(
            x=df_cyield.index,
            y=df_cyield["Cells Built"],
            name="Cells Built",
        ),
        go.Bar(
            x=df_cyield.index,
            y=df_cyield["Formation Yield"],
            name="Formation Yield",
        ),
        go.Bar(
            x=df_cyield.index,
            y=df_cyield["1C Yield"],
            name="1C Yield",
        ),
        go.Bar(
            x=df_cyield.index,
            y=df_cyield["Fast Charge Yield"],
            name="2.5C Yield",
        ),
        # go.Bar(
        #     x=df_cyield.index,
        #     y=df_cyield["4C Yield"],
        #     name="4C Yield",
        # ),
        go.Bar(
            x=df_cyield.index,
            y=df_cyield["Final 1C Yield"],
            name="Final 1C Yield",
        ),
        go.Bar(
            x=df_cyield.index,
            y=df_cyield["C/3 Yield"],
            name="C/3 Yield",
        ),
        # go.Bar(
        #     x=df_cyield.index,
        #     y=df_cyield["Anode Yield Tier 1+2"],
        #     name="Anode 1+2 Yield",  # (HiFi, Ultran, Mass, Radiograph)",
        # ),
    #     go.Bar(
    #         x=df_cyield.index,
    #         y=df_cyield["Anode Yield Tier 1"],
    #         name="Anode Yield (Tier 1)",  # (HiFi, Ultran, Mass, Radiograph)",
    #     ),
    ],
)

# bar group mode
fig.update_layout(barmode="group")

fig.data[0].text = [f"N= {n}" for n in df_cyield["Build Count"]]
fig.update_traces(textposition="inside", textfont_size=15)

fig.update_layout(
    xaxis_title=grouping,
    yaxis_title="Screen yield (%)",
    # y axis font size = 14, x axis font size = 11
    font=dict(size=14),
    legend={"title_text": ""},
    yaxis_range=[0, 101],
)


# fig.update_xaxes(
#     categoryorder="array",
#     categoryarray=data.sort_values(["cell_build_date", "batch"])[grouping].unique(),
# )

fig.update_xaxes(
    categoryorder="array",
    categoryarray=data.sort_values(["batch"])[grouping].unique(),
)


# change the bar colors
colors = [
    px.colors.qualitative.Plotly[2],
    px.colors.qualitative.Plotly[5],
    px.colors.qualitative.Plotly[6],
    px.colors.qualitative.Plotly[4],
    # px.colors.qualitative.Plotly[7],
    px.colors.qualitative.Plotly[0],
    px.colors.qualitative.Plotly[3],
    px.colors.qualitative.Plotly[1],
]
for i in range(len(fig.data)):
    fig.data[i].marker.color = colors[i]

fig.show(renderer="browser")

# %% Plot Cell Metrics
######## Plot Cell Metrics
# =============================================================================
# ========================        CELL METRICS          =======================
# =============================================================================

grouping = "process"
color_by = "experiment"

data = df_testdata.copy()


fig = make_subplots(
    1,
    2,
    horizontal_spacing=0.12,
    vertical_spacing=0.1,
    shared_xaxes=True,
)

# create a color dictionary for each color_by category
color = dict(zip(data[color_by].unique(), px.colors.qualitative.Plotly))

# Set a flag to ensure legend items are added only once
legend_added = {key: False for key in data[color_by].unique()}

for label, group in data.groupby(grouping):
    for color_value, group_color in group.groupby(color_by):
        fig.add_trace(
            go.Box(
                x=group_color[grouping],
                y=group_color["AMSDischargeCapactiy_Co3"],
                quartilemethod="linear",
                name=color_value,
                text=group_color["US_id"],
                showlegend=not legend_added[color_value],
                fillcolor=color[color_value],
                line=dict(color="black"),
            ),
            1,
            1,
        )
        legend_added[color_value] = True


fig.update_yaxes(
    title_text="1C Discharge Capacity (mAh/g)",
    range=[190, 205],
    row=1,
    col=1,
)

for label, group in data.groupby(grouping):
    for color_value, group_color in group.groupby(color_by):
        fig.add_trace(
            go.Box(
                x=group_color[grouping],
                y=group_color[
                    "ADDischargeCapactiy_Co3"
                ],  # [group["Final 1C Count"] == 1]
                quartilemethod="linear",
                name=color_value,
                text=group_color["US_id"],
                showlegend=not legend_added[color_value],
                fillcolor=color[color_value],
                line=dict(color="black"),
            ),
            1,
            2,
        )
        legend_added[color_value] = True


fig.update_yaxes(
    title_text="C/3 Discharge Capacity (mAh/cm<sup>2</sup>)",
    range=[5, 7],
    row=1,
    col=2,
)

for i in range(2):
    fig.update_yaxes(
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,
        ticks="outside",
        row=1,
        col=i + 1,
    )
    fig.update_xaxes(
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,
        ticks="outside",
        row=1,
        col=i + 1,
    )

fig.update_layout(
    title_text="",
    # xaxis_title=grouping,
    font=dict(
        size=16,
    ),
)

fig.update_traces(boxpoints="all", jitter=0.1)

fig.update_xaxes(
    categoryorder="array",
    categoryarray=data.sort_values(["cell_build_date", "batch"])[grouping].unique(),
)

fig.show(renderer="browser")


fig = make_subplots(
    1,
    2,
    horizontal_spacing=0.12,
    vertical_spacing=0.1,
    shared_xaxes=True,
)

# plot colors in px.colors.qualitative.Plotly
color = dict(zip(data[color_by].unique(), px.colors.qualitative.Plotly))


# Set a flag to ensure legend items are added only once
legend_added = {key: False for key in data[color_by].unique()}

for label, group in data.groupby(grouping):
    for color_value, group_color in group.groupby(color_by):
        fig.add_trace(
            go.Box(
                x=group_color[grouping],
                y=group_color[
                    "MedDischargeASR_form"
                ],  # [group["Formation Count"] == 1]
                quartilemethod="linear",
                name=color_value,
                text=group_color["US_id"],
                showlegend=not legend_added[color_value],
                fillcolor=color[color_value],
                line=dict(color="black"),
            ),
            1,
            1,
        )
        legend_added[color_value] = True


fig.update_yaxes(
    title_text="Formation Discharge ASR (Ohm cm<sup>2</sup>)",
    range=[20, 30],
    row=1,
    col=1,
)

for label, group in data.groupby(grouping):
    for color_value, group_color in group.groupby(color_by):
        fig.add_trace(
            go.Box(
                x=group_color[grouping],
                y=group_color["MedDischargeASR_1C"],  # [group["Final 1C Count"] == 1]
                quartilemethod="linear",
                name=color_value,
                text=group_color["US_id"],
                showlegend=not legend_added[color_value],
                fillcolor=color[color_value],
                line=dict(color="black"),
            ),
            1,
            2,
        )
        legend_added[color_value] = True


fig.update_yaxes(
    title_text="1C Discharge ASR (Ohm cm<sup>2</sup>)",
    range=[20, 30],
    row=1,
    col=2,
)

for i in range(2):
    fig.update_yaxes(
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,
        ticks="outside",
        row=1,
        col=i + 1,
    )
    fig.update_xaxes(
        showline=True,
        linecolor="black",
        linewidth=1,
        mirror=True,
        ticks="outside",
        row=1,
        col=i + 1,
    )


fig.update_layout(
    # xaxis_title=grouping,
    font=dict(
        size=16,
    ),
    # show legend
    showlegend=True,
)

fig.update_traces(boxpoints="all", jitter=0.1)

fig.update_xaxes(
    categoryorder="array",
    categoryarray=data.sort_values(["batch"])[grouping].unique(),
)  #

fig.show(renderer="browser")


# %% Create dataframe that will feed into the Cell Tiering script
######## Tabulate Yielded Cells
#Filter yielded Cells
data = df_testdata[df_testdata['Yield Count'] == 1]
data = data.rename(columns={"US_id": "Cell ID"})
data = data.rename(columns={"process": "Batch"})

#Generate new dataframe to input into Cell Tiering Function
YieldedCells = data[data['Yield Count'] == 1]
YieldedCells = YieldedCells[["Cell ID","Batch"]]

# List of columns to add
new_columns = [
    "Cell Status", "Pairing Group", "Edge Wetting", "Thickness", "Alignment", 
    "Anode", "DischargeASR_1C", "Capacity_Co3", 
    "dVdt_fastcharge", "total_rank_score", "Tray", "TrayRow", "TrayColumn"
]
# Add each column with NaN values
for col in new_columns:
    YieldedCells[col] = np.nan

#Update if yielded cell is on 2L Reliability
data = data[["Cell ID","Reliability Test Count"]]
# Merge the two dataframes on the 'Cell ID' column
merged_df = pd.merge(YieldedCells, data, on='Cell ID', how='right')
# Update 'Cell Status' where 'Reliability Test Count' is 1 in merged dataframe
merged_df.loc[merged_df['Reliability Test Count'] == 1, 'Cell Status'] = '2L Reliability'
# Drop the added columns to keep only the original columns from YieldedCells
YieldedCells = merged_df.drop(columns=['Reliability Test Count'])

#Update if yielded cell is on already in a multilayer pouch
# Query dataframe from database
gen = genealogy_v2.get_genealogy_2L('APD|ML|UC|QSC', conn) # 
# Map the 'US_id' to '6L_cell_id' using '2L_cell_id' in the 'gen' dataframe
us_to_ps_mapping = dict(zip(gen['2L_cell_id'], gen['6L_cell_id']))
YieldedCells['Cell Status'] = YieldedCells['Cell ID'].map(us_to_ps_mapping)
YieldedCells['Cell Status'] = YieldedCells['Cell Status'].fillna('Waiting')

#YieldedCells.to_clipboard(index=False)

#%% Query Perfomance Metrics and Tier Cells
######## Query Performance Metrics and Tier Cells
#import modules and functions
import pandas as pd
import numpy as np
from qsdc.client import Client
import met_client as app
import os
import unit_cell_metro_metrics_ZI
import unit_cell_electrical_yield_and_metrics_v2 as uceym
import warnings
from datetime import datetime
import rel_sim as rs
import query_tray_samples_V4 as query_tray_samples
from importlib import reload
from met_client import SearchQuery, ImageAgent
from met_client.constants import AnalysisType, ImageSize
from image_client.client import ImageClient
from image_client.manual_review import convert_manual_reviews_to_dataframe


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
            "Cell ID", "Batch", "Cell Status", "Pairing Group", 
            "Edge Wetting", "Thickness", "Alignment", 
            "Anode", "MedDischargeASR_1C", 
            "DischargeCapacity_Co3", "dVdt_delta_fastcharge", 
            "total_rank_score", "tray_id", "row_index", "col_index"
        ]

        # Filter the DataFrame to only include the specified columns
        rank = rank[final_columns]

        # Save the ranked cells to an Excel file
        rank.to_excel(output_name, index=False)
        print(f"Saved ranked {label} cells to {output_name}")

        return rank


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
    #df_pairing['Edge Wetting'] = np.where(df_pairing['median_contour_catholyte_pct'] < 80, 3, 1)
    
    
    ## Query Anode tier for pairing 
    print("Aquiring Anode Data")
    df_anode_metrics = unit_cell_metro_metrics_ZI.get_anode_tier_A1(df_pairing['Cell ID'].str.slice(stop=13).unique(), agent)
    df_pairing['Anode'] = df_pairing['Cell ID'].str.slice(start=0, stop=16).map(df_anode_metrics.set_index('sample')['A1_anode_tier'])
    ## drop this column or else it will be considered in your ranking and script will bug out 


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
                    #print(f"{sample_name} is a 1 based on CV model")
                elif cathodemisalignment.iloc[0] == "no-go":
                    df_pairing.at[index, 'Alignment'] = 3
                    #print(f"{sample_name} is a 3 based on CV model")

            #Continue with manual review of cathode misalignment and edge wetting
            manual_reviews = image_client.get_manual_reviews(samples=[sample_name], include_history=True)
            manualreviewCM = convert_manual_reviews_to_dataframe(manual_reviews, include_modified_date=True)
            if 'cathode_alignment' in manualreviewCM:
                if not manualreviewCM.empty and manualreviewCM['cathode_alignment'].notnull().any():
                    df_pairing.at[index, 'Alignment'] = manualreviewCM['cathode_alignment'].iloc[0]
                    #print(f"Manual Review corrected {sample_name} cathode misalignment to {manualreviewCM['cathode_alignment'].iloc[0]}")

            if 'edge_wetting' in manualreviewCM:
                if not manualreviewCM.empty and manualreviewCM['edge_wetting'].notnull().any():
                    df_pairing.at[index, 'median_contour_catholyte_pct'] = manualreviewCM['edge_wetting'].iloc[0]
                    #print(f"Manual Review corrected {sample_name} edge wetting to {manualreviewCM['edge_wetting'].iloc[0]}")

    conditions = [
        df_pairing['median_contour_catholyte_pct'] < 80,
        (df_pairing['median_contour_catholyte_pct'] >= 80) & (df_pairing['median_contour_catholyte_pct'] <= 98),
        df_pairing['median_contour_catholyte_pct'] > 98
    ]
    choices = [3, 2, 1]
    df_pairing['Edge Wetting'] = np.select(conditions, choices)
    df_pairing = df_pairing.drop(columns = ['median_contour_catholyte_pct'])

    # Remove duplicate rows across all columns
    df_pairing = df_pairing.drop_duplicates()
    # Select columns that we are considering for tiering
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
    
    #df_pairing.to_clipboard(index=False)

    #return ranked_tier_1, ranked_tier_2, ranked_tier_3, ranked
    return ranked

#%% Run Function on Batches
######## Run Function on Batches

df_pairing = YieldedCells
TieredCells = pairing_process(df_pairing)

# %%

df = TieredCells

# Define Layers
Layers = 11  # Number of samples in each group

# Group samples by "Pairing Group" and form groups by "Layers"
grouped_samples = []
group_id = 1  # To uniquely identify each full group

for pairing_group, group_df in df.groupby('Pairing Group'):
    # Sort samples by "MedDischargeASR_1C" within each "Pairing Group"
    sorted_group = group_df.sort_values(by='MedDischargeASR_1C').reset_index(drop=True)
    
    # Divide into groups of size "Layers"
    num_groups = int(np.ceil(len(sorted_group) / Layers))
    
    for i in range(num_groups):
        # Extract the i-th group (slice of the sorted DataFrame)
        group_chunk = sorted_group.iloc[i * Layers: (i + 1) * Layers].copy()
        
        # Check if the chunk has enough samples
        if len(group_chunk) < Layers:
            # Assign "Backfill" ID if it's a partial group
            group_chunk['Group ID'] = f"Tier {pairing_group} Backfill"
        else:
            # Assign a unique group identifier for full groups
            group_chunk['Group ID'] = f"Group_{group_id}"
            group_id += 1
        
        grouped_samples.append(group_chunk)

# Combine all grouped samples into a single DataFrame
GroupedCells = pd.concat(grouped_samples).reset_index(drop=True)
GroupedCells.to_clipboard(index=False)

#dslafkj;lksdjf
# %%
