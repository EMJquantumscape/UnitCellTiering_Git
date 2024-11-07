import pandas as pd
import numpy as np

from qsdc.client import Client
from et.cloud import Cloud

qs_client = Client(et_host="et.qscape.app")
mysql_engine = qs_client.get_mysql_engine()
def get_CRV(
    sample_regexp_list=['NPI003%%-US%%','PLT000%%-US%%'],
    recipe_list = [14346,14450]
):
    qs_client = Client(et_host="et.qscape.app")
    mysql_engine = qs_client.get_mysql_engine()
    pd.set_option('display.max_columns', None)
    recipes = ",".join([str(x) for x in recipe_list])
    recipes = '('+recipes+')'
    
    # Get electrical data
    sample_regexp = []
    for i in sample_regexp_list:
        sample_regexp.append('device_structure.displayname LIKE \''+i+'\'')
    sample_regexp = ' OR '.join(sample_regexp)
    
    q = '''
    SELECT
    device_structure.displayname AS 'UCT_id',
    process_flow.displayname as 'US_process_flow',
    test_run_E12_cycle.VoltagePostCeilingRestEndDVdt * 1E6 AS 'dvdt',
    test_run_E12_cycle.CapacityChargeActiveMassSpecific AS 'AMSChCapacity',
    test_run_E12_cycle.CapacityDischargeActiveMassSpecific AS 'AMSDcCapacity',
    test_run_E12_cycle.CapacityDischarge AS 'DischargeCapacity',
    test_run_E12_cycle.CapacityCharge AS 'ChargeCapacity',
    test_run_E12_cycle.CoulombicEfficiency AS 'CE',
    test_run_E12_cycle.AsrDcChargeMedian AS 'MedChASR',
    test_run_E12_cycle.AsrEndCeilingRest AS 'CycleChASR',
    test_run_E12_cycle.AsrDcDischargeMedian AS 'MedDcASR',
    test_run_E12_cycle.CapacityChargeFraction,
    test_run_E12_cycle.CapacityDischargeFraction,
    (test_run_E12_cycle.ASRdcChargeMedian/test_run_E12_cycle.ASRDcDischargeMedian) AS 'ASR_ratio',
    test_run_E12_cycle.VoltageEndCeilingRest AS 'CeilingRestVoltage',
    test_run_E12_cycle.TimeCeilingHold AS 'CeilingHoldTime',
    test_run_E12_cycle.`index` AS 'CycleIndex',
    test_run.`Index` AS 'RunIndex',
    test_run_E12_cycle.`MiscTestAnomaly`,
    test_run.idtest_recipe,
    test_request.Name as test_request_name,
    test_run_E12_cycle.datetime_start AS 'TestCycleStart_datetime',
    test_run_E12_cycle.datetime_end AS 'TestCycleEnd_datetime',
    test_run_E12_cycle.IsChargeComplete AS 'CompletedCharge',
    tool.displayname AS 'ElectricalTestTool',
    test_run.Channel AS 'ElectricalTestChannel'
    -- device_structure.results as US_results
    FROM test_run_E12_cycle
    INNER JOIN test_run_E12
        ON test_run_E12_cycle.idtest_run_E12 = test_run_E12.idtest_run_E12
    INNER JOIN test_run
        ON test_run_E12.idtest_run = test_run.idtest_run
    INNER JOIN test_setup_E12
        ON test_run_E12.idtest_setup_E12 = test_setup_E12.idtest_setup_E12
    INNER JOIN test_request
        ON test_run.idtest_request = test_request.idtest_request
    INNER JOIN device_structure
        ON test_run.iddevice = device_structure.iddevice
    INNER JOIN process
        ON device_structure.idprocess_createdby = process.idprocess
    INNER JOIN process_flow
        ON process.idprocess_flow = process_flow.idprocess_flow
    INNER JOIN tool 
        ON test_run.idtool=tool.idtool
    WHERE ''' + sample_regexp + '''
    AND test_run.idtest_recipe IN ''' + recipes
    
    df_cycle = pd.read_sql(q, mysql_engine)
#     uct_cycle = qs_client.data_hub.get_dataset('MFG-60L-UNIT-CELL-TEST-CYCLE')
    electrical_metrics = df_cycle.set_index('UCT_id')
    electrical_metrics = electrical_metrics.join(electrical_metrics[
        (
            (electrical_metrics.idtest_recipe.isin([14346]))
            & (electrical_metrics.CycleIndex==5)
        )
        | (
            (electrical_metrics.idtest_recipe.isin([14450]))
            & (electrical_metrics.CycleIndex==3)
        )

    ].groupby(['UCT_id'])['CeilingRestVoltage'].nth(0), rsuffix='_final_FC')

    electrical_metrics = electrical_metrics.reset_index().drop_duplicates()
    electrical_metrics = electrical_metrics[[
        'UCT_id',
        'CeilingRestVoltage_final_FC',
    ]].copy().drop_duplicates()
    return electrical_metrics

def within_range(asr: list,asr_range=2):
    return max(asr)-min(asr)<=asr_range

def matchmaking(
    df_in,
    n_cell,
    ranking_params,
    ranking_pis,
    ranking_weights,
    limiting_params, # if more than one limiting param is introduced, modified the logic for 
    limiting_range,
    standardized,
    sample_col = 'UCT_id',
    random = False,
    
):
    df = df_in.copy()
    limit_columns = [str(i)+'_limiting' for i in limiting_params]
    df[limit_columns[0]] = df[limiting_params[0]]
    if standardized:
        for i in ranking_params:
            df[i] = (df[i]-df[i].min(axis=0)) / (df[i].max(axis=0)-df[i].min(axis=0)) # normalize the population between 0 and 1
    df[ranking_params] = df[ranking_params].mul(ranking_weights, axis=1)#.sum(axis=1)

    # Determine the positive ideal solution and negative ideal solution
    pis = {}
    nis = {}
    for i in range(len(ranking_params)):
        if ranking_pis[i]=='max':
            pis[ranking_params[i]] = df[ranking_params[i]].max()
            nis[ranking_params[i]] = df[ranking_params[i]].min()
        else:
            pis[ranking_params[i]] = df[ranking_params[i]].min()
            nis[ranking_params[i]] = df[ranking_params[i]].max()
    pis = pd.Series(pis)
    nis = pd.Series(nis)

    # Calculate the distance to the positive ideal solution and negative ideal solution
    df["D+"] = ((df - pis) ** 2).sum(axis=1) ** 0.5
    df["D-"] = ((df - nis) ** 2).sum(axis=1) ** 0.5

    # Calculate the relative closeness to the ideal solution
    df["C*"] = df["D-"] / (df["D+"] + df["D-"])

    # Rank the samples
    df["overall_ranking"] = df["C*"].rank(ascending=False)  # Higher C* is better
    
    # Randomized ranking as a reference point
    if random:
        df['overall_ranking'] = np.random.uniform(0,1, size=len(df))
    # Highest overall ranking is the best cell
    df = df.sort_values(by='overall_ranking',ascending=True).reset_index(drop=True)

    res = pd.DataFrame()
    unmatched = pd.DataFrame()
    group_counter = 1
    ## Stategy: ranked_df is a dynamic copy of the cells with ASR and sorted by overall ranking, group_df contains cells to be paired with the first cell in ranked_df
    ## Going down the list, as long as the cell passed the ASR accepted range, move it to group_df
    ## If not, discard it from ranked_df

    ranked_df = df[[sample_col,limit_columns[0],'C*','overall_ranking']].copy()
    
    # Stop when not enough cells in ranked_df
    while len(ranked_df)>(n_cell-1):
        group_df = ranked_df.head(1).copy()
        for i in range(n_cell-1):
            
            # Check ASR range between each cell in ranked_df and all the cells in group_df
            ranked_df['Accepted_'+str(i+1)] = ranked_df.apply(lambda x: within_range([x[limit_columns[0]]]+group_df[limit_columns[0]].to_list(),limiting_range[0]), axis=1)
            sub = ranked_df[
                (~ranked_df[sample_col].isin(group_df[sample_col]))
                & (ranked_df['Accepted_'+str(i+1)]==True)
            ]

            # If no cell is within the ASR range
            if len(sub)==0:
                unmatched = pd.concat([unmatched,ranked_df.head(1)],axis=0)
                break
            else:
                group_df = pd.concat([group_df,sub.head(1)],axis=0,ignore_index=True)
        if len(group_df)==n_cell:
            group_df[limit_columns[0]+'_range'] = group_df[limit_columns[0]].max(axis=0)-group_df[limit_columns[0]].min(axis=0)
            group_df['group_C*'] = [group_df['C*'].astype(str).to_list()]*len(group_df)
            group_df['group_C*'] = group_df['group_C*'].apply(lambda x: '|'.join(x))
            group_df['group'] = group_counter
            group_counter += 1
            ranked_df = ranked_df[~ranked_df[sample_col].isin(group_df[sample_col])]
            ranked_df = ranked_df.reset_index(drop=True)
            res = pd.concat([res,group_df],axis=0)
        else:
            ranked_df = ranked_df.tail(len(ranked_df)-1)
    res = res.reset_index(drop=True)
    unmatched = pd.concat([unmatched,ranked_df],axis=0).reset_index(drop=True)
    return res,unmatched,df,pis

def ml_rel_simulation(res,df_full,ranking_weights):
    res = res.merge(df_full[['UCT_id','Failure Mode','ReliabilityCycles','Event','metro_yield_cat1','metro_yield_cat2']],on='UCT_id',how='left')
    res['group_ReliabilityCycles'] = res.groupby(['group'])['ReliabilityCycles'].transform("min")
    res['group_Event'] = res[res['ReliabilityCycles']==res['group_ReliabilityCycles']].groupby(['group','group_ReliabilityCycles'])['Event'].transform("min")
    res['group_failure_mode'] = res[
        (res['ReliabilityCycles']==res['group_ReliabilityCycles'])
        & (res['Event']==res['group_Event'])
    ].groupby(['group','group_ReliabilityCycles','group_Event'])['Failure Mode'].transform(list)
    ml_res = res[
    (~res['group_Event'].isna())
    ][[
        'group',
        'group_C*',
        'group_ReliabilityCycles',
        'group_Event',
        'group_failure_mode'
    ]]
    ml_res = ml_res.drop_duplicates().reset_index(drop=True)
    ml_res['ranking_weights'] = [ranking_weights]*len(ml_res)
    return ml_res,res