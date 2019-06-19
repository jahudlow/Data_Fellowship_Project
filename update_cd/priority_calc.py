import pandas as pd
import numpy as np
from datetime import date

def sum_and_join(x):
    '''Aggregate count of victims willing to testify by Case ID.'''
    return pd.Series(dict(count=x['count'].sum(),
                          willing_to_testify=', '.join(x.astype(str)['willing_to_testify'])))

def get_vics_willing_to_testify(victims):
    '''Get subset of victims who have indicated they're willing to testify against traffickers.'''
    victims['willing_to_testify'] = victims.Name[
        victims.Case_Status.str.contains("Step Complete", na=False)]
    vics_willing = victims[['Case_ID', 'willing_to_testify']]
    vics_willing = vics_willing.dropna(axis=0, subset=['willing_to_testify'])
    vics_willing['count'] = 1
    if len(vics_willing) > 0:
        vics_willing = vics_willing.groupby('Case_ID').apply(sum_and_join)
    return vics_willing

def add_vic_names_to_pol(pol, vics_willing):
    '''Add comma separated list of victims willing to testify to active police sheet.'''
    pol = pd.merge(pol, vics_willing, how='left', on='Case_ID')
    pol['Victims_Willing_to_Testify'] = pol['willing_to_testify'].fillna('')
    pol.drop(columns=['willing_to_testify', 'count'], inplace=True)
    return pol

def calc_vics_willing_scores(sus, vics_willing, Parameters):
    '''Calculate scores for number of victims willing to testify and add them to suspect sheet.'''
    sus = pd.merge(sus, vics_willing, how='left',on='Case_ID')
    v_multiplier = pd.DataFrame(Parameters.iloc[:10, 6:8])
    v_multiplier.Victims_Willing_to_Testify = v_multiplier.Victims_Willing_to_Testify.astype(int)
    sus['count'] = sus['count'].fillna(0).astype(int)
    sus = pd.merge(sus,
                   v_multiplier,
                   how='left', left_on='count', right_on='Victims_Willing_to_Testify')
    sus.drop(columns=['Victims_Willing_to_Testify',
                      'willing_to_testify',
                      'count'], inplace=True)
    sus['V_Multiplier'].fillna(0, inplace=True)
    sus['V_Multiplier'] = sus['V_Multiplier'].astype('float')
    return sus

def calc_arrest_scores(sus, arrests, pol):
    '''Calculate scores for the number of other suspects arrested in each case and create fields \
    for 'bio known' and for police willing to arrest.'''
    sus['Bio_Known'] = np.where(sus['Bio_and_Location'].eq(''), 0, 1)
    sus = pd.merge(sus, arrests[['Case_ID', 'Total_Arrests']], how='left', on='Case_ID')
    sus['Total_Arrests'] = sus['Total_Arrests'].fillna(0).astype(int)
    sus.rename(columns={'Total_Arrests': 'Others_Arrested'}, inplace=True)
    pol['Willing_to_Arrest'] = np.where(
        pol.Case_Status.str.contains("Step Complete", na=False), 1, 0)
    sus = pd.merge(sus, pol[['Case_ID', 'Willing_to_Arrest']], how='left', on='Case_ID')
    return sus

def calc_recency_scores(sus, db_cif):
    '''Assign score to each case that is higher the more recent it is.'''
    today = date.today()
    today.strftime("%m/%d/%Y")
    cif_dates = db_cif[['cif_number', 'interview_date']]
    cif_dates['Days_Old'] = (today - cif_dates.loc[:, 'interview_date']) / np.timedelta64(1, 'D')
    cif_dates['Case_ID'] = cif_dates['cif_number'].str[:-1].replace('.', '')
    sus = pd.merge(sus, cif_dates[['Case_ID', 'Days_Old']], how='left', on='Case_ID')
    sus['Recency_Score'] = np.where(sus['Days_Old'] < 100, 1 - sus.Days_Old * .01, 0)
    sus = sus.drop_duplicates(subset='Suspect_ID')
    return sus

def get_new_soc_score(sus, soc_df):
    '''Merge newly calculated Strength of Case scores to suspects sheet.'''
    sus = pd.merge(sus,
                   soc_df[['suspect_id', 'soc']],
                   how='left', left_on='Suspect_ID', right_on='suspect_id')
    sus['Strength_of_Case'] = sus['soc'].round(decimals=3)
    return sus

def get_eminence_score(sus):
    '''Get eminence score from active sheet, if blank enter '1'.'''
    sus['Em2'] = sus['Eminence'].fillna(1)
    sus.loc[sus['Eminence'].str.len() < 1, 'Em2'] = 1
    sus['Em2'] = sus['Em2'].astype(int)
    return sus

def calculate_weights(Parameters):
    '''Get current weights from Parameters Google Sheet.'''
    weights_vs = pd.Series(Parameters.iloc[0:7, 1]).replace('', 0).append(
        pd.Series(Parameters.iloc[0:3, 5])).astype(float)
    weights_keys = pd.Series(Parameters.iloc[0:7, 0]).append(
        pd.Series(Parameters.iloc[0:3, 4]))
    weights = {k: v for k, v in zip(weights_keys, weights_vs)}
    return weights

def calc_solvability(sus, weights):
    '''Calculate weighted solvability score on active suspects.'''
    sus['Solvability'] = (
        sus['V_Multiplier'].apply(lambda x: x * weights['Victim Willing to Testify']) + \
        sus['Bio_Known'].apply(lambda x: x * weights['Bio and Location of Suspect']) + \
        sus['Others_Arrested'].apply(lambda x: x * weights['Other Suspect(s) Arrested']) + \
        sus['Willing_to_Arrest'].apply(lambda x: x * weights['Police Willing to Arrest']) + \
        sus['Recency_Score'].apply(lambda x: x * weights['Recency of Case'])
        ) / sum(weights.values())
    return sus

def calc_priority(sus, weights, Suspects):
    '''Calculate weighted priority score on active suspects.'''
    sus['Priority'] = (
        sus['Solvability'].apply(lambda x: x * weights['Solvability']) + \
        sus['Strength_of_Case'].apply(lambda x: x * weights['Strength of Case']) + \
        sus['Em2'].apply(lambda x: x * 0.1 * weights['Eminence'])
    ).round(decimals=3)
    sus['Priority'] = sus['Priority'].fillna(0)
    sus['Priority'].astype(float)
    sus.sort_values('Priority', ascending=False, inplace=True)
    sus = sus.iloc[:, 0:len(Suspects.columns)].fillna('')
    sus = sus.drop_duplicates(subset='Suspect_ID')
    return sus

def calc_all_sus_scores(sus, vics_willing, Parameters, arrests, pol, db_cif, soc_df, Suspects):
    '''Complete all suspect sheet calculations in priority_calc module.'''
    sus = calc_vics_willing_scores(sus, vics_willing, Parameters)
    sus = calc_arrest_scores(sus, arrests, pol)
    sus = calc_recency_scores(sus, db_cif)
    sus = get_new_soc_score(sus, soc_df)
    sus = get_eminence_score(sus)
    weights = calculate_weights(Parameters)
    sus = calc_solvability(sus, weights)
    sus = calc_priority(sus, weights, Suspects)
    return sus

def add_priority_to_others(sus, other_entity_group, id_type, entity_gsheet, uid):
    '''Copy priority score from suspects to other active sheets and sort them by priority.'''
    other_entity_group = pd.merge(other_entity_group, sus[[id_type, 'Priority']])
    other_entity_group['Priority'].astype(float)
    other_entity_group.drop_duplicates(subset=uid, inplace=True)
    other_entity_group.sort_values('Priority', ascending=False, inplace=True)
    other_entity_group = other_entity_group.iloc[:, 0:len(entity_gsheet.columns)].fillna('')
    return other_entity_group
