'''
This program updates the Case Dispatcher Google Sheet with the latest cases, moves closed cases to the closed sheets,
calculates Strength of Case (SOC) and Solvability scores for each case, and sorts cases according to priority.
'''

import numpy as np
from config import config
import arrest_module as am
from closed_date import add_cdate_var
import db_module as dm
import gsheets as gs
import pre_processing as pp
import soc_features as sf
import soc_pipe as sp
import new_case_processing as ncp
import gspread
import json
import pandas as pd
import importlib
from copy import deepcopy

pd.options.mode.chained_assignment = None
import pickle
import psycopg2
from sklearn.pipeline import Pipeline, FeatureUnion, make_pipeline
from oauth2client.client import SignedJwtAssertionCredentials
from sklearn.preprocessing import StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from time import time
from datetime import date
import zipfile
import re

t0 = time()

cif = dm.DBConn()
query = str(
    "select * from dataentry_cifnepal as CIF inner join \
    dataentry_personboxnepal as PB on CIF.id = PB.cif_id;")
db_cif = cif.ex_query(query)


soc_df = pp.pre_proc(db_cif)

vics = dm.DBConn()
query = str(
    "SELECT * FROM public.dataentry_person as p inner join \
    dataentry_cifnepal as CIF on p.id = CIF.main_pv_id;")
db_vics = vics.ex_query(query)

sus = dm.DBConn()
query = str(
    "SELECT * FROM public.dataentry_personboxnepal as pb inner join \
    public.dataentry_person as p on pb.person_id = p.id;")
db_sus = sus.ex_query(query)

add = dm.DBConn()
query = str(
    "SELECT * FROM public.dataentry_address1 as ad1 inner join \
    public.dataentry_address2 as ad2 on ad1.id = ad2.address1_id;")
db_add = add.ex_query(query)

cdws = gs.get_gsheets("Case Dispatcher 2.0")

dfs = gs.get_dfs(cdws)
locals().update(dfs)

gs.save_backups(dfs)

arrests = am.get_arrests(Arrests)

soc_df = pd.merge(soc_df, arrests, how='outer', on='suspect_id', sort=True,
                  suffixes=('x', 'y'))
soc_df.Arrest = soc_df.Arrest.fillna('0').astype(int)
soc_df = soc_df.dropna(axis=0, subset=['cif_number'])

soc_df = sf.en_features(soc_df)

soc_df = sp.make_new_predictions(soc_df) # test this code

cif_dates = db_cif[['cif_number', 'interview_date']]

cif_ids = db_cif[['cif_number', 'person_id', 'pb_number']]

new_victims = db_vics
victims = ncp.Entity_Group('Victim_ID', new_victims, Victims)
new_suspects = db_sus
suspects = .Entity_Group('Suspect_ID', new_suspects, Suspects)
addr = ncp.subset_addresses(db_add)

ncp.Entity_Group.merge_addresses(addr)

victims.new = ncp.set_vic_id(victims.new)
suspects.new = ncp.set_sus_id(suspects.new, db_cif)
ncp.Entity_Group.set_case_id()

new_police = copy.deepcopy(suspects.new)
new_police.rename(columns={'Name': 'Suspect_Name'}, inplace = True)
police = ncp.Entity_Group('Suspect_ID', new_police, Police)

ncp.Entity_Group.combine_sheets()

#Haven't finished modularizing code below this point

prev_closed_sus = suspects[suspects.Suspect_ID.isin(arrests.suspect_id)]
prev_closed_pol = police[police.Suspect_ID.isin(arrests.suspect_id)]
prev_closed_sus['Case_Status'] = "Closed: Already in Legal Cases Sheet"
prev_closed_pol['Case_Status'] = "Closed: Already in Legal Cases Sheet"

closed_suspects = suspects[suspects['Date_Closed'].str.len() > 1]
closed_victims = victims[victims['Date_Closed'].str.len() > 1]
closed_police = police[police['Date_Closed'].str.len() > 1]

closed_cases = [closed_suspects,
                closed_victims,
                closed_police,
                prev_closed_sus,
                prev_closed_pol]

add_cdate_var(closed_cases)

closed_sus = pd.concat([Closed_Sus, closed_suspects, prev_closed_sus], sort=False)
closed_pol = pd.concat([Closed_Pol, closed_police, prev_closed_pol], sort=False)
closed_vic = pd.concat([Closed_Vic, closed_victims], sort=False)

# Remove from Active Sheets
suspects = suspects[~suspects.Suspect_ID.isin(closed_sus.Suspect_ID)]
police = police[~police.Suspect_ID.isin(closed_pol.Suspect_ID)]
victims = victims[~victims.Victim_ID.isin(closed_vic.Victim_ID)]

closed_suspects = suspects[(suspects.Suspect_ID.isin(closed_pol.Suspect_ID)) |
                           (~suspects.Case_ID.isin(victims.Case_ID))]
closed_police = police[(police.Suspect_ID.isin(closed_sus.Suspect_ID)) |
                       (~police.Case_ID.isin(victims.Case_ID))]
closed_victims = victims[(~victims.Case_ID.isin(police.Case_ID)) |
                         (~victims.Case_ID.isin(suspects.Case_ID))]

closed_cases = [closed_suspects,
                closed_victims,
                closed_police]

add_cdate_var(closed_cases)

closed_sus = pd.concat([closed_sus, closed_suspects], sort=False).drop_duplicates(subset='Suspect_ID')
closed_vic = pd.concat([closed_vic, closed_victims], sort=False).drop_duplicates(subset='Victim_ID')
closed_pol = pd.concat([closed_pol, closed_police], sort=False).drop_duplicates(subset='Suspect_ID')
closed_pol.drop(columns=['Victims_Willing_to_Testify'])

suspects = suspects[~suspects.Suspect_ID.isin(closed_sus.Suspect_ID)]
police = police[~police.Suspect_ID.isin(closed_pol.Suspect_ID)]
victims = victims[~victims.Victim_ID.isin(closed_vic.Victim_ID)]

json_key = json.load(open('creds.json'))
scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']
credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope)

victims['willing_to_testify'] = victims.Name[victims.Case_Status.str.contains("Step Complete", na=False)]
vics_willing = victims[['Case_ID', 'willing_to_testify']]
vics_willing = vics_willing.dropna(axis=0, subset=['willing_to_testify'])
vics_willing['count'] = 1


def sum_and_join(x):
    return pd.Series(dict(count=x['count'].sum(),
                          willing_to_testify=', '.join(x.astype(str)['willing_to_testify'])))


if len(vics_willing) > 0:
    vics_Willing = vics_willing.groupby('Case_ID').apply(sum_and_join)

police = pd.merge(police, vics_willing, how='left', on='Case_ID')

police['victims_willing_to_testify'] = police['willing_to_testify']
police.drop(columns=['willing_to_testify', 'count'], inplace=True)

suspects = pd.merge(suspects, vics_willing, how='left', on='Case_ID')
v_multiplier = pd.DataFrame(Parameters.iloc[:10, 6:8])
v_multiplier.Victims_Willing_to_Testify = v_multiplier.Victims_Willing_to_Testify.astype(int)

suspects['count'] = suspects['count'].fillna(0).astype(int)
suspects = pd.merge(suspects, v_multiplier, how='left', left_on='count', right_on='Victims_Willing_to_Testify')
suspects.drop(columns=['Victims_Willing_to_Testify',
                       'willing_to_testify',
                       'count'], inplace=True)
suspects['V_Multiplier'].fillna(0, inplace=True)
suspects['V_Multiplier'] = suspects['V_Multiplier'].astype('float')
suspects['Bio_Known'] = np.where(suspects['Bio_and_Location'].eq(''), 0, 1)
suspects = pd.merge(suspects, arrests[['Case_ID', 'Total_Arrests']], how='left', on='Case_ID')
suspects['Total_Arrests'] = suspects['Total_Arrests'].fillna(0).astype(int)
suspects.rename(columns={'Total_Arrests': 'Others_Arrested'}, inplace=True)
police['Willing_to_Arrest'] = np.where(police.Case_Status.str.contains("Step Complete", na=False), 1, 0)
suspects = pd.merge(suspects, police[['Case_ID', 'Willing_to_Arrest']], how='left', on='Case_ID')

today = date.today()
today.strftime("%m/%d/%Y")
cif_dates['Days_Old'] = (today - cif_dates.loc[:, 'interview_date']) / np.timedelta64(1, 'D')

cif_dates['Case_ID'] = cif_dates['cif_number'].str[:-1].replace('.', '')

suspects = pd.merge(suspects, cif_dates[['Case_ID', 'Days_Old']], how='left', on='Case_ID')
suspects['Recency_Score'] = np.where(suspects['Days_Old'] < 100, 1 - suspects.Days_Old * .01, 0)
suspects = suspects.drop_duplicates(subset='Suspect_ID')

# Get 'Strength of Case' results of CD module
suspects = pd.merge(suspects, soc_df[['suspect_id', 'soc']], how='left', left_on='Suspect_ID', right_on='suspect_id')
suspects['Strength_of_Case'] = suspects['soc'].round(decimals=3)
suspects['Strength_of_Case']

suspects['Em2'] = suspects['Eminence'].fillna(1)
suspects.loc[suspects['Eminence'].str.len() < 1, 'Em2'] = 1
suspects['Em2'] = suspects['Em2'].astype(int)

weights_Vs = pd.Series(
    Parameters.iloc[0:7, 1]).replace('', 0).append(
    pd.Series(
        Parameters.iloc[0:3, 5])).astype(float)

weights_Keys = pd.Series(
    Parameters.iloc[0:7, 0]).append(
    pd.Series(
        Parameters.iloc[0:3, 4]))

weights = {k: v for k, v in zip(weights_Keys, weights_Vs)}

suspects['Solvability'] = (
                                  suspects['V_Multiplier'].apply(lambda x: x * weights['Victim Willing to Testify']) + \
                                  suspects['Bio_Known'].apply(lambda x: x * weights['Bio and Location of Suspect']) + \
                                  suspects['Others_Arrested'].apply(
                                      lambda x: x * weights['Other Suspect(s) Arrested']) + \
                                  suspects['Willing_to_Arrest'].apply(
                                      lambda x: x * weights['Police Willing to Arrest']) + \
                                  suspects['Recency_Score'].apply(lambda x: x * weights['Recency of Case'])
                          ) / sum(weights.values())

suspects['Priority'] = (
        suspects['Solvability'].apply(lambda x: x * weights['Solvability']) + \
        suspects['Strength_of_Case'].apply(lambda x: x * weights['Strength of Case']) + \
        suspects['Em2'].apply(lambda x: x * 0.1 * weights['Eminence'])
).round(decimals=3)
suspects['Priority'] = suspects['Priority'].fillna(0)

suspects['Priority'].astype(float)
suspects.sort_values('Priority', ascending=False, inplace=True)
suspects = suspects.iloc[:, 0:len(Suspects.columns)].fillna('')
suspects = suspects.drop_duplicates(subset='Suspect_ID')

police = pd.merge(police, suspects[['Suspect_ID', 'Priority']])
police['Priority'].astype(float)
police.sort_values('Priority', ascending=False, inplace=True)
police = police.iloc[:, 0:len(Police.columns)].fillna('')
police = police.drop_duplicates(subset='Suspect_ID')

victims = pd.merge(victims, suspects[['Case_ID', 'Priority']])
victims.sort_values('Priority', ascending=False, inplace=True)
victims = victims.iloc[:, 0:len(Victims.columns)].fillna('')
victims = victims.drop_duplicates(subset='Victim_ID')

suspects.name = 'suspects.csv'
police.name = 'police.csv'
victims.name = 'victims.csv'
closed_sus.name = 'closed_sus.csv'
closed_pol.name = 'closed_pol.csv'
closed_vic.name = 'closed_vic.csv'

all_sheets = [suspects,
              police,
              victims,
              closed_sus,
              closed_pol,
              closed_vic]

for sheet in all_sheets:
    sheet.to_csv(sheet.name, index=False, header=None)

up_sheets = []

for sheet in all_sheets:
    up_sheet = open(sheet.name, 'r').read()
    up_sheets.append(up_sheet)

file = gspread.authorize(credentials)

ss = file.open("Suspects")
ps = file.open("Police")
vs = file.open("Victims")
css = file.open("Closed_Sus")
cps = file.open("Closed_Pol")
cvs = file.open("Closed_Vic")

gs = [ss,
      ps,
      vs,
      css,
      cps,
      cvs]

sheet_dict = {k: v for k, v in zip(gs, up_sheets)}


def upload_sheets(dict):
    """Uploads csv files to Google Sheets from a dictionary where keys are Google Sheets and values are csvs."""
    file = gspread.authorize(credentials)
    last = len(list(dict))
    for i in range(0, last):
        file.import_csv(
            list(
                dict.keys())[i].id,
            list(
                dict.values())[i].encode('utf-8'))


upload_sheets(sheet_dict)

print("done in %0.3fs" % (time() - t0))
