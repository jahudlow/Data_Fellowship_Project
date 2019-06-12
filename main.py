'''
This program updates the Case Dispatcher Google Sheet with the latest cases, moves closed cases
 to the closed sheets, calculates Strength of Case (SOC) and Solvability scores for each case,
 and sorts cases according to priority.
'''

from time import time
import pandas as pd
import arrest_module as am
import db_connect as dc
import gsheets as gs
import soc_pipe as sp
import entity_groups as eg
import priority_calc as pc
from copy import deepcopy
pd.options.mode.chained_assignment = None

t0 = time()
cif = dc.DB_Conn()
query = str(
    "select * from dataentry_cifnepal as CIF inner join \
    dataentry_personboxnepal as PB on CIF.id = PB.cif_id;")
db_cif = cif.ex_query(query)

soc_df = sp.pre_proc(db_cif)

vics = dc.DB_Conn()
query = str(
    "SELECT * FROM public.dataentry_person as p inner join \
    dataentry_cifnepal as CIF on p.id = CIF.main_pv_id;")
db_vics = vics.ex_query(query)

sus = dc.DB_Conn()
query = str(
    "SELECT * FROM public.dataentry_personboxnepal as pb inner join \
    public.dataentry_person as p on pb.person_id = p.id;")
db_sus = sus.ex_query(query)

add = dc.DB_Conn()
query = str(
    "SELECT * FROM public.dataentry_address1 as ad1 inner join \
    public.dataentry_address2 as ad2 on ad1.id = ad2.address1_id;")
db_add = add.ex_query(query)

cdws = gs.get_gsheets("Case Dispatcher 2.0")

dfs = gs.get_dfs(cdws)

locals().update(dfs)

arrests = am.get_arrests(Arrests)

soc_df = pd.merge(soc_df, arrests, how='outer', on='suspect_id', sort=True,
                  suffixes=('x', 'y'))
soc_df.Arrest = soc_df.Arrest.fillna('0').astype(int)
soc_df = soc_df.dropna(axis=0, subset=['cif_number'])

soc_df = sp.en_features(soc_df)

soc_df = sp.make_new_predictions(soc_df)

new_victims = db_vics
victims = eg.Entity_Group('Victim_ID',
                          new_victims,
                          Victims,
                          Closed_Vic,
                          'victims')
new_suspects = db_sus
suspects = eg.Entity_Group('Suspect_ID',
                           new_suspects,
                           Suspects,
                           Closed_Sus,
                           'suspects')
addr = eg.subset_addresses(db_add)

eg.Entity_Group.merge_addresses(addr)

victims.new = eg.set_vic_id(victims.new)
suspects.new = eg.set_sus_id(suspects.new, db_cif)
eg.Entity_Group.set_case_id()

new_police = deepcopy(suspects.new)
new_police.rename(columns={'Name': 'Suspect_Name'}, inplace=True)
police = eg.Entity_Group('Suspect_ID',
                         new_police,
                         Police,
                         Closed_Pol,
                         'police')

eg.Entity_Group.combine_sheets()

eg.Entity_Group.move_closed(arrests)

eg.Entity_Group.move_other_closed(suspects, police, victims)

vics_willing = pc.get_vics_willing_to_testify(victims.active)
police.active = pc.add_vic_names_to_pol(police.active, vics_willing)

suspects.active = pc.calc_all_sus_scores(suspects.active,
                                         vics_willing,
                                         Parameters,
                                         arrests,
                                         police.active,
                                         db_cif,
                                         soc_df,
                                         Suspects)
victims.active = pc.add_priority_to_others(suspects.active,
                                           victims.active,
                                           'Case_ID',
                                           Victims,
                                           'Victim_ID')
police.active = pc.add_priority_to_others(suspects.active,
                                          police.active,
                                          'Suspect_ID',
                                          Police,
                                          'Suspect_ID')

suspects.active = gs.create_new_rel_sheets(suspects.active, 3)

new_gsheets = eg.Entity_Group.save_csvs()

gs.upload_sheets(new_gsheets)

print("done in %0.3fs" % (time() - t0))
