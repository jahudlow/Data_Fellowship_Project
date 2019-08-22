"""
This program updates the Case Dispatcher Google Sheet with the latest cases, moves closed cases
 to the closed sheets, calculates Strength of Case (SOC) and Solvability scores for each case,
 and sorts cases according to priority.
"""

import pandas as pd
import arrest_module as am
import db_connect as dc
import gsheets as gs
import net_db.network_db as ndb
import soc_pipe as sp
import entity_groups as eg
import priority_calc as pc
import argparse
from copy import deepcopy
import schedule
import time
pd.options.mode.chained_assignment = None


def main(db_cred='database.ini', gs_cred='creds.json', gs_name='Case Dispatcher 2.0'):
    """Update Case Dispatcher Google Sheet """
    dbc = dc.DB_Conn(db_cred)

    db_cif = dbc.ex_query("SELECT * FROM dataentry_cifnepal as CIF inner join \
    dataentry_personboxnepal as PB on CIF.id = PB.cif_id;")

    soc_df = sp.pre_proc(db_cif)

    db_vics = dbc.ex_query("SELECT * FROM public.dataentry_person as p inner join \
    dataentry_cifnepal as CIF on p.id = CIF.main_pv_id;")

    db_sus = dbc.ex_query("SELECT * FROM public.dataentry_personboxnepal as pb inner join \
    public.dataentry_person as p on pb.person_id = p.id;")

    db_add = dbc.ex_query("SELECT * FROM public.dataentry_address1 as ad1 inner join \
    public.dataentry_address2 as ad2 on ad1.id = ad2.address1_id;")

    dbc.close_conn()

    credentials = gs.get_gs_cred(gs_cred)
    auth = gs.get_auth(credentials)
    cdws = gs.get_gsheets(gs_name, auth)
    dfs = gs.get_dfs(cdws)
    gs.save_backups(dfs)
    #locals().update(dfs)

    arrests = am.get_arrests(dfs['Arrests']) #'Arrests' is Google Sheet with latest arrest data

    soc_df = pd.merge(soc_df, arrests, how='outer', on='suspect_id', sort=True,
                      suffixes=('x', 'y'))
    soc_df.Arrest = soc_df.Arrest.fillna('0').astype(int)
    soc_df = soc_df.dropna(axis=0, subset=['cif_number'])

    soc_df = sp.en_features(soc_df)

    #Do Grid Search CV
    #sub_df = sp.remove_recent(soc_df, 90)
    #X_train, X_validation, y_train, y_validation = sp.train_test_val_split(sub_df)
    #cls_pipeline = sp.get_cls_pipe()
    #best_model = sp.do_gridsearch(cls_pipeline, X_train, y_train)
    #sp.save_results(best_model, X_validation, 'soc_model.sav')

    soc_df = sp.make_new_predictions(soc_df, 'soc_model.sav')

    new_victims = db_vics
    victims = eg.Entity_Group('Victim_ID',
                              new_victims,
                              dfs['Victims'],
                              dfs['Closed_Vic'],
                              'victims')
    new_suspects = db_sus
    suspects = eg.Entity_Group('Suspect_ID',
                               new_suspects,
                               dfs['Suspects'],
                               dfs['Closed_Sus'],
                               'suspects')
    addr = eg.subset_addresses(db_add)

    eg.Entity_Group.merge_addresses(addr)

    victims.new = eg.set_vic_id(victims.new)
    suspects.new = eg.set_sus_id(suspects.new, db_cif)
    eg.Entity_Group.set_case_id()

    new_police = deepcopy(x=suspects.new)
    new_police.rename(columns={'Name': 'Suspect_Name'}, inplace=True)
    police = eg.Entity_Group('Suspect_ID',
                             new_police,
                             dfs['Police'],
                             dfs['Closed_Pol'],
                             'police')
    eg.Entity_Group.combine_sheets()

    eg.Entity_Group.move_closed(arrests)

    eg.Entity_Group.move_other_closed(suspects, police, victims)

    vics_willing = pc.get_vics_willing_to_testify(victims.active)
    police.active = pc.add_vic_names_to_pol(police.active, vics_willing)

    suspects.active = pc.calc_all_sus_scores(suspects.active,
                                             vics_willing,
                                             dfs['Parameters'],
                                             arrests,
                                             police.active,
                                             db_cif,
                                             soc_df,
                                             dfs['Suspects'])
    victims.active = pc.add_priority_to_others(suspects.active,
                                               victims.active,
                                               'Case_ID',
                                               dfs['Victims'],
                                               'Victim_ID')
    police.active = pc.add_priority_to_others(suspects.active,
                                              police.active,
                                              'Suspect_ID',
                                              dfs['Police'],
                                              'Suspect_ID')

    suspects.active = gs.new_relationship_gsheets(suspects.active, 3, credentials)

    new_links_dict = gs.get_sheets_for_network_db(suspects, auth)
    ndb.add_entries_dict(new_links_dict)
    ndb.update_links()

    new_gsheets = eg.Entity_Group.save_csvs()

    gs.upload_sheets(new_gsheets, auth)

    gs.upload_stats_sheet(auth)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Update Case Dispatcher')
    parser.add_argument('--cred_file', dest='db_cred', default='database.ini',
                        help="File containing credentials for 'Searchlight' postgresql database")
    parser.add_argument('--gs_cred_file', dest='gs_cred', default='creds.json',
                        help="File containing credentials for Google Drive/Sheets")
    parser.add_argument('--name_of_sheet', dest='gs_name', default='Case Dispatcher 2.0',
                        help="Name of Google Sheet functioning as Case Dispatcher interface")
    args = parser.parse_args()

    schedule.every().day.at("12:00").do(main,
                                        db_cred=args.db_cred,
                                        gs_cred=args.gs_cred,
                                        gs_name=args.gs_name)

    while True:
        schedule.run_pending()
        time.sleep(1)
