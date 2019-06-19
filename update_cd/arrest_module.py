'''
This module extracts and processes the latest arrest related data for the Case Dispatcher.
'''


import pandas as pd

def get_arrests(a_df):
    '''Extracts relevant arrest data and reformats them.'''
    arrests = pd.DataFrame(a_df)
    arrests.infer_objects()
    arrests['Outcome (Arrest)'] = arrests['Outcome (Arrest)'].fillna(0).astype(int)
    arrests = arrests.loc[arrests['Outcome (Arrest)'] == 1]

    pbs = ['PB' + str(n) for n in range(1, 8)]
    for p in pbs:
        arrests[p + '_ID'] = arrests['IRF#'] + '.' + p
    for p in pbs:
        arrests[p + '_Case_ID'] = arrests['IRF#']
    dpb = {}
    for p in pbs:
        cnames = [col for col in arrests.columns if p in col]
        dpb['df_{0}'.format(p)] = pd.DataFrame(arrests[cnames])
    locals().update(dpb)

    df_list = []
    for k, v in dpb.items():
        df_list.append(v)

    new_col_names = ['Name',
                     'Arrested',
                     'Arrest_Date',
                     'suspect_id',
                     'Case_ID']

    for i, df in enumerate(df_list, 1):
        df.columns = new_col_names

    df_pb_all = pd.concat(df_list)

    arrests = df_pb_all[df_pb_all.Arrested.str.contains("Yes")]
    arrests['Total_Arrests'] = arrests.groupby(['Case_ID'])['Case_ID'].transform('count')
    arrests['Arrest'] = 1

    return arrests
