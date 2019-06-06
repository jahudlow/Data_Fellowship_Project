import pandas as pd

def pre_proc(soc_df):
    '''Takes data from DB and removes columns that won't be used.'''
    dfcols = list(soc_df.columns)
    dfcols[16] = 'pv_occupation'
    soc_df.columns = dfcols
    soc_df['pb_number'] = soc_df['pb_number'].fillna(0).astype(int)
    soc_df['suspect_id'] = soc_df['cif_number'].str.replace('.', '')
    soc_df['suspect_id'] = soc_df['suspect_id'].str[:-1] + ".PB" + soc_df['pb_number'].map(str)
    soc_df = soc_df.drop_duplicates(subset='suspect_id')

    #Remove columns that won't be used
    drop_cols = [
        'id',
        'date_time_entered_into_system',
        'status',
        'location',
        'date_time_last_updated',
        'staff_name',
        'informant_number',
        'case_notes',
        'pv_signed_form',
        'consent_for_fundraising',
        'social_media',
        'legal_action_taken_filed_against',
        'officer_name',
        'cif_id',
        'person_id',
        'flag_count',
        'main_pv_id',
        'expected_earning',
        'expected_earning_currency',
        'travel_expenses_paid_to_broker_amount',
        'broker_relation',
        'travel_expenses_broker_repaid_amount',
        'form_entered_by_id',
        'source_of_intelligence',
        'date_time_last_updated',
        'incident_date',
        'how_recruited_broker_other',
        'legal_action_taken',
        'legal_action_taken_case_type',
        'appearance',
        'date_visit_police_station',
        'victim_statement_certified_date',
        'purpose_for_leaving_other',
        'relation_to_pv',
        'exploitation_other_value'
    ]

    for x in soc_df.columns:
        if "contact" in x[:] or "_lb" in x[:] or "guardian" in x[:]:
            drop_cols.append(x)

    soc_df = soc_df.drop(columns=drop_cols)

    return soc_df
