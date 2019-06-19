'''
This is a module for working with Entity Groups (Victims, Suspects, Police).
'''
from datetime import date
import pandas as pd

def subset_addresses(db_add):
    '''Selects and renames address fields from database which will be used.'''
    addr = db_add.iloc[:, [1, 6, 7]]
    acols = ['address_1',
             'address2_id',
             'address_2']
    addr.columns = acols
    return addr

class GetAttr:
    '''This is a class which allows objects in it's subclasses to be indexed.'''
    def __getitem__(cls, x):
        return getattr(cls, x)

class Entity_Group(GetAttr):
    '''This is a class for Victims, Suspects, and Police entity groups with corresponding sheets.'''
    sheets = []

    def __init__(self, uid, new_cases, active_gsheet, closed_gsheet, name):
        Entity_Group.sheets.append(self)
        self.uid = uid
        self.new = new_cases
        self.gsheet = active_gsheet
        self.closed = closed_gsheet
        self.name = name

    @classmethod
    def merge_addresses(cls, addr):
        '''Adds relevant address data to new entity groups.'''
        addr = addr
        for sheet in cls.sheets:
            #sheet.new.infer_objects
            sheet.new['address1_id'] = sheet.new['address1_id'].fillna(0).astype(int)
            sheet.new['address2_id'] = sheet.new['address2_id'].fillna(0).astype(int)
            sheet.new = pd.merge(sheet.new, addr, how='left', on='address2_id')
            sheet.new['Address'] = sheet.new['address_2'].map(str) + ", " + sheet.new['address_1']

    @classmethod
    def set_case_id(cls):
        '''Creates a Case ID from the form ID stored in the database.'''
        for sheet in cls.sheets:
            sheet.new.loc[:, 'Case_ID'] = sheet.new['Case_ID'].str.replace('.', '')
            sheet.new['Case_ID'] = sheet.new['Case_ID'].str[:-1]

    @classmethod
    def combine_sheets(cls):
        '''Adds new case data to data already in the corresponding Google Sheet.'''
        for sheet in cls.sheets:
            sheet.new = sheet.new.reindex(
                columns=sheet.new.columns.tolist() + list(sheet.gsheet.columns))
            sheet.new = sheet.new.iloc[:, 5:len(sheet.new.columns)]
            sheet.active = pd.concat([sheet.gsheet, sheet.new], sort=False)
            sheet.active.drop_duplicates(subset=sheet.uid, inplace=True)

    @classmethod
    def move_closed(cls, arrests):
        '''Moves closed cases to the closed sheet for each Entity Group instance.'''
        for sheet in cls.sheets:
            prev_closed = sheet.new[sheet.new[sheet.uid].isin(arrests.suspect_id)]
            prev_closed['Case_Status'] = "Closed: Already in Legal Cases Sheet"
            newly_closed = sheet.gsheet[sheet.gsheet['Date_Closed'].str.len() > 1]
            sheet.closed = pd.concat([sheet.closed, prev_closed, newly_closed], sort=False)
            sheet.closed.drop_duplicates(subset=sheet.uid, inplace=True)
            sheet.active = sheet.active[~sheet.active[sheet.uid].isin(sheet.closed[sheet.uid])]

    @classmethod
    def move_other_closed(cls, suspects, police, victims):
        '''Moves cases closed in other Entity Group instances to closed sheets.'''
        closed_suspects = suspects.active[
            (suspects.active['Suspect_ID'].isin(police.closed['Suspect_ID'])) |
            (~suspects.active['Case_ID'].isin(victims.active['Case_ID']))]
        closed_police = police.active[
            (police.active['Suspect_ID'].isin(suspects.closed['Suspect_ID'])) |
            (~police.active['Case_ID'].isin(victims.active['Case_ID']))]
        closed_victims = victims.active[
            (~victims.active['Case_ID'].isin(police.active['Case_ID'])) |
            (~victims.active['Case_ID'].isin(suspects.active['Case_ID']))]
        suspects.closed = pd.concat(
            [suspects.closed, closed_suspects],
            sort=False).drop_duplicates(subset='Suspect_ID')
        police.closed = pd.concat(
            [police.closed, closed_police],
            sort=False).drop_duplicates(subset='Suspect_ID')
        victims.closed = pd.concat(
            [victims.closed, closed_victims],
            sort=False).drop_duplicates(subset='Victim_ID')
        for sheet in cls.sheets:
            sheet.active = sheet.active[~sheet.active[sheet.uid].isin(sheet.closed[sheet.uid])]

    new_gsheets = []

    @classmethod
    def save_csvs(cls):
        ''''Write csvs for active/closed in each Entity Group and return list of new gsheets.'''
        for sheet in cls.sheets:
            sheet.active.csv = 'backups/' + sheet.name + '.csv'
            sheet.closed.csv = 'backups/closed_' + sheet.name[:3] + '.csv'
        for sheet in cls.sheets:
            Entity_Group.new_gsheets.append(sheet.active)
            Entity_Group.new_gsheets.append(sheet.closed)
        for ngs in cls.new_gsheets:
            ngs.to_csv(ngs.csv, index=False, header=None)
        return cls.new_gsheets

def set_vic_id(new_victims):
    '''Creates a unique ID for each victim from Case ID and subsets/renames columns.'''
    new_victims = new_victims[['cif_number',
                               'full_name',
                               'phone_contact',
                               'Address']]
    new_victims.loc[:, 'Victim_ID'] = new_victims['cif_number']
    replacements = {
        'Victim_ID': {
            r'(\.1|A$)': '.V1', r'B$': '.V2', r'C$': '.V3', r'D$': '.V4', r'E$': '.V5',
            r'F$': '.V6', r'G$': '.V7', r'H$': '.V8', r'I$': '.V9', r'J$': '.V10'}
    }
    new_victims.replace(replacements, regex=True, inplace=True)
    new_victims.sort_values('full_name', inplace=True)
    new_victims = new_victims.drop_duplicates(subset='Victim_ID')
    non_blanks = new_victims['full_name'] != ""
    new_victims = new_victims[non_blanks]
    vcols = ['Case_ID', 'Name',
             'Phone_Number(s)',
             'Address',
             'Victim_ID']
    new_victims.columns = vcols
    return new_victims

def set_sus_id(new_suspects, db_cif):
    '''Creates a unique ID for each suspect from Case ID and subsets/renames columns.'''
    new_suspects = new_suspects[['person_id',
                                 'full_name',
                                 'phone_contact',
                                 'Address']]
    cif_ids = db_cif[['cif_number', 'person_id', 'pb_number']]
    new_suspects = pd.merge(new_suspects, cif_ids, how='outer', on='person_id', sort=True,
                            suffixes=('x', 'y'), copy=True)
    new_suspects.loc[:, 'pb_number'] = new_suspects['pb_number'].fillna(0).astype(int)
    new_suspects.loc[:, 'Suspect_ID'] = new_suspects.loc[:, 'cif_number'].str.replace('.', '')
    new_suspects.loc[:, 'Suspect_ID'] = new_suspects.loc[:, 'Suspect_ID'].str[:-1] + \
                                        ".PB" + new_suspects['pb_number'].map(str)
    new_suspects = new_suspects.drop_duplicates(subset='Suspect_ID')
    new_suspects = new_suspects.iloc[:, [1, 2, 3, 4, 6]]
    new_suspects.rename(columns={
        'full_name': 'Name',
        'phone_contact': 'Phone_Number(s)',
        'cif_number': 'Case_ID'}, inplace=True)
    return new_suspects

def add_cdate_var(Sheets):
    """Adds a variable with the current date to the end of each dataframe in a list."""
    today = date.today()
    for sheet in Sheets:
        if len(sheet) > 0:
            sheet.loc[:, 'Date_Closed'] = today.strftime("%m/%d/%Y")
        else:
            sheet['Date_Closed'] = ""
