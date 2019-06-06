import json
import gspread
import zipfile
from datetime import date
from oauth2client.client import SignedJwtAssertionCredentials
import pandas as pd
import re
import zipfile

def get_gsheets(workbook_name):
    '''Return a list of Google worksheets from the name of a Google Sheet.'''
    json_key = json.load(open('creds.json'))
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']
    credentials = SignedJwtAssertionCredentials(json_key['client_email'], json_key['private_key'].encode(), scope)
    file = gspread.authorize(credentials)  #remember to share new sheets with client email
    workbook = file.open(workbook_name)
    gsheets = workbook.worksheets()
    return gsheets

class GSheet:
    '''This is a class for Google Worksheets.'''
    def __init__(self, wrksht):
        self.wrksht = wrksht
        self.name = re.findall(r"'(.*?)'", str(wrksht))[0]
        df = pd.DataFrame(self.wrksht.get_all_values())
        df.columns = df.iloc[0]
        df.drop(0, inplace=True)
        self.df = df

def get_dfs(cdws):
    '''Completes conversion of Google Sheets to Dataframes.'''
    all_sheets = []
    for i in range(len(cdws)):
        sheet = GSheet(cdws[i])
        all_sheets.append(sheet)
    dfs = {sheet.name: sheet.df for sheet in all_sheets}
    return dfs

def save_backups(dfs):
    '''Saves backup files with existing Google Sheet (Case Dispatcher) data.'''
    today = date.today().strftime("%m-%d-%Y")
    zip_name = today + "_Backup_Sheets.zip"
    with zipfile.ZipFile(zip_name, 'w') as csv_zip:
        for k,v in dfs.items():
            fname= k + "_" + today + ".csv"
            csv_zip.writestr(fname, pd.DataFrame(v).to_csv())
    return

def rename_gs(dfs):
    for sheet in dfs:
        sheet.name = sheet.name + '_GS'
    return dfs
