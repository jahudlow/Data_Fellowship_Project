'''
This is a module for working with Google Sheets.
'''

import json
import logging
import zipfile
from datetime import date
import re
import gspread
from gspread_dataframe import set_with_dataframe

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build
import pandas as pd

def get_credentials():
    json_key = json.load(open('creds.json'))
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = SignedJwtAssertionCredentials(json_key['client_email'],
                                                json_key['private_key'].encode(),
                                                scope)
    return credentials

def get_gsheets(workbook_name):
    '''Return a list of Google worksheets from the name of a Google Sheet.'''
    credentials = get_credentials()
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
        for k, v in dfs.items():
            fname = k + "_" + today + ".csv"
            csv_zip.writestr(fname, pd.DataFrame(v).to_csv())

def rename_gs(dfs):
    '''Add 'GS' suffix to sheet names.'''
    for sheet in dfs:
        sheet.name = sheet.name + '_GS'
    return dfs

def upload_sheets(new_gsheets):
    """Uploads csv files to Google Sheets."""
    up_sheets = []
    for sheet in new_gsheets:
        up_sheet = open(sheet.csv, 'r').read()
        up_sheets.append(up_sheet)
        credentials = get_credentials()
        file = gspread.authorize(credentials)

        vs = file.open("Victims")
        cvs = file.open("Closed_Vic")
        ss = file.open("Suspects")
        css = file.open("Closed_Sus")
        ps = file.open("Police")
        cps = file.open("Closed_Pol")

        gs = [vs,
              cvs,
              ss,
              css,
              ps,
              cps]

        sheet_dict = {k: v for k, v in zip(gs, up_sheets)}
        file = gspread.authorize(credentials)
        last = len(list(sheet_dict))
        for i in range(0, last):
            file.import_csv(
                list(
                    sheet_dict.keys())[i].id,
                list(
                    sheet_dict.values())[i].encode('utf-8'))

def create_new_links_sheet(s_list):

    credentials = get_credentials()
    gc = gspread.authorize(credentials)
    for s in s_list:
        spreadsheet = gc.create(s + '_links')
        spreadsheet.share('jon@lovejustice.ngo', perm_type='user', role='writer')
        spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % spreadsheet.id
        print(spreadsheet_url)

# Code below is modified from https://gist.github.com/miohtama/f988a5a83a301dd27469

logger = logging.getLogger(__name__)

def open_google_spreadsheet(spreadsheet_id: str):
    """Open sheet using gspread.
    :param spreadsheet_id: Grab spreadsheet id from URL to open.
    """
    credentials = get_credentials()
    gc = gspread.authorize(credentials)
    return gc.open_by_key(spreadsheet_id)

def create_google_spreadsheet(title: str, sus_name, share_domains: list = None):
    """Create a new spreadsheet and open gspread object for it.
    :param title: Spreadsheet title
    :param share_domains: Example:: ``["redinnovation.com"]``.
    """
    credentials = get_credentials()
    drive_api = build('drive', 'v3', credentials=credentials)
    logger.info("Creating Sheet %s", title)
    body = {
        'name': title,
        'mimeType': 'application/vnd.google-apps.spreadsheet',
    }
    req = drive_api.files().create(body=body)
    new_sheet = req.execute()

    # Get id of fresh sheet
    spread_id = new_sheet["id"]

    # Grant permissions
    if share_domains:
        for domain in share_domains:

            # https://developers.google.com/drive/v3/web/manage-sharing#roles
            # https://developers.google.com/drive/v3/reference/permissions#resource-representations
            domain_permission = {
                'type': 'domain',
                'role': 'writer',
                'domain': domain,
                # Magic almost undocumented variable which makes files appear in your Google Drive
                'allowFileDiscovery': True,
            }

            req = drive_api.permissions().create(
                fileId=spread_id,
                body=domain_permission,
                fields="id"
            )

            req.execute()

    worksheet = open_google_spreadsheet(spread_id).sheet1
    rel_headers = pd.DataFrame(
        {str(sus_name): [],
         "Relationship_Type": [],
         "Source": [],
         "Target": [],
         "Target_Label": []}, index=[])
    set_with_dataframe(worksheet, rel_headers)

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % spread_id
    return spreadsheet_url

def create_new_rel_sheets(sus, x):
    '''Generate new google sheets for relationship data of top x suspects.'''
    high_priority = sus.iloc[0:x, [1, 9, 15]]
    for index, row in high_priority.iterrows():
        if row['Relationships'] == "":
            row['Relationships'] = create_google_spreadsheet(
                str(row['Suspect_ID']) + "_relationships",
                sus_name=row['Name'],
                share_domains=['lovejustice.ngo', 'tinyhands.org'])
    high_priority = high_priority.iloc[:, [0, 2]]
    high_priority.columns = ['Suspect_ID', 'Rel']
    sus = pd.merge(sus, high_priority, how='left')
    sus['Relationships'] = sus['Rel']
    sus = sus.iloc[:, 0:43]
    return sus
