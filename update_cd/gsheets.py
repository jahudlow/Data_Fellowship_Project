'''
This is a module for working with Google Sheets.
'''

import json
import logging
import zipfile
import re
import gspread
from datetime import date, timedelta
from gspread_dataframe import set_with_dataframe

from oauth2client.client import SignedJwtAssertionCredentials
from apiclient.discovery import build
import pandas as pd


def get_gs_cred(cred_file):
    json_key = json.load(open(cred_file))
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = SignedJwtAssertionCredentials(json_key['client_email'],
                                                json_key['private_key'].encode(),
                                                scope)
    return credentials


def get_auth(credentials):
    """Authorize credentials for opening individual Google worksheets."""
    auth = gspread.authorize(credentials)
    return auth


def get_gsheets(workbook_name, auth): #remember to share new sheets with client email
    """Return a list of Google worksheets from the name of a Google Sheet."""
    workbook = auth.open(workbook_name)
    gsheets = workbook.worksheets()
    return gsheets


class GSheet:
    """This is a class for Google Worksheets."""
    def __init__(self, wrksht):
        self.wrksht = wrksht
        self.name = re.findall(r"'(.*?)'", str(wrksht))[0]
        df = pd.DataFrame(self.wrksht.get_all_values())
        df.columns = df.iloc[0]
        df.drop(0, inplace=True)
        self.df = df


def get_dfs(cdws):
    """Completes conversion of Google Sheets to Dataframes."""
    all_sheets = []
    for i in range(len(cdws)):
        sheet = GSheet(cdws[i])
        all_sheets.append(sheet)
    dfs = {sheet.name: sheet.df for sheet in all_sheets}
    return dfs


def save_backups(dfs):
    """Saves backup files with existing Google Sheet (Case Dispatcher) data."""
    today = date.today().strftime("%m-%d-%Y")
    zip_name = "backups/" + today + "_Backup_Sheets.zip"
    with zipfile.ZipFile(zip_name, 'w') as csv_zip:
        for k, v in dfs.items():
            fname = k + "_" + today + ".csv"
            csv_zip.writestr(fname, pd.DataFrame(v).to_csv())


def upload_sheets(new_gsheets, auth):
    """Uploads csv files to Google Sheets."""
    up_sheets = []
    for sheet in new_gsheets:
        up_sheet = open(sheet.csv, 'r').read()
        up_sheets.append(up_sheet)

        vs = auth.open("Victims")
        cvs = auth.open("Closed_Vic")
        ss = auth.open("Suspects")
        css = auth.open("Closed_Sus")
        ps = auth.open("Police")
        cps = auth.open("Closed_Pol")

        gs = [vs,
              cvs,
              ss,
              css,
              ps,
              cps]

        sheet_dict = {k: v for k, v in zip(gs, up_sheets)}
        last = len(list(sheet_dict))
        for i in range(0, last):
            auth.import_csv(
                list(
                    sheet_dict.keys())[i].id,
                list(
                    sheet_dict.values())[i].encode('utf-8'))


def upload_stats_sheet(auth):
    """"""
    today = date.today().strftime("%m/%d/%Y")
    sh = auth.open_by_key('19bm_1qKNV2KI6O4KNzQYUpQzycayd09Iw1MJna43FVA')
    stats_sheet = sh.sheet1
    stats_sheet.update_acell('B2', today)
    print("Google Sheet Case Dispatcher updated ", today)


def new_relationship_gsheets(sus, x, credentials):
    """Generate new google sheets for relationship data of top x number of suspects."""
    high_priority = sus[['Suspect_ID', 'Name', 'Relationships']]
    high_priority = high_priority.iloc[0:x,:]
    for index, row in high_priority.iterrows():
        if row['Relationships'] == "":
            row['Relationships'] = create_google_spreadsheet(
                str(row['Suspect_ID']) + "_relationships",
                sus_name=row['Name'],
                share_domains=['lovejustice.ngo', 'tinyhands.org'],
                credentials=credentials)
        else:
            continue
    y = sus.columns.get_loc('Relationships')
    sus.iloc[0:x, y] = high_priority.iloc[0:x,2]
    return sus

logger = logging.getLogger(__name__)


def open_google_spreadsheet(spreadsheet_id: str, credentials):
    """Open sheet using gspread.
    :param spreadsheet_id: Grab spreadsheet id from URL to open.
    Adapted from https://gist.github.com/miohtama/f988a5a83a301dd27469
    """
    gc = gspread.authorize(credentials)
    return gc.open_by_key(spreadsheet_id)


def create_google_spreadsheet(title, sus_name, share_domains, credentials):
    """Create a new spreadsheet and open gspread object for it.
    :param title: Spreadsheet title
    :param share_domains: Example:: ``["lovejustice.ngo"]``.
    """
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

    worksheet = open_google_spreadsheet(spread_id, credentials).sheet1
    rel_headers = pd.DataFrame(
        {"Name": [str(sus_name)],
         "Relationship_Type": [""],
         "Source": [""],
         "Target": [""],
         "Target_Label": [""],
         "Case_ID": [str(title[:-18])],
         "Suspect_Case_ID": [str(title[:-14])]},
        columns=["Name",
                 "Relationship_Type",
                 "Source",
                 "Target",
                 "Target_Label",
                 "Case_ID",
                 "Suspect_Case_ID"])
    set_with_dataframe(worksheet, rel_headers)

    spreadsheet_url = "https://docs.google.com/spreadsheets/d/%s" % spread_id
    return spreadsheet_url


def get_edge_direction(row):
    """Determine edge direction based on relationship type."""
    friend = re.findall(r'Friend', str(row))
    like = re.findall(r'Like', str(row))
    if friend:
        return 1
    elif like:
        return 3
    else:
        return 2


def pre_proc_links(new_link_sheet):
    """Fill content from first row of selected columns through last row and add a column."""
    cols = ['Name', 'Case_ID', 'Suspect_Case_ID']
    for c, col in enumerate(cols):
        new_link_sheet[cols[c]][new_link_sheet[cols[c]] == ''] = None
        new_link_sheet[cols[c]] = new_link_sheet[cols[c]].fillna(new_link_sheet[cols[c]][1])
    new_link_sheet['Edge_Direction'] = new_link_sheet['Relationship_Type'].apply(get_edge_direction)
    return new_link_sheet


def get_sheets_for_network_db(suspects, auth):
    """Collect and pre-process dictionary of recently updated relationship sheets so they are
    ready to be added to network database."""
    new_link_sheets = suspects.active[['Relationships', 'Date_Relationships_Updated']]
    new_link_sheets['Date_Relationships_Updated'] = pd.to_datetime(
        new_link_sheets['Date_Relationships_Updated'])
    yesterday = date.today() - timedelta(days=1)
    yesterday = yesterday.strftime("%m-%d-%Y")
    new_link_sheets = new_link_sheets[
        new_link_sheets.Date_Relationships_Updated >= yesterday]
    new_sheets = new_link_sheets.Relationships
    new_sheets = new_sheets.reset_index(drop=True)

    d = {}
    if not new_link_sheets.empty:
        for i in range(len(new_sheets.index)):
            d[i] = GSheet(auth.open_by_url(new_sheets[i]).sheet1)
    for i in d:
        d[i].df = pre_proc_links(d[i].df)
    return d
