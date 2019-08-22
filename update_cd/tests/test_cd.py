from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import update_cd.network_db as ndb
import update_cd.gsheets as gs
import pandas as pd
import numpy as np

engine = create_engine('sqlite:///:memory:')

Session = sessionmaker(bind=engine)
session = Session()


def setup():
    ndb.setup_database(engine, session)


def teardown():
    session.remove()


def test_pre_proc_links():
    """Use a test dataframe to make sure that the pre_proc function is correctly copying
    the string in the first cell down to the last cell."""
    test_net = pd.read_csv('test_network_data.csv', encoding="ISO-8859-1", keep_default_na=False)
    test_net.index = np.arange(1, len(test_net) + 1)
    output = gs.pre_proc_links(test_net)
    assert output.iloc[3, 0] == 'Test 1'


def test_import_initial_data():
    test_net = pd.read_csv('test_network_data.csv', encoding="ISO-8859-1", keep_default_na=False)
    test_net.index = np.arange(1, len(test_net) + 1)
    old_entries = gs.pre_proc_links(test_net)
    ndb.import_initial_data(old_entries, engine)
    sus_name = pd.read_sql('select name from suspects', engine)
    assert sus_name.iloc[0, 0] == 'Test 1'


def test_update_first_degree_links(engine):
    test_net = pd.read_csv('test_network_data.csv', encoding="ISO-8859-1", keep_default_na=False)
    test_net.index = np.arange(1, len(test_net) + 1)
    old_entries = gs.pre_proc_links(test_net)
    ndb.import_initial_data(old_entries, engine)
    ndb.update_first_degree_links(engine)
    sus_links = pd.read_sql('select first_degree_links from suspects', engine)
    assert int(sus_links.iloc[0, 0]) > 0


def test_gs_conn(gs_cred='creds.json', gs_name='Case Dispatcher 2.0'):
    try:
        credentials = gs.get_gs_cred(gs_cred)
        cdws = gs.get_gsheets(gs_name, credentials)
        dfs = gs.get_dfs(cdws)
        return True
    except:
        return False


if __name__ == '__main__':
    unittest.main()
