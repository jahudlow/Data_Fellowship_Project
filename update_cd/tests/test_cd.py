
def test_gs_conn(gs_cred='creds.json', gs_name='Case Dispatcher 2.0'):
    try:
        credentials = gs.get_gs_cred(gs_cred)
        cdws = gs.get_gsheets(gs_name, credentials)
        dfs = gs.get_dfs(cdws)
        return True
    except:
        return False
