from config import config
import pandas as pd
import psycopg2

class DBConn(object):
    '''This is a class for querying the database and returning a pandas dataframe.'''
    def __init__(self):
        params = config()
        conn = psycopg2.connect(**params)
        self.cur = conn.cursor()

    def ex_query(self, query):
        query = query
        cur = self.cur
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows, columns=colnames)