'''
This is a module for connecting to postgresql database and executing queries.
'''

from configparser import ConfigParser
import pandas as pd
import psycopg2

def config(filename='database.ini', section='postgresql'):
    '''Configure database connection.'''
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

class DB_Conn(object):
    '''This is a class for querying the database and returning a pandas dataframe.'''
    def __init__(self):
        params = config()
        conn = psycopg2.connect(**params)
        self.cur = conn.cursor()

    def ex_query(self, query):
        '''Execute query and return dataframe.'''
        query = query
        cur = self.cur
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        cur.close()
        return pd.DataFrame(rows, columns=colnames)