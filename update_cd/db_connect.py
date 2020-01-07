'''
This is a module for connecting to postgresql database and executing queries.
'''

from configparser import ConfigParser
import pandas as pd
import psycopg2

class DB_Conn(object):
    """This is a class for establishing a connection with the database."""
    def __init__(self, db_filename, section='postgresql'):
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(str(db_filename))
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, db_filename))

        params = db
        conn = psycopg2.connect(**params)
        self.conn = conn
        self.cur = conn.cursor()

    def ex_query(self, select_query):
        """Execute query and return dataframe."""
        query = select_query
        cur = self.cur
        cur.execute(query)
        colnames = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=colnames)

    def close_conn(self):
        """Close the cursor and the connection."""
        self.cur.close()
        self.conn.close()
        print("PostgreSQL connection is closed")
