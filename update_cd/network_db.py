'''
This is a  module for setting up and updating a database of known suspect associations.
'''

import pandas as pd
import re
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy import ForeignKey
from sqlalchemy import Sequence
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import exists
pd.options.mode.chained_assignment = None


Base = declarative_base()

class Suspect(Base):
    '''Table containing suspect names and links metadata.'''
    __tablename__ = "suspects"

    id = Column(Integer, Sequence('sus_id_seq'), primary_key=True)
    name = Column(String(50))
    first_degree_links = Column(Integer)
    second_degree_links = Column(Integer)
    first_degree_case_links = Column(Integer)
    second_degree_case_links = Column(Integer)

    account = relationship("Account", backref="suspects", order_by="Account.id")
    edge = relationship("Edge", backref="suspects", order_by="Edge.id")

    def __repr__(self):
        return "<Suspect(name='%s', first_degree_case_links='%d', second_degree_case_links='%d', \
        first_degree_links='%d', second_degree_links='%d')>" % (
            self.name, self.first_degree_case_links, self.second_degree_case_links,
            self.first_degree_links, self.second_degree_links)


class Account(Base):
    '''Table containing data on accounts, including phone numbers and facebook pages.'''
    __tablename__ = "accounts"
    # __table_args__ = {'extend_existing': True}

    id = Column(Integer, Sequence('acc_id_seq'), primary_key=True)
    account_name = Column(String(50), nullable=False)
    account_label = Column(String(50))
    account_type_id = Column(Integer, ForeignKey('account_types.id'))
    suspect_id = Column(Integer, ForeignKey('suspects.id'))
    suspect = relationship(Suspect, primaryjoin=suspect_id == Suspect.id)


    def __repr__(self):
        return "<Account(account_name='%s', account_label='%s', account_type_id'%d')>" % (
            self.account_name, self.account_label, self.account_type_id)


Suspect.accounts = relationship(
    "Account", order_by=Account.id, back_populates="suspect")

class Account_Type(Base):
    '''Table for the types of connections between accounts.'''
    __tablename__ = "account_types"
    id = Column(Integer, primary_key=True)
    account_type = Column(String(20), nullable=False)

    def __repr__(self):
        return "<Account_Type(account_type='%s')>" % self.account_type


class Edge_Type(Base):
    '''Table for the types of connections between accounts.'''
    __tablename__ = "edge_types"
    # __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True)
    edge_type = Column(String(20), nullable=False)
    edge_type_weight = Column(Integer)

    def __repr__(self):
        return "<Edge_Type(edge_type='%s', edge_type_weight='%d')>" % (
            self.edge_type, self.edge_type_weight)


class Edge(Base):
    '''Table with all known associations between accounts.'''
    __tablename__ = "edges"
    id = Column(Integer, Sequence('edge_id_seq'), primary_key=True)
    source_suspect_id = Column(Integer, ForeignKey('suspects.id'))
    source_account_id = Column(Integer, ForeignKey('accounts.id'))
    target_account_id = Column(Integer, ForeignKey('accounts.id'))
    edge_type_id = Column(Integer, ForeignKey('edge_types.id'))
    edge_direction = Column(Integer)
    edge_combo_id = Column(String(20))

    def __repr__(self):
        return "<Edge(source_suspect_id='%d', source_account_id='%d', target_account_id='%d')>" % (
            self.source_suspect_id, self.source_account_id, self.target_account_id)


class Case(Base):
    '''Table with Case IDs associated with each suspect.'''
    __tablename__ = "cases"
    id = Column(Integer, Sequence('case_id_seq'), primary_key=True)
    case_number = Column(String(8))
    case_date = Column(String(8))

    case_sus = relationship("Case_Suspect", backref="case_suspect", order_by="Case_Suspect.id")


    def __repr__(self):
        return "<Case(case_number='%s', case_date='%s')>" % (
            self.case_number, self.case_date)


class Case_Suspect(Base):
    '''Table linking cases and suspects (many to many).'''
    __tablename__ = "case_suspects"
    id = Column(Integer, Sequence('case_suspect_id_seq'), primary_key=True)
    case_id = Column(Integer, ForeignKey('cases.id'))
    suspect_id = Column(Integer, ForeignKey('suspects.id'))
    suspect_case_id = Column(String(20))

    case = relationship(Case, primaryjoin=case_id == Case.id)
    suspect = relationship(Suspect, primaryjoin=suspect_id == Suspect.id)

    def __repr__(self):
        return "<Case_Suspect(suspect_id='%d', case_id='%d', suspect_case_id='%d')>" % (
            self.suspect_id, self.case_id, self.suspect_case_id)



engine = create_engine('sqlite:///test_db.db')
Session = sessionmaker(bind=engine)
session = Session()


def setup_database(engine, session):
    Base.metadata.create_all(engine)
    session.add_all([
        Edge_Type(id=1, edge_type='Facebook Friend', edge_type_weight=3),
        Edge_Type(id=2, edge_type='Facebook Like', edge_type_weight=2),
        Edge_Type(id=3, edge_type='Phone Contact', edge_type_weight=5),
        Edge_Type(id=4, edge_type='Phone Call', edge_type_weight=4),
        Edge_Type(id=5, edge_type='SMS', edge_type_weight=4),
    ])
    session.commit()

    session.add_all([
        Account_Type(id=1, account_type='Facebook'),
        Account_Type(id=2, account_type='Phone')
    ])
    session.commit()

    return


#old_entries = pd.read_csv('old_entries.csv', encoding="ISO-8859-1")

def import_initial_data(old_entries, engine):
    '''Setup database by importing old entries.'''

    # Note: This is just to show how I got the initial data in, I'm not planning to keep
    # this long function in the module after the db is set up.

    oe_suspects = old_entries[['Name']].drop_duplicates()
    oe_suspects.to_sql(name='suspects',
                       con=engine,
                       if_exists='append',
                       index=False)
    suspects = pd.read_sql('select * from suspects', engine)
    oe_sus_source = old_entries[['Name', 'Source']].drop_duplicates()
    oe_accounts = pd.merge(oe_sus_source,
                           suspects[['name', 'id']],
                           how='left',
                           left_on='Name',
                           right_on='name')
    oe_accounts = oe_accounts[['Source', 'id']]
    oe_accounts.columns = ['account_name', 'suspect_id']
    oe_target_accounts = old_entries[['Target', 'Target_Label']]
    oe_target_accounts.columns = ['account_name', 'account_label']
    oe_accounts = pd.merge(oe_accounts,
                           oe_target_accounts,
                           how='left').drop_duplicates(subset=['account_name', 'suspect_id'])
    oe_accounts.to_sql(name='accounts',
                       con=engine, if_exists='append', index=False)
    oe_target_accounts = oe_target_accounts.drop_duplicates(subset=['account_name'])
    oe_target_accounts = oe_target_accounts[~oe_target_accounts['account_name'].isin(
        oe_accounts['account_name'])]
    oe_target_accounts.to_sql(name='accounts',
                              con=engine,
                              if_exists='append',
                              index=False)

    accounts = pd.read_sql('select * from accounts', engine)
    account_ids = accounts[['account_name', 'id', 'suspect_id']]
    edges = pd.merge(old_entries,
                     account_ids,
                     how='left',
                     left_on='Source',
                     right_on='account_name')
    account_ids = accounts[['account_name', 'id']]
    edges = pd.merge(edges,
                     account_ids,
                     how='left',
                     left_on='Target',
                     right_on='account_name')
    edge_types = pd.read_sql('select * from edge_types', engine)
    edges = pd.merge(edges,
                     edge_types,
                     how='left',
                     left_on='Relationship_Type',
                     right_on='edge_type')
    edges = edges.drop_duplicates(subset=['Source', 'Target'])
    edges = edges[['suspect_id',
                   'id_x',
                   'id_y',
                   'id',
                   'Edge_Direction']]
    edges.columns = ['source_suspect_id',
                     'source_account_id',
                     'target_account_id',
                     'edge_type_id',
                     'edge_direction']
    edges['edge_combo_id'] = ''
    edges.edge_combo_id[
        edges.edge_direction == 1] = edges[
                                             'source_account_id'].astype(str) + '<->' + edges[
        'target_account_id'].astype(str)
    edges.edge_combo_id[
        edges.edge_direction == 2] = edges[
                                             'source_account_id'].astype(str) + '->' + edges[
        'target_account_id'].astype(str)
    edges.edge_combo_id[
        edges.edge_direction == 3] = edges[
                                             'source_account_id'].astype(str) + '<-' + edges[
        'target_account_id'].astype(str)
    edges.to_sql(name='edges',
                 con=engine,
                 if_exists='append',
                 index=False)
    oe_cases = old_entries[['Case_ID','Suspect_Case_ID']]
    oe_cases['case_date'] = ''
    oe_cases = oe_cases[['Case_ID','case_date']]
    oe_cases.rename(columns={'Case_ID':'case_number'}, inplace=True)
    oe_cases = oe_cases.drop_duplicates('case_number')
    oe_cases.to_sql(name='cases',
                    con=engine,
                    if_exists='append', index=False)
    oe_cases = pd.read_sql('select * from cases', engine)
    oe_case_suspects = old_entries[['Case_ID', 'Suspect_Case_ID', 'Name']]
    oe_case_suspects = pd.merge(oe_cases,
                             oe_case_suspects,
                             how='left',
                             left_on='case_number',
                             right_on='Case_ID')
    oe_case_suspects = pd.merge(oe_case_suspects,
                                 suspects[['name', 'id']],
                                 how='left',
                                 left_on='Name',
                                 right_on='name')
    oe_case_suspects = oe_case_suspects[['id_x','id_y','Suspect_Case_ID']]
    oe_case_suspects.columns = ['case_id', 'suspect_id', 'suspect_case_id']
    oe_case_suspects.drop_duplicates(subset='suspect_id',inplace=True)
    oe_case_suspects.to_sql(name='case_suspects',
                    con=engine,
                    if_exists='append', index=False)


def add_suspect(new_links):
    '''Add a suspect's name to the database and return his db suspect_id.'''
    session = Session()
    new_sus_name = new_links['Name'][1]
    new_suspect = Suspect(name=new_sus_name)
    session.add(new_suspect)
    session.commit()
    session.close()
    return new_suspect.id


def add_case(new_links):
    '''Add case number to database and return table case ID.'''
    case_number = new_links['Case_ID'][1]
    new_case = Case(case_number=case_number)
    session.add(new_case)
    session.commit()
    return new_case.id


def add_case_suspect_link(new_suspect_id, new_case_id, new_links):
    '''Add case_suspect entry linking case and suspect tables.'''
    new_case_suspect = Case_Suspect(case_id=new_case_id,
                                    suspect_id=new_suspect_id,
                                    suspect_case_id=new_links['Suspect_Case_ID'][1])
    session.add(new_case_suspect)
    session.commit()


def get_account_type(row):
    '''Assign account type based on whether the word 'facebook' appears in account name.'''
    match = re.findall(r'facebook', str(row))
    if match:
        return 1
    else:
        return 2

def add_new_accounts(new_links, new_suspect_id):
    '''Add source and target accounts from csv file to database.'''
    new_sus_source = new_links[['Name', 'Source']].drop_duplicates()
    new_sus_source['account_label'] = ''
    new_sus_source['suspect_id'] = new_suspect_id
    new_sus_source = new_sus_source[['Source', 'account_label', 'suspect_id']]
    new_sus_source.rename(columns={'Source': 'account_name'}, inplace=True)

    new_target_accounts = new_links[['Target', 'Target_Label']]
    new_target_accounts.columns = ['account_name', 'account_label']
    new_target_accounts['suspect_id'] = ''
    new_accounts = new_sus_source.append(
        new_target_accounts, ignore_index=True)
    new_accounts['account_type_id'] = new_accounts['account_name'].apply(get_account_type)
    accounts = pd.read_sql('select * from accounts', engine)
    new_accounts = new_accounts[~new_accounts['account_name'].isin(
        accounts['account_name'])]
    new_accounts.to_sql(name='accounts',
                        con=engine,
                        if_exists='append',
                        index=False)


def add_new_edges(new_links):
    '''Add edges from csv file to database.'''
    accounts = pd.read_sql('select * from accounts', engine)
    account_ids = accounts[['account_name', 'id', 'suspect_id']]
    new_edges = pd.merge(new_links,
                         account_ids,
                         how='left',
                         left_on='Source',
                         right_on='account_name')
    account_ids = accounts[['account_name', 'id']]
    new_edges = pd.merge(new_edges,
                         account_ids,
                         how='left',
                         left_on='Target',
                         right_on='account_name')
    new_edge_types = pd.read_sql('select * from edge_types', engine)
    new_edges = pd.merge(new_edges,
                         new_edge_types,
                         how='left',
                         left_on='Relationship_Type',
                         right_on='edge_type')
    new_edges = new_edges.drop_duplicates(subset=['Source', 'Target'])
    new_edges = new_edges[['suspect_id',
                           'id_x',
                           'id_y',
                           'id',
                           'Edge_Direction']]
    new_edges.columns = ['source_suspect_id',
                         'source_account_id',
                         'target_account_id',
                         'edge_type_id',
                         'edge_direction']
    new_edges['edge_combo_id'] = ''
    new_edges.edge_combo_id[
        new_edges.edge_direction == 1] = new_edges[
            'source_account_id'].astype(str) + '<->' + new_edges[
            'target_account_id'].astype(str)
    new_edges.edge_combo_id[
        new_edges.edge_direction == 2] = new_edges[
            'source_account_id'].astype(str) + '->' + new_edges[
            'target_account_id'].astype(str)
    new_edges.edge_combo_id[
        new_edges.edge_direction == 3] = new_edges[
            'source_account_id'].astype(str) + '<-' + new_edges[
            'target_account_id'].astype(str)
    edges = pd.read_sql('select * from edges', engine)
    new_edges = new_edges[~new_edges['edge_combo_id'].isin(
        edges['edge_combo_id'])]
    new_edges.to_sql(name='edges',
                     con=engine,
                     if_exists='append',
                     index=False)


def update_first_degree_links(engine):
    '''Calculates first degree links for each suspect and updates database with them.'''
    edges = pd.read_sql('select * from edges', engine)
    suspect_links = pd.DataFrame(
        edges.groupby(
            'source_suspect_id',
            as_index=False).source_account_id.count())
    suspects = pd.read_sql('select * from suspects', engine)
    suspects_cols = len(suspects.columns)
    suspects = pd.merge(
        suspects,
        suspect_links,
        how='left',
        left_on='id',
        right_on='source_suspect_id')
    suspects['first_degree_links'] = suspects['source_account_id'].astype(int)
    suspects = suspects.iloc[:, 0:suspects_cols]
    suspects.to_sql(
        name='suspects',
        con=engine,
        if_exists='replace',
        index=False)


def update_second_degree_links(engine):
    '''Calculates second degree links for each suspect and updates database with them.'''
    edges = pd.read_sql('select * from edges', engine)
    second_degree = pd.DataFrame(
        edges.groupby(
            'target_account_id',
            as_index=False).source_account_id.count())
    second_degree.columns = ['target_account_id', 'source_account_count']
    second_degree = pd.merge(
        second_degree,
        edges[['source_account_id', 'source_suspect_id']],
        how='left',
        left_on='target_account_id',
        right_on='source_account_id')
    second_degree.drop_duplicates('target_account_id', inplace=True)
    second_degree.dropna(inplace=True)
    suspect_links2 = pd.DataFrame(second_degree.groupby(
        'source_suspect_id',
        as_index=False).source_account_count.count())
    suspects = pd.read_sql('select * from suspects', engine)
    suspects_cols = len(suspects.columns)
    suspects = pd.merge(
        suspects,
        suspect_links2,
        how='left',
        left_on='id',
        right_on='source_suspect_id')
    suspects['source_account_count'] = suspects['source_account_count'].fillna(0)
    suspects['second_degree_links'] = suspects['source_account_count'].astype(int)
    suspects = suspects.iloc[:, 0:suspects_cols]
    suspects.to_sql(
        name='suspects',
        con=engine,
        if_exists='replace',
        index=False)


def index_multi_match(df, match_col1, match_col2, index):
    '''Returns counts and comma separated lists of indices for matches between two columns.'''
    df1 = df[[match_col1, index]]
    df2 = df[[match_col2, index]]
    df1 = df1.rename(columns={match_col1: match_col2})
    group = pd.merge(df1,
                 df2,
                 how='left',
                 on=match_col2)
    colnames = ['match', 'idx1', 'idx2']
    group.columns = colnames
    group = group[group.idx1 != group.idx2]
    group['combID'] = group['match'].astype(str) + "-case_id-" + group['idx1'].astype(str)
    group = group.drop_duplicates(subset=['idx1', 'idx2'])
    group['count'] = 1
    group2 = group
    group = group.groupby('combID').apply(lambda x: pd.Series(dict(count=x['count'].sum(),
                                                           idx2=', '.join(x.astype(str)['idx2']))))
    group = group.merge(group2[['combID', 'match']], on='combID', how='left')
    group = group.drop_duplicates(subset='combID')
    if not group.empty:
        group = group[group.idx2 != 'nan'].dropna()
    else:
        pass
    return group


def update_case_links(degree):
    '''Calculates for each suspect first or second degree links
    which have a different case number.'''
    if degree == 'first':
        col2 = 'source_account_id'
        sus_col = 'first_degree_case_links'
    elif degree == 'second':
        col2 = 'target_account_id'
        sus_col = 'second_degree_case_links'
    edges = pd.read_sql('select * from edges', engine)
    case_suspects = pd.read_sql('select * from case_suspects', engine)
    cases = pd.read_sql('select * from cases', engine)
    case_links = pd.merge(case_suspects, cases, left_on='case_id', right_on='id')
    case_links = pd.merge(
        edges,
        case_links,
        how='left',
        left_on='source_suspect_id',
        right_on='suspect_id')
    case_links = index_multi_match(case_links, 'source_account_id', col2, 'case_number')
    if not case_links.empty:
        case_links = pd.merge(case_links[['count', 'match']],
                              edges[['source_account_id', 'source_suspect_id']],
                              how='left', left_on='match', right_on='source_account_id')
        case_links.drop_duplicates(subset='source_suspect_id', inplace=True)
        case_links.rename(columns={'count':'case_count'}, inplace=True)
        suspects = pd.read_sql('select * from suspects', engine)
        suspects_cols = len(suspects.columns)
        suspects = pd.merge(
            suspects,
            case_links,
            how='left',
            left_on='id',
            right_on='source_suspect_id')
        suspects['case_count'] = suspects['case_count'].fillna(0)
        suspects[sus_col] = suspects['case_count'].astype(int)
        suspects = suspects.iloc[:, 0:suspects_cols]
        suspects.to_sql(
            name='suspects',
            con=engine,
            if_exists='replace',
            index=False)


def add_update_links(new_links):
    '''Create new entries in db from "new_links" and update link calculations.'''
    new_suspect_id = add_suspect(new_links)
    new_case_id = add_case(new_links)
    add_case_suspect_link(new_suspect_id, new_case_id, new_links)
    add_new_accounts(new_links, new_suspect_id)
    add_new_edges(new_links)
    update_first_degree_links(engine)
    update_second_degree_links(engine)
    update_case_links('first')
    update_case_links('second')


def add_update_links_dict(d):
    '''Run "add_update_links" function in a loop over a dictionary of GSheet objects.'''
    for i in d:
        add_update_links(d[i].df)
