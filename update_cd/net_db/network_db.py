'''
This is a  module for setting up and updating a database of known suspect associations.
'''

import pandas as pd
import re
from net_db.edge_cls import EdgeType, Edge
from net_db.account_cls import AccountType, Account
from net_db.suspect_cls import Suspect
from net_db.case_cls import Case
from net_db.case_suspect_cls import CaseSuspect
from net_db import engine, session, Base

pd.options.mode.chained_assignment = None


def create_database(engine):
    """Creates database using declarative_base.

    Args:
        engine: Object that helps to create and interact with database.
    """
    Base.metadata.create_all(engine)


def create_edge_types(session):
    """Creates edge types in database which describe the connection or relationship
    between persons of interest.

    Args:
        session: The active session for connecting to the database.
    """
    session.add_all([
        EdgeType(id=1, edge_type='Facebook Friend', edge_type_weight=3),
        EdgeType(id=2, edge_type='Facebook Like', edge_type_weight=2),
        EdgeType(id=3, edge_type='Phone Contact', edge_type_weight=5),
        EdgeType(id=4, edge_type='Phone Call', edge_type_weight=4),
        EdgeType(id=5, edge_type='SMS', edge_type_weight=4),
    ])
    session.commit()


def create_account_types(session):
    """Creates account types in database which describe the connection or relationship
    between persons of interest.

    Args:
        session: The active session for connecting to the database.
    """
    session.add_all([
        AccountType(id=1, account_type='Facebook'),
        AccountType(id=2, account_type='Phone')
    ])
    session.commit()


def setup_database(engine=engine, session=session):
    """Sets up a new database by initiating it and defining edge and account types.

    Args:
        engine: Object that helps to create and interact with database.
        session: The active session for connecting to the database.
    """
    create_database(engine)
    create_edge_types(session)
    create_account_types(session)


#old_entries = pd.read_csv('old_entries.csv', encoding="ISO-8859-1")


def add_old_suspects(old_entries, engine):
    """Adds names of suspects to database.

    Assumes that suspect names in dataframe will be unique to each individual.

    Args:
        old_entries: A dataframe containing source and target link data for
        previously collected cases.
        engine: Object that helps to create and interact with database.
    """
    oe_suspects = old_entries[['Name']].drop_duplicates()
    oe_suspects.to_sql(name='suspects',
                       con=engine,
                       if_exists='append',
                       index=False)


def add_old_accounts(old_entries, engine):
    """Adds account names to database.

    Creates an entry for each unique account in the accounts table, generating
    an id for each. For suspect accounts it associates each account with a
    corresponding suspect id.

    Args:
        old_entries: A dataframe containing source and target link data for
        previously collected cases.
        engine: Object that helps to create and interact with database.
    """
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


def add_old_edges(old_entries, engine):
    """Adds edges describing associations between accounts to database.

    Maps the connections between source and target accounts and stores
    their ids in adjacent columns. Creates a 'combo id' which describes
    the relationship including directionality.

    Args:
        old_entries: A dataframe containing source and target link data for
        previously collected cases.
        engine: Object that helps to create and interact with database.
    """
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


def add_old_cases(old_entries, engine):
    """Adds case numbers to database.

    Args:
        old_entries: A dataframe containing source and target link data for
        previously collected cases.
        engine: Object that helps to create and interact with database.
    """
    oe_cases = old_entries[['Case_ID', 'Suspect_Case_ID']]
    oe_cases['case_date'] = ''
    oe_cases = oe_cases[['Case_ID', 'case_date']]
    oe_cases.rename(columns={'Case_ID': 'case_number'}, inplace=True)
    oe_cases = oe_cases.drop_duplicates('case_number')
    oe_cases.to_sql(name='cases',
                    con=engine,
                    if_exists='append', index=False)


def add_old_case_suspects(old_entries, engine):
    """Adds case_suspect table entries to database linking cases and suspects.

    Args:
        old_entries: A dataframe containing source and target link data for
        previously collected cases.
        engine: Object that helps to create and interact with database.
    """
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
    oe_case_suspects = oe_case_suspects[['id_x', 'id_y', 'Suspect_Case_ID']]
    oe_case_suspects.columns = ['case_id', 'suspect_id', 'suspect_case_id']
    oe_case_suspects.drop_duplicates(subset='suspect_id', inplace=True)
    oe_case_suspects.to_sql(name='case_suspects',
                            con=engine,
                            if_exists='append', index=False)


def import_old_data(old_entries, engine=engine):
    """Calls all of the functions needed to import old_entries into database.

    Args:
        old_entries: A dataframe containing source and target link data for
        previously collected cases.
        engine: Object that helps to create and interact with database.
    """
    add_old_suspects(old_entries, engine)
    add_old_accounts(old_entries, engine)
    add_old_edges(old_entries, engine)
    add_old_cases(old_entries, engine)
    add_old_case_suspects(old_entries, engine)


def add_suspect(new_links, session):
    """Adds a suspect's name to the database.

    Args:
         new_links: A dataframe containing source and target link data.
         session: The active session for connecting to the database.

    Returns:
         A unique suspect_id as an integer which is generated when the
         name is added to the 'suspects' table.
    """
    new_sus_name = new_links['Name'][1]
    new_suspect = Suspect(name=new_sus_name)
    session.add(new_suspect)
    session.commit()
    return new_suspect.id


def add_case(new_links, session):
    """Adds the case number to the database.

    Args:
         new_links: A dataframe containing source and target link data.
         session: The active session for connecting to the database.

    Returns:
         A unique case_id as an integer which is generated when the
         case number is added to the 'cases' table.
    """
    case_number = new_links['Case_ID'][1]
    new_case = Case(case_number=case_number)
    session.add(new_case)
    session.commit()
    return new_case.id


def add_case_suspect_link(new_suspect_id, new_case_id, new_links, session):
    """Adds case_suspect table entry to database linking case and suspect tables.

    Args:
        new_suspect_id: The unique identifier for the suspect obtained from
        'add_suspect' function.
        new_case_id: The unique identifier for the case obtained from
        'add_case' function.
        new_links: A dataframe containing source and target link data.
        session: The active session for connecting to the database.
    """
    new_case_suspect = CaseSuspect(case_id=new_case_id,
                                   suspect_id=new_suspect_id,
                                   suspect_case_id=new_links['Suspect_Case_ID'][1])
    session.add(new_case_suspect)
    session.commit()


def get_account_type(account_name):
    """Assigns account type.

    Checks to see whether the word 'facebook' appears in account name,
    and if it does not it assumes the account type is a phone number.

    Args:
        account_name: The unique name of the account, typically a facebook
        account or a phone number.

    Returns:
        An integer id representing the account type.
        """
    match = re.findall(r'facebook', str(account_name))
    if match:
        return 1
    else:
        return 2


def add_new_accounts(new_links, new_suspect_id):
    """Imports source and target accounts into database.

    Args:
        new_links: A dataframe containing source and target link data.
        new_suspect_id: The unique identifier for the suspect obtained from
        'add_suspect' function.
    """
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
    """Adds edges to database.

    Args:
        new_links: A dataframe containing source and target link data.
    """
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


def update_first_degree_links():
    """Calculates first degree links for each suspect.

    Counts the number of relationships (i.e. facebook friends or phone contacts)
    recorded for each suspect and updates the applicable column in the 'suspects'
    table.
    """
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


def update_second_degree_links():
    """Calculates second degree links for each suspect.

    Counts the number of relationships (i.e. facebook friends or phone contacts)
    where the target node is in at least one other relationship with a different
    source node for each suspect and updates the applicable column in the 'suspects'
    table.
    """
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
    """Finds matching entries between two columns and gets adjacent content from
    index column.

    Args:
        df: dataframe used as search space.
        match_col1: name of first column in dataframe to be used in match.
        match_col2: name of second column in dataframe to be used in match.
        index: name of column with variables to be indexed upon finding a match.

    Returns:
         Counts and comma separated lists of indexes for matches between two columns.
    """
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
    """Calculates first or second degree case links for each suspect.

    For each suspect, counts the number of relationships (i.e. facebook friends
    or phone contacts) where the target node (and any of the target node's
    relationships if 'second' degree) has a different case number from the source
    node and updates the applicable column in the 'suspects table.

    Args:
        degree: The links to consider, either first or second degree.

    """
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
        case_links.rename(columns={'count': 'case_count'}, inplace=True)
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


def add_entries(new_links, session=session):
    """Adds new suspect, account, and edge data to the relevant tables.

    Args:
        new_links: A dataframe containing source and target link data.
        session: The active session for connecting to the database.
    """
    new_suspect_id = add_suspect(new_links, session)
    new_case_id = add_case(new_links, session)
    add_case_suspect_link(new_suspect_id, new_case_id, new_links, session)
    add_new_accounts(new_links, new_suspect_id)
    add_new_edges(new_links)


def add_entries_dict(d):
    """Runs 'add_entries' function in a loop.

    Args:
        d: A dictionary of objects belonging to the GSheets class.
    """
    for i in d:
        add_entries(d[i].df)


def update_links(session=session):
    """Update link stats of all types and then close the session.

    Args:
        session: The active session for connecting to the database.
    """
    update_first_degree_links()
    update_second_degree_links()
    update_case_links('first')
    update_case_links('second')
    session.close()
