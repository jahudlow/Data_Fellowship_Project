'''
This module does pre-processing, feature selection, builds a pipeline, conducts grid search CV,
and makes predictions using best model.
'''

from datetime import date
from time import time
import numpy as np
import pandas as pd
import pickle
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.preprocessing import StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV


def pre_proc(soc_df):
    """Takes data from DB and removes columns that won't be used."""
    dfcols = list(soc_df.columns)
    dfcols[16] = 'pv_occupation'
    soc_df.columns = dfcols
    soc_df['pb_number'] = soc_df['pb_number'].fillna(0).astype(int)
    soc_df['suspect_id'] = soc_df['cif_number'].str.replace('.', '')
    soc_df['suspect_id'] = soc_df['suspect_id'].str[:-1] + ".PB" + soc_df['pb_number'].map(str)
    soc_df = soc_df.drop_duplicates(subset='suspect_id')

    #Remove columns that won't be used
    drop_cols = [
        'id',
        'date_time_entered_into_system',
        'status',
        'location',
        'date_time_last_updated',
        'staff_name',
        'informant_number',
        'case_notes',
        'pv_signed_form',
        'consent_for_fundraising',
        'social_media',
        'legal_action_taken_filed_against',
        'officer_name',
        'cif_id',
        'person_id',
        'flag_count',
        'main_pv_id',
        'expected_earning',
        'expected_earning_currency',
        'travel_expenses_paid_to_broker_amount',
        'broker_relation',
        'travel_expenses_broker_repaid_amount',
        'form_entered_by_id',
        'source_of_intelligence',
        'date_time_last_updated',
        'incident_date',
        'how_recruited_broker_other',
        'legal_action_taken',
        'legal_action_taken_case_type',
        'appearance',
        'date_visit_police_station',
        'victim_statement_certified_date',
        'purpose_for_leaving_other',
        'relation_to_pv',
        'exploitation_other_value'
    ]
    for x in soc_df.columns:
        if "contact" in x[:] or "_lb" in x[:] or "guardian" in x[:]:
            drop_cols.append(x)
    soc_df = soc_df.drop(columns=drop_cols)
    return soc_df


def organize_dest(soc_df):
    """Clean and organize desitnation data so it is ready for feature union."""
    soc_df['planned_destination'] = soc_df['planned_destination'].str.replace(r'[^\w\s]+', '')
    soc_df['destination_gulf'] = np.where(soc_df['planned_destination'].str.contains(
        'Gulf|Kuwait|Dubai|UAE|Oman|Saudi|Iraq|Qatar|Bahrain'), True, False)
    soc_df['destination_unknown'] = np.where(
        soc_df['planned_destination'].str.contains('know'), True, False)

    dest = ['Nepal',
            'India',
            'Delhi',
            'Gorakhpur',
            'Bihar',
            'Mumbai',
            'Sunauli',
            'Banaras',
            'Kolkata']

    for d in dest:
        soc_df['destination_' + str(d)] = np.where(soc_df['planned_destination'].str.contains(d),
                                                   True, False)

    soc_df.pb_number = soc_df.pb_number.fillna(0)
    soc_df.pb_number = soc_df.pb_number.astype(int)
    return soc_df


def check_match(x):
    """Checks to see whether the first value is equal to or within the second value."""
    return str(x[0]) in list(str(x[1]))


def organize_dtypes(soc_df):
    """Assigns relevant data types to variables."""
    num_features = [
        'number_of_victims',
        'number_of_traffickers',
        'known_broker_years',
        'known_broker_months',
        'married_broker_years',
        'married_broker_months',
        'reported_blue_flags',
        'total_blue_flags',
        'suspected_trafficker_count']

    cat_features = [
        'education',
        'station_id',
        'role',
        'pv_occupation',
        'occupation']

    boolean_features = list(
        set(list(soc_df.columns)) -
        set(num_features) -
        set(cat_features) -
        set(['suspect_id', 'interview_date']))
    soc_df[boolean_features] = soc_df[boolean_features].astype(bool)
    soc_df[num_features] = soc_df[num_features].fillna(0).astype(float)

    for f in cat_features:
        soc_df[f] = soc_df[f].astype("category")

    for cf in cat_features:
        for elem in soc_df[cf].unique():
            soc_df[str(cf) + "_" + str(elem)] = soc_df[str(cf)] == elem
    soc_df.drop(columns=cat_features, inplace=True)
    soc_df.drop(columns=['cif_number', 'Arrest_Date'], inplace=True)
    return soc_df


def en_features(soc_df):
    """Engineer features for selected destinations Person Box variables."""
    soc_df = organize_dest(soc_df)

    PB_fields = [x for x in soc_df.columns if "_pb" in x[:]]
    # 'PB_fields' contains number(s) for the corresponding Person Box if applicable

    for PBf in PB_fields:
        soc_df[PBf + "2"] = soc_df[['pb_number', PBf]].apply(check_match, axis=1)

    soc_df = soc_df.drop(columns=['planned_destination', 'pb_number'])
    soc_df = soc_df.drop(columns=PB_fields)

    soc_df = organize_dtypes(soc_df)

    return soc_df


class TypeSelector(BaseEstimator, TransformerMixin):
    """This is a class for applying transformations based on data type."""

    def __init__(self, dtype):
        self.dtype = dtype

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        assert isinstance(X, pd.DataFrame)
        return X.select_dtypes(include=[self.dtype])


def build_transformer():
    transformer = Pipeline([
        ('features', FeatureUnion(transformer_list=[
            ('boolean', Pipeline([
                ('selector', TypeSelector('bool')),
            ])),

            ('numericals', Pipeline([
                ('selector', TypeSelector(np.number)),
                ('scaler', StandardScaler()),
            ]))
        ], n_jobs=1)),
    ])
    return transformer


def remove_recent(df, cutoff_days):
    """Eliminates cases more recent than the cutoff date."""
    today = date.today()
    today.strftime("%m/%d/%Y")
    df['Days'] = (today - df.loc[:, 'interview_date']) / np.timedelta64(1, 'D')
    sub_df = df[(df['Days'] > cutoff_days) | (df['Arrest'] == True)]
    return sub_df


def train_test_val_split(sub_df, te_size=.2, val_size=.1):
    """Splits dataset into training, testing, and validation sets."""
    X = (sub_df.drop(columns=['Arrest',
                              'Days',
                              'interview_date',
                              'suspect_id']))
    y = sub_df.Arrest
    val_size = val_size / (1 - te_size)
    X_train, X_test, y_train, y_test = train_test_split(X,
                                                        y,
                                                        test_size=te_size)
    X_train, X_validation, y_train, y_validation = train_test_split(X_train,
                                                                    y_train,
                                                                    test_size=val_size)
    return X_train, X_validation, y_train, y_validation


def get_cls_pipe(clf=RandomForestClassifier()):
    """Builds pipeline with transformer and classifier algorithm."""
    transformer = build_transformer()
    cls_pipeline = Pipeline([
        ('transformer', transformer),
        ('clf', clf)
    ])
    return cls_pipeline


def pipe_predict(cls_pipeline, X_train, y_train, X_validation):
    """Make predictions with classifier pipeline."""
    cls_pipeline.fit(X_train, y_train)
    y_rf = cls_pipeline.predict_proba(X_validation)
    return y_rf


def do_gridsearch(cls_pipeline, X_train, y_train):
    """Conducts gridsearch cross validation on selected classifer."""
    search_space = [{'clf': [RandomForestClassifier()],
                     'clf__bootstrap': [False, True],
                     'clf__n_estimators': [10, 100],
                     'clf__max_depth': [5, 10, 20, 30, 40, 50, None],
                     'clf__max_features': [0.5, 0.6, 0.7, 0.8, 1],
                     'clf__class_weight': ["balanced", "balanced_subsample", None]}]
    grid_search = GridSearchCV(cls_pipeline,
                               search_space,
                               cv=5, n_jobs=-1,
                               verbose=1)

    print("Performing grid search...")
    print("parameters:")
    print(search_space)
    t0 = time()
    best_model = grid_search.fit(X_train, y_train)
    print("done in %0.3fs" % (time() - t0))
    print()
    best_parameters = best_model.best_estimator_.get_params()['clf']

    print("Best score: %0.3f" % grid_search.best_score_)
    print("Best parameters set:")
    print(best_parameters)
    return best_model


def save_results(best_model, X_validation, filename):
    """Pickles model and column names and saves them for later use."""
    pickle.dump(best_model, open(filename, 'wb'))
    xcols = list(X_validation.columns)
    with open('X_cols.txt', 'w') as f:
        for item in xcols:
            f.write("%s\n" % item)


def make_new_predictions(df, filename):
    """Use existing classifier algorithm on new cases without recalculating best fit."""
    x_original_cols = [line.rstrip('\n') for line in open('X_cols.txt')]
    X = df[df.columns & x_original_cols]
    soc_model = pickle.load(open(filename, 'rb'))
    df['soc'] = soc_model.predict_proba(X)[:, 1]
    return df
