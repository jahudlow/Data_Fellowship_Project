import numpy as np
import pandas as pd
import pickle
from sklearn.pipeline import Pipeline, FeatureUnion, make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from datetime import date
from time import time


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
        ('features', FeatureUnion(n_jobs=1, transformer_list=[
            ('boolean', Pipeline([
                ('selector', TypeSelector('bool')),
            ])),

            ('numericals', Pipeline([
                ('selector', TypeSelector(np.number)),
                ('scaler', StandardScaler()),
            ]))
        ])),
    ])
    return transformer

def remove_recent(df,cutoff_days):
    '''Eliminates cases more recent than the cutoff date.'''
    today = date.today()
    today.strftime("%m/%d/%Y")
    df['Days'] = (today - df.loc[:, 'interview_date']) / np.timedelta64(1, 'D')
    sub_df = df[(df['Days'] > cutoff_days) | (df['Arrest'] == True)]
    return sub_df

def train_test_val_split(sub_df, te_size = .2, val_size = .1):
    '''Splits dataset into training, testing, and validation sets.'''
    X = (sub_df.drop(columns=['Arrest',
                             'Days',
                             'interview_date',
                             'suspect_id']
                    )
         )
    y = sub_df.Arrest
    val_size = val_size / (1- te_size)
    X_train, X_test, y_train, y_test = train_test_split(X,
                                                        y,
                                                        test_size=te_size)
    X_train, X_validation, y_train, y_validation = train_test_split(X_train,
                                                                    y_train,
                                                                    test_size=val_size)
    return X_train, X_validation, y_train, y_validation

def get_cls_pipe(clf = RandomForestClassifier()):
    '''Builds pipeline with transformer and classifier algorithm.'''
    transformer = build_transformer()
    cls_pipeline = Pipeline([
        ('transformer', transformer),
        ('clf', clf)
    ])
    return cls_pipeline

def pipe_predict(cls_pipeline, X_train, y_train, X_validation):
    '''Make predictions with classifier pipeline.'''
    cls_pipeline.fit(X_train, y_train)
    y_rf = cls_pipeline.predict_proba(X_validation)
    return y_rf

def do_gridsearch(cls_pipeline, X_train, y_train):
    '''Conducts gridsearch cross validation on selected classifer.'''
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

def save_results(best_model, X_validation):
    '''Pickles model and column names and saves them for later use.'''
    filename = 'soc_model.sav'
    pickle.dump(best_model, open(filename, 'wb'))
    xcols = list(X_validation.columns)
    with open('X_cols.txt', 'w') as f:
        for item in xcols:
            f.write("%s\n" % item)

def make_new_predictions(df):
    '''Use existing classifier algorithm on new cases without recalculating best fit.'''
    x_original_cols = [line.rstrip('\n') for line in open('X_cols.txt')]
    X = df[df.columns & x_original_cols]
    soc_model = pickle.load(open('soc_model.sav', 'rb'))
    df['soc'] = soc_model.predict_proba(X)[:, 1]
    return df
