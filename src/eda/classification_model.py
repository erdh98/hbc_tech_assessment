import pandas as pd
import pickle
from sklearn.linear_model import LogisticRegression

def set_up_features(noise_ts_granular, noise_ts_year_month_all):
    """ Prepares features for the predictive model."""
    def categorical_time_of_day(hour):
        if 4 <= hour < 12:
            return 'morning'
        elif 12 <= hour < 17:
            return 'afternoon'
        else:
            return 'night'

    prev_week_complaints = noise_ts_year_month_all.set_index('created_date_trunc').sort_index()['complaint_count'].shift(7).rename('prev_week_complaints').to_frame()

    model_data = noise_ts_granular.copy().drop(columns=['complaints_per_capita','average_population'])

    model_data['created_date_mdy'] = model_data['created_date_trunc'].dt.normalize()
    model_data['time_of_day'] = model_data['created_date_trunc'].dt.hour.apply(categorical_time_of_day)
    model_data['summer'] = model_data['created_date_trunc'].dt.month.isin([6,7,8,9]).astype(int)
    model_data['weekend'] = model_data['created_date_trunc'].dt.dayofweek.isin([5,6]).astype(int)
    model_data = model_data.merge(prev_week_complaints, left_on='created_date_mdy', right_on='created_date_trunc')
    model_data = pd.get_dummies(model_data, columns=['time_of_day'])
    model_data['year'] = model_data['created_date_trunc'].dt.year

    return model_data

def define_classes(model_data):
    complaint_thresholds_by_year = model_data.groupby('year')['complaint_count'].quantile(0.7).rename('complaint_cutoff').to_frame()
    model_data = model_data.merge(complaint_thresholds_by_year, on='year')
    model_data['high_complaint_period'] = (model_data['complaint_count'] > model_data['complaint_cutoff']).astype(int)
    return model_data

def get_train_and_test_data(model_data, load_from_pickle=True):
    if not load_from_pickle:
        model_data = model_data.dropna().set_index('created_date_trunc').sort_index()
        x = model_data[['summer', 'weekend', 'prev_week_complaints', 'time_of_day_morning','time_of_day_afternoon','time_of_day_night']].copy()
        y = model_data['high_complaint_period']

        split_point = int(len(model_data) * 0.7)
        x_train, x_test = x.iloc[:split_point], x.iloc[split_point:]
        y_train, y_test = y.iloc[:split_point], y.iloc[split_point:]
    else:
        print("Loading model and data from pickle")
        with open("model_pickle.pkl", "rb") as f:
            loaded_model = pickle.load(f)
        model = loaded_model["model"]
        x_train, x_test = loaded_model["x_data"][0], loaded_model["x_data"][1]
        y_train, y_test = loaded_model["y_data"][0], loaded_model["y_data"][1]

    return x_train, x_test, y_train, y_test

def fit_model_and_predict(x_train, y_train, x_test, load_from_pickle=True):
    if not load_from_pickle:
        model = LogisticRegression()
        model.fit(x_train, y_train)

        y_pred = model.predict(x_test)
        y_probs = model.predict_proba(x_test)[:, 1]
        return model, y_pred, y_probs
    else:
        print("Loading model and data from pickle")
        with open("model_pickle.pkl", "rb") as f:
            loaded_model = pickle.load(f)
        model = loaded_model["model"]
        y_pred = model.predict(x_test)
        y_probs = model.predict_proba(x_test)[:, 1]
        return model, y_pred, y_probs