import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.graphics.tsaplots import plot_acf
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import numpy as np
import pandas as pd

def get_noise_complaint_heatmap(data):
    days_map = {0:'M',1:'T',2:'W',3:'Th',4:'F',5:'Sat',6:'Sun'}

    df = data.sort_values("created_date_trunc").copy()
    df["day_of_week"] = df["created_date_trunc"].dt.dayofweek.replace(days_map)
    df["hour"] = df["created_date_trunc"].dt.hour


    pivot = df.pivot_table(index="day_of_week", columns="hour", values="complaint_count", aggfunc="mean").loc[days_map.values()]

    plt.figure(figsize=(10, 5))
    sns.heatmap(pivot, cbar_kws={"label": "Average complaints"})
    plt.title("Average complaints by day and hour")
    plt.xlabel("Hour of day")
    plt.ylabel("Day of week")
    plt.show()


def highlight_summer_spikes(data):
    plt.figure(figsize=(10,5))
    plt.scatter(data['created_date_trunc'], 
        data['complaint_count'],
        c=data['summer'].map({True: 'red', False: 'lightgray'}),
        alpha=0.7
    )
    plt.title("Noise Complaints - summer highlighted")
    plt.xlabel("Incident created date")
    plt.ylabel("Complaint count")
    plt.show()

def get_nyc_complaint_map(data):
    plt.figure(figsize=(10,8))
    plt.hexbin(data["longitude"], data["latitude"], gridsize=300, cmap="inferno", bins="log")
    plt.colorbar(label="log(count)")
    plt.title("NYC 311 Complaints Density (All years)")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.show()

def get_acf_plot(data):
    autocorr = data.set_index('created_date_trunc').sort_index()[['complaint_count']]

    fig, axes = plt.subplots(1, 1, figsize=(10, 4))
    plot_acf(autocorr.dropna(), lags=30, ax=axes)
    axes.set_title("Autocorrelation of Daily Noise Complaint Counts")
    axes.set_xticks(np.arange(1,31,2))
    plt.show();

def get_ts_proportions(data):
    data.pivot_table(index='year',columns='borough', values='complaints_per_capita').plot(figsize=(10,5), title='Calls per resident over time');

def get_confusion_matrix(y_test, y_pred):

    cm = confusion_matrix(y_test, y_pred)
    disp = ConfusionMatrixDisplay(cm, display_labels=['Normal', 'High Complaint Day'])
    disp.plot(cmap='Reds')
    plt.title("Confusion Matrix - High Noise Complaint Classification")
    plt.show()

def get_log_odds_graph(data):
    feature_names = ['summer',
        'weekend',
        'previous week complaint count',
        'morning',
        'afternoon',
        'night']
    
    coefficients = data.coef_[0]

    importance_df = pd.DataFrame({"feature": feature_names,"coefficient": coefficients, "abs_importance": np.abs(coefficients)}).sort_values("abs_importance", ascending=False)

    plt.figure(figsize=(8, 5))
    plt.barh(importance_df["feature"], importance_df["coefficient"], color="steelblue")
    plt.xlabel("Model coefficient value")
    plt.title("Noise complaint classification - feature importance");
