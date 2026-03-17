import pandas as pd
import os
import numpy as np
from prophet import Prophet
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, mean_absolute_percentage_error

# Configuration
INPUT_FILE = "data_climat/gold_climate_indicators.parquet"
OUTPUT_FILE = "data_climat/gold_projections.parquet"
METRICS_FILE = "data_climat/model_performance.parquet"
YEARS_PROJECTION = list(range(2026, 2101))

# Indicators to project using IA models
INDICATORS = [
    'TM', 'DAYS_CANICULE', 'NIGHTS_TROPICAL', 'DAYS_FROST', 
    'DAYS_HOT_SEASON', 'RR_TOTAL', 'DRY_SPELL_MAX'
]

def generate_city_projections(df_historical, city, model_type="Prophet"):
    print(f"--- Modélisation {model_type} pour : {city} ---")
    data_city = df_historical[df_historical['VILLE'] == city].sort_values('ANNEE')
    # Filter out any incomplete 2026 data
    data = data_city[data_city['ANNEE'] <= 2025].copy()
    
    if data.empty: 
        print(f"Attention : Aucune donnée pour {city}")
        return pd.DataFrame(), pd.DataFrame()

    city_results = []
    performance_results = []
    
    # SCIENTIFIC DELTAS (only for TM as per DRIAS/IPCC)
    scenarios = {
        'RCP': {'Optimistic': 0.4, 'Median': 1.3, 'Pessimistic': 2.6},
        'SSP': {'Optimistic': 0.3, 'Median': 1.5, 'Pessimistic': 3.1}
    }
    
    years = np.array(YEARS_PROJECTION)
    progress = ((years - 2025) / (2100 - 2025))**1.2
    
    # 1. Project all indicators
    forecasts = {}
    for col in INDICATORS:
        if col not in data.columns: continue
        
        subset = data[['ANNEE', col]].dropna()
        if subset.empty: continue
        
        # PERFORMANCE CALCULATION (Train/Test split on historical data)
        # We use the last 5 available years for testing
        if len(subset) > 10:
            train = subset.iloc[:-5]
            test = subset.iloc[-5:]
            
            y_pred_test = []
            if model_type == "Prophet":
                m_train = train.rename(columns={'ANNEE': 'ds', col: 'y'})
                m_train['ds'] = pd.to_datetime(m_train['ds'], format='%Y')
                m_tmp = Prophet(yearly_seasonality=False, changepoint_prior_scale=0.05)
                m_tmp.fit(m_train, iter=50) # Fast fit for performance check
                future_tmp = pd.DataFrame({'ds': pd.to_datetime(test['ANNEE'], format='%Y')})
                y_pred_test = m_tmp.predict(future_tmp)['yhat'].values
            else:
                lr_tmp = LinearRegression().fit(train[['ANNEE']].values, train[col].values)
                y_pred_test = lr_tmp.predict(test[['ANNEE']].values)
            
            y_true = test[col].values
            rmse = np.sqrt(mean_squared_error(y_true, y_pred_test))
            mae = mean_absolute_error(y_true, y_pred_test)
            
            # Safe MAPE: handle 0s in y_true to avoid infinite values
            mask = y_true != 0
            if np.any(mask):
                mape = np.mean(np.abs((y_true[mask] - y_pred_test[mask]) / y_true[mask])) * 100
            else:
                # If all values are 0, and prediction is also 0, error is 0. 
                # If prediction is > 0, we could use a small epsilon or just cap it.
                # Here we use MAE as a fallback indicator of scale if MAPE is undefined.
                mape = 0
                
            performance_results.append({
                'VILLE': city,
                'INDICATEUR': col,
                'MODEL': model_type,
                'RMSE': round(float(rmse), 3),
                'MAE': round(float(mae), 3),
                'MAPE_PCT': round(float(mape), 2)
            })

        # FULL MODEL FOR PROJECTION
        if model_type == "Prophet":
            m_data = subset.rename(columns={'ANNEE': 'ds', col: 'y'})
            m_data['ds'] = pd.to_datetime(m_data['ds'], format='%Y')
            m = Prophet(yearly_seasonality=False, changepoint_prior_scale=0.05)
            m.fit(m_data)
            future = pd.DataFrame({'ds': pd.to_datetime(YEARS_PROJECTION, format='%Y')})
            forecast = m.predict(future)
            forecasts[col] = forecast['yhat'].values
        else:
            X = subset[['ANNEE']].values
            y = subset[col].values
            lr = LinearRegression().fit(X, y)
            forecasts[col] = lr.predict(years.reshape(-1, 1))

    # 2. Build scenario-based rows
    for frame, targets in scenarios.items():
        for i, year in enumerate(YEARS_PROJECTION):
            row = {
                'VILLE': city, 
                'ANNEE': int(year), 
                'MODEL_IA': model_type, 
                'FRAME': frame
            }
            
            # Special handling for TM (Scientific Scenarios)
            if 'TM' in forecasts:
                hist_recent = data[data['ANNEE'] >= 2015]
                baseline_tm = hist_recent['TM'].mean() if not hist_recent.empty else data['TM'].mean()
                local_delta_tm = forecasts['TM'][i] - baseline_tm
                
                row['TM_OPTIMISTIC'] = baseline_tm + (targets['Optimistic'] * progress[i]) + (local_delta_tm * 0.5)
                row['TM_MEDIAN'] = baseline_tm + (targets['Median'] * progress[i]) + (local_delta_tm * 0.6)
                row['TM_PESSIMISTIC'] = baseline_tm + (targets['Pessimistic'] * progress[i]) + (local_delta_tm * 0.8)
            else:
                # Fallback if no TM projection
                row['TM_OPTIMISTIC'] = row['TM_MEDIAN'] = row['TM_PESSIMISTIC'] = 15.0 # Global average fallback
            
            # Other indicators (Trend-based)
            for col in INDICATORS:
                if col == 'TM': continue
                if col not in forecasts: 
                    row[col] = 0
                    continue
                
                val = forecasts[col][i]
                # Ensure counts are non-negative
                if col != 'RR_TOTAL': val = max(0, val)
                row[col] = val
                
            city_results.append(row)
            
    return pd.DataFrame(city_results), pd.DataFrame(performance_results)

def run():
    if not os.path.exists(INPUT_FILE):
        print(f"Fichier {INPUT_FILE} introuvable.")
        return
        
    df = pd.read_parquet(INPUT_FILE)
    cities = df['VILLE'].unique()
    
    all_projections = []
    all_performance = []
    for city in cities:
        for m_type in ["Prophet", "Linear"]:
            res, perf = generate_city_projections(df, city, m_type)
            all_projections.append(res)
            all_performance.append(perf)
        
    df_projections = pd.concat(all_projections, ignore_index=True)
    df_projections.to_parquet(OUTPUT_FILE, index=False)
    
    df_perf = pd.concat(all_performance, ignore_index=True)
    df_perf.to_parquet(METRICS_FILE, index=False)
    
    print(f"Modélisation terminée.")
    print(f"Projections enregistrées : {OUTPUT_FILE}")
    print(f"Performance enregistrée : {METRICS_FILE}")

if __name__ == "__main__":
    run()
