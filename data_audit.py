import os
import pandas as pd
import glob

OUTPUT_DIR = "data_climat"
CITY_MAP = {
    "MARIGNANE": "Marseille", "MARSEILLE": "Marseille",
    "LYON-BRON": "Lyon", "LYON": "Lyon",
    "BORDEAUX-MERIGNAC": "Bordeaux", "BORDEAUX": "Bordeaux",
    "NANTES-BOUGUENAIS": "Nantes", "NANTES": "Nantes",
    "PARIS-MONTSOURIS": "Paris", "PARIS": "Paris",
    "RENNES-ST JACQUES": "Rennes", "RENNES": "Rennes"
}

def audit():
    print("--- DATA AUDIT ---")
    search_paths = ["dataset_*_complet.csv", os.path.join(OUTPUT_DIR, "dataset_*_complet.csv"), os.path.join(OUTPUT_DIR, "observations_quotidiennes.csv")]
    
    all_files = []
    for p in search_paths:
        all_files.extend(glob.glob(p))
    
    report = []
    for f in all_files:
        try:
            sep = ";"
            df = pd.read_csv(f, sep=sep, nrows=1000000)
            
            # Identify columns
            date_col = 'AAAAMMJJ' if 'AAAAMMJJ' in df.columns else 'DATE'
            city_col = 'NOM_USUEL' if 'NOM_USUEL' in df.columns else 'ville'
            
            if date_col not in df.columns or city_col not in df.columns:
                # Try reading with comma just in case
                df = pd.read_csv(f, sep=",", nrows=1000000)
                date_col = 'AAAAMMJJ' if 'AAAAMMJJ' in df.columns else 'DATE'
                city_col = 'NOM_USUEL' if 'NOM_USUEL' in df.columns else 'ville'
                if date_col not in df.columns:
                    print(f"Skipping {f}: missing date col")
                    continue
                
            df['DATE_DT'] = pd.to_datetime(df[date_col], errors='coerce')
            
            for city_raw in df[city_col].unique():
                city_name = CITY_MAP.get(str(city_raw).strip().upper(), str(city_raw).strip().capitalize())
                city_data = df[df[city_col] == city_raw]
                report.append({
                    "file": f,
                    "raw_city": city_raw,
                    "mapped_city": city_name,
                    "rows": len(city_data),
                    "min_date": city_data['DATE_DT'].min(),
                    "max_date": city_data['DATE_DT'].max()
                })
        except Exception as e:
            print(f"Error on {f}: {e}")
            
    df_report = pd.DataFrame(report)
    if not df_report.empty:
        print(df_report.sort_values(['mapped_city', 'min_date']).to_string())
    else:
        print("No data found in audit.")

if __name__ == "__main__":
    audit()
