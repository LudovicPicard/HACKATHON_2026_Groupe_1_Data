import pandas as pd
import os
import glob

# Configuration
INPUT_FILE = "data_climat/observations_quotidiennes.csv"
OUTPUT_DIR = "data_climat"
SILVER_FILE = os.path.join(OUTPUT_DIR, "silver_observations.parquet")
GOLD_FILE = os.path.join(OUTPUT_DIR, "gold_climate_indicators.parquet")

CITY_MAP = {
    "MARIGNANE": "Marseille",
    "MARSEILLE": "Marseille",
    "LYON-BRON": "Lyon",
    "LYON": "Lyon",
    "BORDEAUX-MERIGNAC": "Bordeaux",
    "BORDEAUX-PAULIN": "Bordeaux",
    "BORDEAUX": "Bordeaux",
    "NANTES-BOUGUENAIS": "Nantes",
    "NANTES": "Nantes",
    "PARIS-MONTSOURIS": "Paris",
    "PARIS": "Paris",
    "RENNES-ST JACQUES": "Rennes",
    "RENNES": "Rennes"
}

def transform_to_silver():
    print("--- Transformation Silver ---")
    print(f"CWD: {os.getcwd()}")
    print(f"Files in CWD: {os.listdir('.')}")
    all_dfs = []
    
    # Manually search for archives in current dir and data_climat
    historical_files = []
    current_dir = os.path.dirname(os.path.abspath(__file__))
    dirs_to_search = [current_dir, os.path.join(current_dir, OUTPUT_DIR)]
    
    for d in dirs_to_search:
        if os.path.exists(d):
            for f in os.listdir(d):
                if f.startswith("dataset_") and f.endswith("_complet.csv"):
                    historical_files.append(os.path.join(d, f))
    
    # Remove duplicates in file list
    historical_files = list(set(historical_files))
    print(f"Archives à charger : {historical_files}")
    
    # 1. Load API observations (updates) FIRST - They are very complete from 1945 onwards
    if os.path.exists(INPUT_FILE):
        print(f"Chargement des mises à jour API : {INPUT_FILE}")
        df_obs = pd.read_csv(INPUT_FILE, sep=";")
        if "DATE" in df_obs.columns:
            df_obs["DATE"] = pd.to_datetime(df_obs["DATE"])
            # Standardize city names
            df_obs['ville'] = df_obs['ville'].str.strip().str.upper().apply(lambda x: CITY_MAP.get(x, x.capitalize()))
            print(f"File: {INPUT_FILE} | Rows: {len(df_obs)} | Cities: {df_obs['ville'].unique()}")
            all_dfs.append(df_obs)

    # 2. Load historical datasets (archives) SECOND - To fill in pre-1945 history
    for filepath in historical_files:
        print(f"Chargement de l'archive historique : {filepath}")
        try:
            df_hist = pd.read_csv(filepath, sep=";")
            mapping = {
                'AAAAMMJJ': 'DATE',
                'NOM_USUEL': 'ville',
                'TX': 'TX', 'TN': 'TN', 'TM': 'TM', 'RR': 'RR'
            }
            existing_cols = [c for c in mapping.keys() if c in df_hist.columns]
            df_hist = df_hist[existing_cols].rename(columns={c: mapping[c] for c in existing_cols})
            df_hist["DATE"] = pd.to_datetime(df_hist["DATE"])
            
            # Normalize city names using the map
            df_hist['ville'] = df_hist['ville'].str.strip().str.upper().apply(lambda x: CITY_MAP.get(x, x.split('-')[0].capitalize()))
            
            # Fallback for missing TM: compute from TX and TN if possible
            if 'TM' in df_hist.columns and 'TX' in df_hist.columns and 'TN' in df_hist.columns:
                mask = df_hist['TM'].isna() & df_hist['TX'].notna() & df_hist['TN'].notna()
                df_hist.loc[mask, 'TM'] = (df_hist.loc[mask, 'TX'] + df_hist.loc[mask, 'TN']) / 2
            
            all_dfs.append(df_hist)
        except Exception as e:
            print(f"Erreur sur {filepath} : {e}")
    
    if not all_dfs:
        print("Erreur : Aucun fichier de données trouvé.")
        return None
    
    # Combine and drop duplicates
    print(f"Nombre de dataframes à concaténer : {len(all_dfs)}")
    df = pd.concat(all_dfs, ignore_index=True)
    
    # Crucial: standardize names again before deduplication
    df['ville'] = df['ville'].str.strip().str.capitalize()
    
    print(f"Total rows before deduplication: {len(df)}")
    # Sort to keep the most complete data if possible? 
    # Actually, keep the one from archives if overlap, then updates.
    # Archive files are at the beginning of all_dfs so 'first' keeps them.
    df = df.drop_duplicates(subset=['ville', 'DATE'], keep='first')
    print(f"Total rows after deduplication: {len(df)}")
    print(f"Final cities in Silver: {df['ville'].unique()}")
    
    # Sort for consistency
    df = df.sort_values(['ville', 'DATE'])
    
    # Sauvegarde Parquet
    df.to_parquet(SILVER_FILE, index=False)
    print(f"Silver enregistré : {SILVER_FILE} ({len(df)} lignes, {df['ville'].nunique()} villes)")
    return df

def transform_to_gold(df_silver):
    if df_silver is None:
        return
    
    print(f"--- Transformation Gold ---")
    
    # Extraction de l'année
    df = df_silver.copy()
    df["ANNEE"] = df["DATE"].dt.year
    
    # 1. Indicateurs de base et nouveaux
    # Jours de canicule (TX > 35), Nuits tropicales (TN > 20), Gel (TN < 0), Saison chaude (TX > 25)
    df['IS_CANICULE'] = (df['TX'] > 35).astype(int)
    df['IS_TROPICAL_NIGHT'] = (df['TN'] > 20).astype(int)
    df['IS_FROST'] = (df['TN'] < 0).astype(int)
    df['IS_HOT_SEASON'] = (df['TX'] > 25).astype(int)
    
    # 7. Séquences sèches max (Nombre de jours d'affilés RR = 0)
    def max_dry_spell(series):
        is_dry = (series == 0).astype(int)
        # Groupe par l'apparition d'un jour de pluie (cumsum sur !dry)
        # Mais plus simple: diff().ne(0).cumsum() sur is_dry
        groups = is_dry.diff().ne(0).cumsum()
        dry_spells = is_dry.groupby(groups).sum()
        return dry_spells.max() if not dry_spells.empty else 0

    gold = df.groupby(["ville", "ANNEE"]).agg({
        "TM": "mean",
        "TX": "max",
        "RR": "sum"
    }).reset_index()
    
    # Fusion des indicateurs de comptage
    counts = df.groupby(["ville", "ANNEE"]).agg({
        "IS_CANICULE": "sum",
        "IS_TROPICAL_NIGHT": "sum",
        "IS_FROST": "sum",
        "IS_HOT_SEASON": "sum",
    }).reset_index()
    
    dry_spell_data = df.groupby(["ville", "ANNEE"])["RR"].apply(max_dry_spell).reset_index(name="DRY_SPELL_MAX")
    
    gold = gold.merge(counts, on=["ville", "ANNEE"], how="left")
    gold = gold.merge(dry_spell_data, on=["ville", "ANNEE"], how="left")
    
    # 8. Emission de CO2 par personne en France (Valeurs approximatives CITEPA/INSEE)
    # On ajoute une colonne de référence (constante ou décroissante)
    co2_base = {
        2000: 6.8, 2005: 6.5, 2010: 6.2, 2015: 5.5, 2018: 5.0, 
        2020: 4.8, 2021: 4.9, 2022: 4.6, 2023: 4.1, 2024: 4.0, 2025: 3.9, 2026: 3.8
    }
    gold['CO2_FRANCE'] = gold['ANNEE'].map(lambda x: co2_base.get(x, (6.0 if x < 2000 else 3.5)))

    # Renommage
    gold = gold.rename(columns={
        "ville": "VILLE",
        "TX": "TX_MAX",
        "RR": "RR_TOTAL",
        "IS_CANICULE": "DAYS_CANICULE",
        "IS_TROPICAL_NIGHT": "NIGHTS_TROPICAL",
        "IS_FROST": "DAYS_FROST",
        "IS_HOT_SEASON": "DAYS_HOT_SEASON"
    })
    
    # Calcul des anomalies
    complete_years = gold[gold["ANNEE"] < 2026]
    baselines = complete_years.groupby("VILLE")["TM"].mean().to_dict()
    
    gold["ANOMALIE_TM"] = gold.apply(
        lambda row: row["TM"] - baselines.get(row["VILLE"], row["TM"]), 
        axis=1
    )
    
    # On garde toutes les années, même 2026 pour le temps réel
    # (Note: Anomaly baseline used years < 2026 above, which is correct)
    
    # Sauvegarde
    gold.to_parquet(GOLD_FILE, index=False)
    print(f"Gold enregistré : {GOLD_FILE} ({len(gold)} lignes)")
    print(gold.head())

if __name__ == "__main__":
    df_silver = transform_to_silver()
    transform_to_gold(df_silver)
