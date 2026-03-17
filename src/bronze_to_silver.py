import pandas as pd
import numpy as np
import os

# Configuration des chemins
PATH_DATA = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"

VILLES_CONFIG = {
    "Rennes": "dataset_rennes_complet.csv",
    "Bordeaux": "dataset_bordeaux_complet.csv",
    "Nantes": "dataset_nantes_complet.csv",
    "Marseille": "dataset_marseille_complet.csv",
    "Lyon": "dataset_lyon_complet.csv",
    "Paris": "dataset_paris_complet.csv",
    "Toulouse": "dataset_toulouse_complet.csv",
    "Nice": "dataset_nice_complet.csv",
    "Montpellier": "dataset_montpellier_complet.csv",
    "Strasbourg": "dataset_strasbourg_complet.csv",
    "Lille": "dataset_lille_complet.csv",
    "Reims": "dataset_reims_complet.csv",
    "Saint-Etienne": "dataset_st_etienne_complet.csv",
    "Le Havre": "dataset_le_havre_complet.csv",
    "Toulon": "dataset_toulon_complet.csv",
    "Grenoble": "dataset_grenoble_complet.csv",
    "Dijon": "dataset_dijon_complet.csv",
    "Angers": "dataset_angers_complet.csv",
    "Nîmes": "dataset_nimes_complet.csv",
    "Brest": "dataset_brest_complet.csv"
}

def bronze_to_silver():
    silver_data = []
    print(f"🚀 Lancement de la transformation pour {len(VILLES_CONFIG)} villes...")

    for ville, filename in VILLES_CONFIG.items():
        file_path = os.path.join(PATH_DATA, filename)
        
        if not os.path.exists(file_path):
            print(f"⚠️ Fichier absent : {ville}")
            continue
            
        try:
            print(f"⚙️ Nettoyage : {ville}...")
            df = pd.read_csv(file_path, sep=';', low_memory=False)
            
            if df.empty:
                print(f"❗ {ville} est vide, ignoré.")
                continue

            # Harmonisation Date
            df['AAAAMMJJ'] = pd.to_datetime(df['AAAAMMJJ'], errors='coerce')
            
            # Conversion Numérique
            cols_prioritaires = ['TN', 'TX', 'RR', 'TM']
            for col in cols_prioritaires:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                else:
                    df[col] = np.nan # On crée la colonne vide si elle manque

            # Nettoyage Outliers
            df.loc[(df['TX'] > 55) | (df['TX'] < -40), 'TX'] = np.nan
            df.loc[(df['TN'] > 40) | (df['TN'] < -50), 'TN'] = np.nan
            df.loc[df['RR'] < 0, 'RR'] = 0

            # Calcul de TM sécurisé (ne plante plus si TN ou TX est NaN)
            mask_tm_null = df['TM'].isnull()
            df.loc[mask_tm_null, 'TM'] = (df['TN'] + df['TX']) / 2

            # Métadonnées
            df['VILLE'] = ville
            df['CLEANED_AT'] = pd.Timestamp.now()
            silver_data.append(df)

        except Exception as e:
            print(f"❌ Erreur critique sur {ville} : {e}")

    if silver_data:
        master_silver = pd.concat(silver_data, ignore_index=True)
        # On ne garde que les lignes où on a au moins une info météo
        master_silver = master_silver.dropna(subset=['TN', 'TX', 'RR'], how='all')
        
        output_path = os.path.join(PATH_DATA, "silver_climate_data.parquet")
        master_silver.to_parquet(output_path, index=False)
        print(f"✅ SILVER terminé ! ({master_silver.shape[0]} lignes)")
    else:
        print("❌ Aucune donnée traitée.")

if __name__ == "__main__":
    bronze_to_silver()