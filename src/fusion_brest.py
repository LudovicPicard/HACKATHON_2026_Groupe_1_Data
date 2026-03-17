import pandas as pd
import os

# 1. Configuration des chemins
path = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"
output_file = os.path.join(path, "dataset_brest_complet.csv")

# Liste des fichiers pour le Finistère (29)
files_to_merge = [
    "Q_29_1855-1949_RR-T-Vent.csv.gz", 
    "Q_29_previous-1950-2024_RR-T-Vent.csv.gz",
    "Q_29_latest-2025-2026_RR-T-Vent.csv.gz"
]

all_data_list = []

print("🚀 Analyse du Finistère pour trouver la station principale...")

# ÉTAPE 1 : Trouver la station qui a le plus de données entre 1950 et 2024
main_file = os.path.join(path, "Q_29_previous-1950-2024_RR-T-Vent.csv.gz")
if os.path.exists(main_file):
    df_check = pd.read_csv(main_file, sep=';', compression='gzip', low_memory=False, usecols=['NUM_POSTE'])
    top_stations = df_check['NUM_POSTE'].value_counts()
    detected_station = top_stations.index[0]
    print(f"🎯 Station détectée comme principale : {detected_station} ({top_stations.iloc[0]} jours)")
else:
    print("❌ Fichier de référence manquant !")
    detected_station = 29019001 # On garde le code par défaut au cas où

# ÉTAPE 2 : Unification
for file in files_to_merge:
    file_path = os.path.join(path, file)
    
    if os.path.exists(file_path):
        print(f"📖 Lecture : {file}")
        df = pd.read_csv(file_path, sep=';', compression='gzip', low_memory=False)
        
        # Filtrage sur la station détectée
        df_filtered = df[df['NUM_POSTE'] == detected_station].copy()
        
        if not df_filtered.empty:
            df_filtered['AAAAMMJJ'] = pd.to_datetime(df_filtered['AAAAMMJJ'], format='%Y%m%d')
            all_data_list.append(df_filtered)
            print(f"   ✅ {len(df_filtered)} lignes ajoutées pour cette période.")
        else:
            print(f"   ℹ️ Aucune donnée pour {detected_station} ici.")

if all_data_list:
    final_df = pd.concat(all_data_list, ignore_index=True).sort_values('AAAAMMJJ')
    final_df = final_df.dropna(axis=1, how='all')
    final_df.to_csv(output_file, index=False, sep=';')

    print("-" * 30)
    print(f"✨ SUCCESS pour Brest (Station {detected_station}) !")
    print(f"📅 Couverture : du {final_df['AAAAMMJJ'].min().date()} au {final_df['AAAAMMJJ'].max().date()}")
else:
    print("❌ Rien n'a pu être extrait.")