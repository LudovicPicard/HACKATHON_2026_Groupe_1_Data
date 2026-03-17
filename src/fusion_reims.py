import pandas as pd
import os

# 1. Configuration des chemins
path = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"
output_file = os.path.join(path, "dataset_reims_complet.csv")

# Liste des fichiers pour la Marne (51)
files_to_merge = [
    "Q_51_1871-1949_RR-T-Vent.csv.gz",
    "Q_51_previous-1950-2024_RR-T-Vent.csv.gz",
    "Q_51_latest-2025-2026_RR-T-Vent.csv.gz"
]

all_data_list = []
detected_station = None

print("🚀 Analyse et unification des données pour Reims...")

# ÉTAPE PRÉALABLE : Trouver la station la plus dense dans le fichier principal
main_file = os.path.join(path, "Q_51_previous-1950-2024_RR-T-Vent.csv.gz")
if os.path.exists(main_file):
    print("🔍 Recherche de la station historique principale dans le 51...")
    df_temp = pd.read_csv(main_file, sep=';', compression='gzip', low_memory=False, usecols=['NUM_POSTE'])
    detected_station = df_temp['NUM_POSTE'].value_counts().index[0]
    count = df_temp['NUM_POSTE'].value_counts().iloc[0]
    print(f"🎯 Station détectée : {detected_station} ({count} jours trouvés)")
else:
    print("❌ Fichier principal Q_51 manquant, impossible de détecter la station.")

if detected_station:
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
                print(f"   ✅ {len(df_filtered)} lignes ajoutées.")
            else:
                print(f"   ℹ️ Aucune donnée pour {detected_station} dans ce fichier.")
        else:
            print(f"⚠️ Fichier manquant : {file}")

if all_data_list:
    # Fusion
    final_df = pd.concat(all_data_list, ignore_index=True)
    final_df = final_df.sort_values(by='AAAAMMJJ')
    final_df = final_df.dropna(axis=1, how='all')

    # Sauvegarde
    final_df.to_csv(output_file, index=False, sep=';')

    print("-" * 30)
    print(f"✅ Terminé ! Reims est prêt avec la station {detected_station}")
    print(f"📊 Dimensions : {final_df.shape[0]} lignes x {final_df.shape[1]} colonnes")
    print(f"📅 Couverture : du {final_df['AAAAMMJJ'].min().date()} au {final_df['AAAAMMJJ'].max().date()}")
else:
    print("❌ Échec de l'unification pour Reims.")