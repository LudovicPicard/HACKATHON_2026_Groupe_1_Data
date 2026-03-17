import pandas as pd
import os

# 1. Configuration des chemins
path = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"
output_file = os.path.join(path, "dataset_angers_complet.csv")

# Liste des fichiers pour le Maine-et-Loire (49)
files_to_merge = [
    "Q_49_1841-1949_RR-T-Vent.csv.gz", 
    "Q_49_previous-1950-2024_RR-T-Vent.csv.gz",
    "Q_49_latest-2025-2026_RR-T-Vent.csv.gz"
]

all_data_list = []
detected_station = None

print("🚀 Analyse et unification des données pour Angers...")

# ÉTAPE 1 : Détection de la station principale (généralement Angers-Avrillé ou Marcé)
main_file = os.path.join(path, "Q_49_previous-1950-2024_RR-T-Vent.csv.gz")
if os.path.exists(main_file):
    print("🔍 Recherche de la station la plus dense dans le 49...")
    # On lit juste la colonne NUM_POSTE pour aller vite
    df_temp = pd.read_csv(main_file, sep=';', compression='gzip', low_memory=False, usecols=['NUM_POSTE'])
    detected_station = df_temp['NUM_POSTE'].value_counts().index[0]
    count = df_temp['NUM_POSTE'].value_counts().iloc[0]
    print(f"🎯 Station détectée : {detected_station} ({count} jours trouvés)")
else:
    print("❌ Fichier principal Q_49 manquant !")

# ÉTAPE 2 : Unification
if detected_station:
    for file in files_to_merge:
        file_path = os.path.join(path, file)
        
        if os.path.exists(file_path):
            print(f"📖 Lecture : {file}")
            df = pd.read_csv(file_path, sep=';', compression='gzip', low_memory=False)
            
            # Filtrage sur la station détectée
            df_filtered = df[df['NUM_POSTE'] == detected_station].copy()
            
            if not df_filtered.empty:
                # Conversion Date
                df_filtered['AAAAMMJJ'] = pd.to_datetime(df_filtered['AAAAMMJJ'], format='%Y%m%d')
                all_data_list.append(df_filtered)
                print(f"   ✅ {len(df_filtered)} lignes ajoutées.")
            else:
                print(f"   ℹ️ Aucune donnée pour {detected_station} dans ce fichier.")
        else:
            print(f"⚠️ Fichier manquant : {file}")

if all_data_list:
    # Fusion et Tri
    final_df = pd.concat(all_data_list, ignore_index=True)
    final_df = final_df.sort_values(by='AAAAMMJJ')
    final_df = final_df.dropna(axis=1, how='all')

    # Sauvegarde
    final_df.to_csv(output_file, index=False, sep=';')

    print("-" * 30)
    print(f"✅ Terminé ! Angers est prêt (Station {detected_station})")
    print(f"📊 Dimensions : {final_df.shape[0]} lignes")
    print(f"📅 Couverture : du {final_df['AAAAMMJJ'].min().date()} au {final_df['AAAAMMJJ'].max().date()}")
else:
    print("❌ Échec de l'unification pour Angers.")