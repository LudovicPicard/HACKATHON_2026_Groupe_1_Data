import pandas as pd
import os

# 1. Configuration des chemins
path = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"
# Code de la station Toulon (Référence historique de la ville)
station_toulon = 83137001  
output_file = os.path.join(path, "dataset_toulon_complet.csv")

# Liste des fichiers RR-T-Vent pour le Var (83)
files_to_merge = [
    "Q_83_1871-1949_RR-T-Vent.csv.gz",
    "Q_83_previous-1950-2024_RR-T-Vent.csv.gz",
    "Q_83_latest-2025-2026_RR-T-Vent.csv.gz"
]

all_data = []

print("🚀 Début de l'unification des données pour Toulon (Toutes colonnes)...")

for file in files_to_merge:
    file_path = os.path.join(path, file)
    
    if os.path.exists(file_path):
        print(f"📖 Lecture en cours : {file}")
        
        # Lecture du CSV avec compression gzip
        df = pd.read_csv(file_path, sep=';', compression='gzip', low_memory=False)
        
        # 2. Filtrage immédiat sur la station de Toulon
        df_filtered = df[df['NUM_POSTE'] == station_toulon].copy()
        
        # 3. Conversion de la date
        df_filtered['AAAAMMJJ'] = pd.to_datetime(df_filtered['AAAAMMJJ'], format='%Y%m%d')
        
        all_data.append(df_filtered)
    else:
        print(f"⚠️ Fichier manquant : {file}")

if all_data:
    # 4. Fusion
    final_df = pd.concat(all_data, ignore_index=True)

    # 5. Tri chronologique
    final_df = final_df.sort_values(by='AAAAMMJJ')

    # 6. Nettoyage : suppression des colonnes 100% vides
    final_df = final_df.dropna(axis=1, how='all')

    # 7. Sauvegarde
    final_df.to_csv(output_file, index=False, sep=';')

    print("-" * 30)
    print(f"✅ Terminé avec succès pour Toulon !")
    print(f"📁 Fichier : {output_file}")
    print(f"📊 Dimensions : {final_df.shape[0]} lignes x {final_df.shape[1]} colonnes")
    print(f"📅 Couverture : du {final_df['AAAAMMJJ'].min().date()} au {final_df['AAAAMMJJ'].max().date()}")
else:
    print("❌ Aucune donnée n'a pu être chargée pour Toulon.")