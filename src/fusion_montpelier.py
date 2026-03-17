import pandas as pd
import os

# 1. Configuration des chemins
path = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"
output_file = os.path.join(path, "dataset_montpellier_complet.csv")

# Liste des fichiers RR-T-Vent pour l'Hérault (34)
files_to_merge = [
    "Q_34_1864-1949_RR-T-Vent.csv.gz",
    "Q_34_previous-1950-2024_RR-T-Vent.csv.gz",
    "Q_34_latest-2025-2026_RR-T-Vent.csv.gz"
]

all_data = []

print("🚀 Analyse de l'Hérault pour identifier la station de Montpellier active...")

# ÉTAPE 1 : Détecter la station la plus robuste sur la période récente
main_file = os.path.join(path, "Q_34_previous-1950-2024_RR-T-Vent.csv.gz")
if os.path.exists(main_file):
    # On scanne les stations ayant le plus de données sur la fin du fichier (après 2020)
    df_scan = pd.read_csv(main_file, sep=';', compression='gzip', low_memory=False, usecols=['NUM_POSTE', 'AAAAMMJJ'])
    recent_data = df_scan[df_scan['AAAAMMJJ'] > 20200101]
    detected_station = recent_data['NUM_POSTE'].value_counts().index[0]
    count = recent_data['NUM_POSTE'].value_counts().iloc[0]
    print(f"🎯 Station détectée : {detected_station} ({count} relevés récents trouvés)")
else:
    print("❌ Fichier Q_34_previous manquant. Utilisation du code par défaut.")
    detected_station = 34172001

# ÉTAPE 2 : Unification avec le bon NUM_POSTE
for file in files_to_merge:
    file_path = os.path.join(path, file)
    
    if os.path.exists(file_path):
        print(f"📖 Lecture : {file}")
        df = pd.read_csv(file_path, sep=';', compression='gzip', low_memory=False)
        
        # Filtrage sur la station détectée
        df_filtered = df[df['NUM_POSTE'] == detected_station].copy()
        
        if not df_filtered.empty:
            df_filtered['AAAAMMJJ'] = pd.to_datetime(df_filtered['AAAAMMJJ'], format='%Y%m%d')
            all_data.append(df_filtered)
            print(f"   ✅ {len(df_filtered)} lignes extraites.")
        else:
            print(f"   ℹ️ Aucune donnée pour {detected_station} dans ce fichier.")
    else:
        print(f"⚠️ Fichier manquant : {file}")

if all_data:
    # 4. Fusion et Nettoyage
    final_df = pd.concat(all_data, ignore_index=True)
    final_df = final_df.sort_values(by='AAAAMMJJ')
    final_df = final_df.dropna(axis=1, how='all')

    # 5. Sauvegarde
    final_df.to_csv(output_file, index=False, sep=';')

    print("-" * 30)
    print(f"✅ Terminé avec succès pour Montpellier !")
    print(f"📁 Fichier : {output_file}")
    print(f"📊 Dimensions : {final_df.shape[0]} lignes")
    print(f"📅 Couverture : du {final_df['AAAAMMJJ'].min().date()} au {final_df['AAAAMMJJ'].max().date()}")
else:
    print("❌ Aucune donnée n'a pu être chargée.")