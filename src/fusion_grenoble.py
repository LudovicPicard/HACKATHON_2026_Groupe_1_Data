import pandas as pd
import os

path = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"
file_name = "Q_38_previous-1950-2024_RR-T-Vent.csv.gz"
file_path = os.path.join(path, file_name)
output_file = os.path.join(path, "dataset_grenoble_complet.csv")

if os.path.exists(file_path):
    print(f"📖 Lecture de {file_name} pour analyse...")
    df = pd.read_csv(file_path, sep=';', compression='gzip', low_memory=False)
    
    # 1. On trouve la station qui a le plus grand nombre de lignes (la plus robuste)
    top_stations = df['NUM_POSTE'].value_counts()
    best_station = top_stations.index[0]
    
    print(f"🔍 Station la plus complète détectée : {best_station} ({top_stations.iloc[0]} jours de relevés)")
    
    # 2. On extrait les données de cette station
    df_grenoble = df[df['NUM_POSTE'] == best_station].copy()
    
    # 3. On ajoute les données récentes si elles existent
    file_latest = os.path.join(path, "Q_38_latest-2025-2026_RR-T-Vent.csv.gz")
    if os.path.exists(file_latest):
        df_lat = pd.read_csv(file_latest, sep=';', compression='gzip', low_memory=False)
        df_lat_filtered = df_lat[df_lat['NUM_POSTE'] == best_station]
        df_grenoble = pd.concat([df_grenoble, df_lat_filtered])
        print(f"✅ Données 2025-2026 ajoutées.")

    # 4. Nettoyage et tri
    df_grenoble['AAAAMMJJ'] = pd.to_datetime(df_grenoble['AAAAMMJJ'], format='%Y%m%d')
    df_grenoble = df_grenoble.sort_values(by='AAAAMMJJ')
    df_grenoble = df_grenoble.dropna(axis=1, how='all')
    
    # 5. Sauvegarde
    df_grenoble.to_csv(output_file, index=False, sep=';')
    
    print("-" * 30)
    print(f"✨ SUCCESS : Station {best_station} extraite !")
    print(f"📊 Total : {len(df_grenoble)} lignes.")
    print(f"📅 Période : du {df_grenoble['AAAAMMJJ'].min().date()} au {df_grenoble['AAAAMMJJ'].max().date()}")

else:
    print(f"❌ Erreur : Le fichier {file_path} est introuvable.")