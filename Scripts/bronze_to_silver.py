import pandas as pd
import numpy as np
import os

# 1. Configuration des chemins
PATH_DATA = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Datas"
INPUT_FILE = os.path.join(PATH_DATA, "france_climat_complet.parquet")
OUTPUT_FILE = os.path.join(PATH_DATA, "silver_climate_data.parquet")

def get_france_regions():
    """Retourne le dictionnaire officiel complet sans requêtes HTTP."""
    reg_ara = ["01", "03", "07", "15", "26", "38", "42", "43", "63", "69", "73", "74"]
    reg_bfc = ["21", "25", "39", "58", "70", "71", "89", "90"]
    reg_bre = ["22", "29", "35", "56"]
    reg_cvl = ["18", "28", "36", "37", "41", "45"]
    reg_cor = ["2A", "2B", "20"]
    reg_est = ["08", "10", "51", "52", "54", "55", "57", "67", "68", "88"]
    reg_hdf = ["02", "59", "60", "62", "80"]
    reg_idf = ["75", "77", "78", "91", "92", "93", "94", "95"]
    reg_nor = ["14", "27", "50", "61", "76"]
    reg_naq = ["16", "17", "19", "23", "24", "33", "40", "47", "64", "79", "86", "87"]
    reg_occ = ["09", "11", "12", "30", "31", "32", "34", "46", "48", "65", "66", "81", "82"]
    reg_pdl = ["44", "49", "53", "72", "85"]
    reg_paca = ["04", "05", "06", "13", "83", "84"]
    
    mapping = {}
    for l, name in [(reg_ara, "Auvergne-Rhône-Alpes"), (reg_bfc, "Bourgogne-Franche-Comté"), 
                    (reg_bre, "Bretagne"), (reg_cvl, "Centre-Val de Loire"), (reg_cor, "Corse"), 
                    (reg_est, "Grand Est"), (reg_hdf, "Hauts-de-France"), (reg_idf, "Île-de-France"), 
                    (reg_nor, "Normandie"), (reg_naq, "Nouvelle-Aquitaine"), (reg_occ, "Occitanie"), 
                    (reg_pdl, "Pays de la Loire"), (reg_paca, "Provence-Alpes-Côte d'Azur")]:
        for code in l:
            mapping[code] = name
            
    # Ajout des DOM-TOM et Collectivités territoriales détectés
    dom_tom = {
        "971": "Guadeloupe", "972": "Martinique", "973": "Guyane", 
        "974": "La Réunion", "975": "Saint-Pierre-et-Miquelon",
        "984": "TAAF", "985": "Mayotte", "986": "Wallis-et-Futuna", 
        "987": "Polynésie Française", "988": "Nouvelle-Calédonie"
    }
    mapping.update(dom_tom)
    return mapping

def bronze_to_silver():
    print(f"🚀 Lancement de la transformation Silver sur le fichier national...")
    
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Fichier Bronze introuvable : {INPUT_FILE}")
        return

    try:
        print("📖 Lecture du dataset national Bronze (Parquet)...")
        df = pd.read_parquet(INPUT_FILE)
        print(f"⚙️ Nettoyage de {df.shape[0]:,} lignes en cours...")

        # 1. Harmonisation de la Date
        df['AAAAMMJJ'] = pd.to_datetime(df['AAAAMMJJ'], errors='coerce')
        
        # 2. Conversion numérique
        cols_prioritaires = ['TN', 'TX', 'RR', 'TM']
        for col in cols_prioritaires:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 3. Traitement des Outliers
        df.loc[(df['TX'] > 55) | (df['TX'] < -40), 'TX'] = np.nan
        df.loc[(df['TN'] > 40) | (df['TN'] < -50), 'TN'] = np.nan
        df.loc[df['RR'] < 0, 'RR'] = 0

        # 4. Calcul de TM si NaN
        mask_tm_null = df['TM'].isnull()
        df.loc[mask_tm_null, 'TM'] = (df['TN'] + df['TX']) / 2

        # 5. Enrichissement régional fiable
        print("🗺️ Mapping des régions (Métropole + Outre-Mer)...")
        dept_to_region = get_france_regions()
        
        # Nettoyage des codes départements pour la correspondance (ex: '1' -> '01')
        df['DEPARTEMENT'] = df['DEPARTEMENT'].astype(str).str.strip().str.zfill(2)
        # Cas spécifique pour les codes à 3 chiffres (DOM-TOM) : zfill(2) ne doit pas casser le '971'
        df.loc[df['DEPARTEMENT'].str.len() > 2, 'DEPARTEMENT'] = df['DEPARTEMENT'].str.lstrip('0')

        df['REGION'] = df['DEPARTEMENT'].map(dept_to_region)
        df['REGION'] = df['REGION'].fillna("Inconnu / Autre")

        # 6. Métadonnées
        df['CLEANED_AT'] = pd.Timestamp.now()

        # 7. Suppression des lignes vides
        initial_rows = df.shape[0]
        df = df.dropna(subset=['TN', 'TX', 'RR'], how='all')
        print(f"🧹 Suppression des lignes vides : {initial_rows - df.shape[0]:,} lignes retirées.")

        # 8. Sauvegarde
        print(f"💾 Sauvegarde du fichier Silver final (Parquet)...")
        df.to_parquet(OUTPUT_FILE, index=False)
        
        print(f"✅ LAYER SILVER TERMINÉ AVEC SUCCÈS !")
        print(f"📊 Dimensions finales : {df.shape[0]:,} lignes")
        
        # Petit check pour le jury : vérifier qu'on n'a pas de "Inconnu / Autre"
        unmapped_count = df[df['REGION'] == "Inconnu / Autre"].shape[0]
        if unmapped_count > 0:
            print(f"⚠️ Attention : {unmapped_count} lignes n'ont pas trouvé de région.")
        else:
            print("🎉 Parfait ! 100% des lignes ont été associées à une vraie région.")

    except Exception as e:
        print(f"❌ Erreur critique : {e}")

if __name__ == "__main__":
    bronze_to_silver()