import pandas as pd
import numpy as np
import os

# 1. Configuration des chemins
PATH_DATA = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Datas"
INPUT_FILE = os.path.join(PATH_DATA, "silver_climate_data.parquet")

OUTPUT_DEPT = os.path.join(PATH_DATA, "gold_indicators_departement.parquet")
OUTPUT_REGION = os.path.join(PATH_DATA, "gold_indicators_region.parquet")

def calculate_anomalies(group):
    """
    Calcule l'écart par rapport à la normale du GIEC (1961-1990).
    Si le territoire n'a pas cette période, sa propre référence historique globale est calculée.
    """
    ref_period = group[(group['ANNEE'] >= 1961) & (group['ANNEE'] <= 1990)]
    
    if not ref_period.empty:
        ref_tm = ref_period['TM'].mean()
        ref_rr = ref_period['RR'].mean()
    else:
        # Secours dynamique : prend toute la profondeur du signal disponible pour ce groupe précis
        ref_tm = group['TM'].mean()
        ref_rr = group['RR'].mean()
        
    # Transformation des colonnes
    group['ANOMALIE_TM'] = group['TM'] - ref_tm
    group['ANOMALIE_RR_PCT'] = np.where(ref_rr > 0, ((group['RR'] - ref_rr) / ref_rr) * 100, 0)
    return group

def silver_to_gold_national():
    if not os.path.exists(INPUT_FILE):
        print(f"❌ Fichier Silver introuvable : {INPUT_FILE}")
        return

    print("📖 Chargement de la couche nationale Silver (Parquet)...")
    df = pd.read_parquet(INPUT_FILE)
    
    # --- PRÉ-TRAITEMENT ET SÉCURITÉ ---
    mask_nan_tm = df['TM'].isna()
    df.loc[mask_nan_tm, 'TM'] = (df['TN'] + df['TX']) / 2
    df = df.dropna(subset=['TM'])
    
    # Extraction de l'année de manière dynamique
    df['ANNEE'] = df['AAAAMMJJ'].dt.year
    
    min_year = int(df['ANNEE'].min())
    max_year = int(df['ANNEE'].max())
    print(f"📅 Profondeur historique détectée : de {min_year} à {max_year}")

    # --- CALCUL DES INDICATEURS CLIMATIQUES QUOTIDIENS ---
    print("🌡️ Calcul des indices climatiques extrêmes à la maille quotidienne...")
    df['IS_HEATWAVE'] = (df['TX'] > 30).astype(int)        # Forte chaleur
    df['IS_FROST'] = (df['TN'] < 0).astype(int)            # Gel
    df['IS_TROPICAL_NIGHT'] = (df['TN'] >= 20).astype(int)  # Nuit tropicale
    df['IS_DRY_DAY'] = (df['RR'] == 0).astype(int)         # Jour sans pluie

    # =========================================================================
    # 🏢 AXE 1 : AGRÉGATION ANNUELLE PAR DÉPARTEMENT
    # =========================================================================
    print(f"📊 Agrégation annuelle par Département ({df['DEPARTEMENT'].nunique()} territoires)...")
    
    gold_dept = df.groupby(['DEPARTEMENT', 'REGION', 'ANNEE']).agg({
        'TM': 'mean',
        'TN': 'mean',
        'TX': 'mean',
        'RR': 'sum',
        'IS_HEATWAVE': 'sum',
        'IS_FROST': 'sum',
        'IS_TROPICAL_NIGHT': 'sum',
        'IS_DRY_DAY': 'sum'
    }).reset_index()

    print("⚖️ Calcul des anomalies climatiques par Département...")
    # Utilisation de include_groups=False pour supprimer le message de Warning Pandas
    gold_dept = (
        gold_dept.groupby('DEPARTEMENT', group_keys=False)
        .apply(calculate_anomalies, include_groups=True)
    )
    
    # Sauvegarde Axe 1
    gold_dept.to_parquet(OUTPUT_DEPT, index=False)
    print(f"💾 Couche Gold Département enregistrée ({gold_dept.shape[0]:,} lignes).")

    # =========================================================================
    # 🌍 AXE 2 : AGRÉGATION ANNUELLE PAR RÉGION
    # =========================================================================
    print(f"\n📊 Agrégation annuelle par Région ({df['REGION'].nunique()} régions)...")
    
    gold_region = gold_dept.groupby(['REGION', 'ANNEE']).agg({
        'TM': 'mean',
        'TN': 'mean',
        'TX': 'mean',
        'RR': 'mean',
        'IS_HEATWAVE': 'mean',
        'IS_FROST': 'mean',
        'IS_TROPICAL_NIGHT': 'mean',
        'IS_DRY_DAY': 'mean'
    }).reset_index()

    print("⚖️ Calcul des anomalies climatiques par Région...")
    gold_region = (
        gold_region.groupby('REGION', group_keys=False)
        .apply(calculate_anomalies, include_groups=True)
    )
    
    # Sauvegarde Axe 2
    gold_region.to_parquet(OUTPUT_REGION, index=False)
    print(f"💾 Couche Gold Région enregistrée ({gold_region.shape[0]:,} lignes).")

    # --- SYNTHÈSE FINALE POUR LE PITCH ---
    print("\n" + "="*50)
    print("🏆 PIPELINE GOLD NATIONAL TERMINÉ AVEC SUCCÈS !")
    print(f"➡️ Couverture temporelle totale : {min_year} ➡️ {max_year}")
    print(f"➡️ Vue Départementale : {OUTPUT_DEPT}")
    print(f"➡️ Vue Régionale      : {OUTPUT_REGION}")
    print("="*50)

if __name__ == "__main__":
    silver_to_gold_national()