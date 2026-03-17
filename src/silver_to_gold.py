import pandas as pd
import numpy as np
import os

PATH_DATA = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Data"
INPUT_FILE = os.path.join(PATH_DATA, "silver_climate_data.parquet")
OUTPUT_FILE = os.path.join(PATH_DATA, "gold_climate_indicators.parquet")

def silver_to_gold_expert():
    if not os.path.exists(INPUT_FILE):
        print("❌ Fichier Silver introuvable. Lancez d'abord bronze_to_silver.py")
        return

    print("📖 Chargement de la couche Silver...")
    df = pd.read_parquet(INPUT_FILE)
    
    # --- PRÉ-TRAITEMENT ---
    # Sécurité : Si TM est NaN, on la recalcule (au cas où le silver aurait raté une ligne)
    mask_nan_tm = df['TM'].isna()
    df.loc[mask_nan_tm, 'TM'] = (df['TN'] + df['TX']) / 2
    
    # Nettoyage final avant agrégation
    df = df.dropna(subset=['TM'])
    df['ANNEE'] = df['AAAAMMJJ'].dt.year

    # --- CALCUL DES INDICATEURS QUOTIDIENS ---
    df['IS_HEATWAVE'] = (df['TX'] > 30).astype(int)       # Jours de forte chaleur
    df['IS_FROST'] = (df['TN'] < 0).astype(int)           # Jours de gel
    df['IS_TROPICAL_NIGHT'] = (df['TN'] >= 20).astype(int) # Nuits tropicales (confort nocturne)
    df['IS_DRY_DAY'] = (df['RR'] == 0).astype(int)        # Jours sans pluie

    print(f"📊 Agrégation annuelle pour {df['VILLE'].nunique()} villes...")
    
    # --- AGRÉGATION ANNUELLE ---
    gold_df = df.groupby(['VILLE', 'ANNEE']).agg({
        'TM': 'mean',              # Température moyenne annuelle
        'TN': 'mean',              # Moyenne des minimales (pour voir l'évolution des nuits)
        'TX': 'mean',              # Moyenne des maximales
        'RR': 'sum',               # Cumul annuel de pluie
        'IS_HEATWAVE': 'sum',      # Nombre de jours > 30°C
        'IS_FROST': 'sum',         # Nombre de jours de gel
        'IS_TROPICAL_NIGHT': 'sum',# Nombre de nuits tropicales
        'IS_DRY_DAY': 'sum'        # Nombre de jours sans pluie
    }).reset_index()

    # --- CALCUL DES ANOMALIES (Réf: 1961-1990) ---
    print("🌡️ Calcul des anomalies thermiques par ville...")
    
    def calculate_anomalies(group):
        # On définit la période de référence historique
        ref_period = group[(group['ANNEE'] >= 1961) & (group['ANNEE'] <= 1990)]
        
        if not ref_period.empty:
            ref_tm = ref_period['TM'].mean()
            ref_rr = ref_period['RR'].mean()
        else:
            # Si pas de données 61-90, on prend la moyenne de toute la série pour la ville
            ref_tm = group['TM'].mean()
            ref_rr = group['RR'].mean()
            
        group['ANOMALIE_TM'] = group['TM'] - ref_tm
        group['ANOMALIE_RR_PCT'] = ((group['RR'] - ref_rr) / ref_rr) * 100
        return group

    gold_df = gold_df.groupby('VILLE', group_keys=False).apply(calculate_anomalies)

    # --- SAUVEGARDE FINALE ---
    gold_df.to_parquet(OUTPUT_FILE, index=False)
    
    print("-" * 30)
    print(f"✅ COUCHE GOLD GÉNÉRÉE !")
    print(f"📁 Fichier : {OUTPUT_FILE}")
    print(f"📈 Indicateurs disponibles : TM, TN_moy, TX_moy, RR_cumul, Jours_Chaleur, Jours_Gel, Nuits_Tropicales, Anomalies")

if __name__ == "__main__":
    silver_to_gold_expert()