import pandas as pd
import os
import glob

# 1. Configuration des chemins
path = r"C:\Users\matgu\Documents\SupDeVinci\Hackaton\Datas"
output_file = os.path.join(path, "france_climat_complet.parquet") # Format Parquet recommandé pour la performance nationale

print("🚀 Initialisation du Pipeline d'Ingestion National...")

# 2. Détection dynamique de tous les départements présents dans le dossier
# On cherche tous les fichiers "previous" pour lister les départements disponibles
search_pattern = os.path.join(path, "Q_*_previous-1950-2024_RR-T-Vent.csv.gz")
previous_files = glob.glob(search_pattern)

# Extraction des codes de départements uniques (ex: '01', '33', '971', '988')
list_depts = sorted(list(set([os.path.basename(f).split('_')[1] for f in previous_files])))

print(f"📦 {len(list_depts)} départements détectés (incluant les DOM-TOM).")
print(f"🔍 Liste des départements à traiter : {', '.join(list_depts)}")
print("-" * 50)

all_departments_data = []

# 3. Boucle globale sur chaque département
for dept in list_depts:
    print(f"\n🍏 [Département {dept}] - Début du traitement...")
    
    # Reconstruction dynamique des noms des 3 fichiers
    file_avant = f"Q_{dept}_avant-1949_RR-T-Vent.csv.gz"
    file_prev = f"Q_{dept}_previous-1950-2024_RR-T-Vent.csv.gz"
    file_late = f"Q_{dept}_latest-2025-2026_RR-T-Vent.csv.gz"
    
    files_to_merge = [file_avant, file_prev, file_late]
    
    # Chemin du fichier de référence pour l'auto-détection
    ref_file_path = os.path.join(path, file_prev)
    
    if not os.path.exists(ref_file_path):
        print(f"⚠️ Fichier de référence manquant pour le {dept}. Passage au suivant.")
        continue
        
    # --- ÉTAPE A : Auto-détection de la station la plus dense ---
    try:
        # Lecture ultra-rapide de la colonne NUM_POSTE uniquement
        df_scan = pd.read_csv(ref_file_path, sep=';', compression='gzip', low_memory=False, usecols=['NUM_POSTE'])
        station_la_plus_dense = df_scan['NUM_POSTE'].value_counts().index[0]
        nb_jours = df_scan['NUM_POSTE'].value_counts().iloc[0]
        print(f"   🎯 Station reine détectée : {station_la_plus_dense} ({nb_jours} jours de relevés)")
    except Exception as e:
        print(f"   ❌ Erreur lors du scan du département {dept} : {e}")
        continue

    # --- ÉTAPE B : Extraction et unification des 3 fichiers de ce département ---
    dept_frames = []
    for file in files_to_merge:
        file_path = os.path.join(path, file)
        
        if os.path.exists(file_path):
            try:
                df = pd.read_csv(file_path, sep=';', compression='gzip', low_memory=False)
                
                # Filtrage immédiat sur la station automatique
                df_filtered = df[df['NUM_POSTE'] == station_la_plus_dense].copy()
                
                if not df_filtered.empty:
                    # Conversion propre de la date
                    df_filtered['AAAAMMJJ'] = pd.to_datetime(df_filtered['AAAAMMJJ'], format='%Y%m%d')
                    dept_frames.append(df_filtered)
            except Exception as e:
                print(f"   ⚠️ Erreur de lecture sur le fichier {file} : {e}")
        else:
            print(f"   ℹ️ Fichier historique non disponible : {file}")

    # --- ÉTAPE C : Fusion locale du département ---
    if dept_frames:
        df_dept_total = pd.concat(dept_frames, ignore_index=True)
        df_dept_total = df_dept_total.sort_values(by='AAAAMMJJ')
        df_dept_total = df_dept_total.dropna(axis=1, how='all')
        
        # Ajout explicite de la colonne Département pour la traçabilité future
        df_dept_total['DEPARTEMENT'] = str(dept)
        
        all_departments_data.append(df_dept_total)
        print(f"   ✅ Unification réussie : {len(df_dept_total)} lignes conservées.")
    else:
        print(f"   ❌ Aucune donnée extraite pour le département {dept}.")

# 4. Étape Finale : Fusion nationale et sauvegarde
if all_departments_data:
    print("\n" + "="*50)
    print("📦 Fusion de tous les départements en un unique Dataset National...")
    final_france_df = pd.concat(all_departments_data, ignore_index=True)
    
    # Sauvegarde au format Parquet (plus léger, conserve les types et ultra-rapide)
    final_france_df.to_parquet(output_file, index=False)
    
    print("-" * 50)
    print(f"🏆 PIPELINE ACCOMPLI AVEC SUCCÈS !")
    print(f"📁 Fichier final généré : {output_file}")
    print(f"📊 Volume total de l'Observatoire National : {final_france_df.shape[0]:,} lignes")
    print(f"📅 Couverture temporelle globale : du {final_france_df['AAAAMMJJ'].min().date()} au {final_france_df['AAAAMMJJ'].max().date()}")
else:
    print("\n❌ Échec critique : Aucun département n'a pu être fusionné.")