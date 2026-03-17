import subprocess
import os
import sys

def run_script(script_name):
    print(f"\n>>> Démarrage : {script_name}")
    try:
        # Run with current python interpreter
        result = subprocess.run([sys.executable, script_name], check=True)
        print(f">>> Terminée : {script_name}")
    except subprocess.CalledProcessError as e:
        print(f"!!! Erreur lors de l'exécution de {script_name} : {e}")
        sys.exit(1)

def main():
    print("="*60)
    print("PIPELINE AUTOMATIQUE : CLIMASPHERE")
    print("="*60)
    
    # 1. Récupération des dernières données
    run_script("recuperation_donnees_climat.py")
    
    # 2. Transformation Silver & Gold
    run_script("transform_data.py")
    
    # 3. Calcul des Projections IA
    run_script("modeling_projections.py")
    
    print("\n" + "="*60)
    print("PIPELINE TERMINÉE AVEC SUCCÈS")
    print("Les données du dashboard sont à jour.")
    print("="*60)

if __name__ == "__main__":
    main()
