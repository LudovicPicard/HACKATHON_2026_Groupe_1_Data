from flask import Flask, render_template, jsonify, Response
from flask_cors import CORS
from dotenv import load_dotenv
import pandas as pd
import os
import json

load_dotenv()

app = Flask(__name__)
CORS(app)

# Configuration
GOLD_DATA_PATH = "data_climat/gold_climate_indicators.parquet"
PROJECTION_DATA_PATH = "data_climat/gold_projections.parquet"
PERF_DATA_PATH = "data_climat/model_performance.parquet"
IGT_DATA_PATH = "data_climat/igt_emissions.json"
SEA_TEMP_DATA_PATH = "data_climat/gold_sea_temperature.parquet"  # Force reload 2


def load_data(path):
    if os.path.exists(path):
        return pd.read_parquet(path, engine='pyarrow')
    return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    if df_gold_global is not None:
        return Response(df_gold_global.to_json(orient='records'), mimetype='application/json')
    return jsonify({"error": "Data not found"}), 404

@app.route('/api/projections')
def get_projections():
    if df_proj_global is not None:
        return Response(df_proj_global.to_json(orient='records'), mimetype='application/json')
    return jsonify({"error": "Projections not found"}), 404

@app.route('/api/cities')
def get_cities():
    if df_gold_global is not None:
        cities = sorted(df_gold_global['VILLE'].unique().tolist())
        options_to_top = ['France (avec Outre-mer)', 'France (sans Outre-mer)']
        for opt in options_to_top:
            if opt in cities:
                cities.remove(opt)
        if 'France' in cities:
            cities.remove('France')
        cities = options_to_top + cities
        return jsonify(cities)
    return jsonify({"error": "Data not found"}), 404

@app.route('/api/departments')
def get_departments():
    mapping_path = "data_climat/dept_mapping.json"
    if os.path.exists(mapping_path):
        try:
            with open(mapping_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify(data)
        except Exception as e:
            return jsonify({"error": f"Error reading mapping data: {e}"}), 500
    return jsonify({"error": "Department mapping not found"}), 404

@app.route('/api/performance')
def get_performance():
    df = load_data(PERF_DATA_PATH)
    if df is not None:
        return Response(df.to_json(orient='records'), mimetype='application/json')
    return jsonify({"error": "Performance data not found"}), 404

@app.route('/api/igt')
def get_igt():
    if os.path.exists(IGT_DATA_PATH):
        try:
            with open(IGT_DATA_PATH, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return jsonify(data)
        except:
            return jsonify({"error": "Error reading IGT data"}), 500
    return jsonify({"error": "IGT data not found"}), 404

@app.route('/api/deforestation')
def get_deforestation():
    """Return per‑department tree‑cover loss for the most recent year as JSON."""
    if df_deforestation is not None:
        return Response(df_deforestation.to_json(orient='records'), mimetype='application/json')
    return Response('[]', mimetype='application/json')

@app.route('/api/sea-temperature')
def get_sea_temperature():
    """Return sea surface temperature evolution as JSON."""
    if df_sea_temp is not None:
        return Response(df_sea_temp.to_json(orient='records'), mimetype='application/json')
    return jsonify({"error": "Sea temperature data not found"}), 404


CITY_TO_DEPT = {
    "Bedarieux": "Hérault (34)",
    "Begrolles": "Maine-et-Loire (49)",
    "Bordeaux": "Gironde (33)",
    "Brest": "Finistère (29)",
    "Charavines": "Isère (38)",
    "Dijon": "Côte-d'Or (21)",
    "Lille": "Nord (59)",
    "Lyon": "Rhône (69)",
    "Marseille": "Bouches-du-Rhône (13)",
    "Nantes": "Loire-Atlantique (44)",
    "Nice": "Alpes-Maritimes (06)",
    "Nimes": "Gard (30)",
    "Octeville": "Seine-Maritime (76)",
    "Paris": "Paris (75)",
    "Rennes": "Ille-et-Vilaine (35)",
    "Sommesous": "Marne (51)",
    "St etienne": "Loire (42)",
    "Strasbourg": "Bas-Rhin (67)",
    "Toulon": "Var (83)",
    "Toulouse": "Haute-Garonne (31)"
}

def load_deforestation():
    """Load deforestation data per department and return a DataFrame.
    Returns columns: ['departement', 'loss_ha'] for the most recent year.
    It reads 'Subnational 2 tree cover loss' filtered by threshold=30, loads
    'data_climat/dept_mapping.json' to translate raw department names to the
    dashboard's display names (e.g. 'Ain (01)').
    """
    import json
    import unicodedata
    
    excel_path = "data_climat/Deforestation_FRANCE.xlsx"
    mapping_path = "data_climat/dept_mapping.json"
    
    if not os.path.exists(excel_path) or not os.path.exists(mapping_path):
        return None
        
    # Load department mapping
    with open(mapping_path, 'r', encoding='utf-8') as f:
        dept_map = json.load(f)
        
    def normalize_name(name):
        if not name:
            return ""
        name = unicodedata.normalize('NFD', str(name))
        return "".join(c for c in name if unicodedata.category(c) != 'Mn').lower().replace("-", "").replace(" ", "").replace("'", "")
        
    # Build lookup from normalized name to "DeptName (code)"
    norm_to_full = {}
    for code, info in dept_map.items():
        name = info.get('dept_name', '')
        full_name = f"{name} ({code})"
        norm_to_full[normalize_name(name)] = full_name
        
    # Special cases for Corse
    norm_to_full[normalize_name("Corse-du-Sud")] = "Corse (20)"
    norm_to_full[normalize_name("Haute-Corse")] = "Corse (20)"
    
    # Load the sheet with department-level loss
    df = pd.read_excel(excel_path, sheet_name='Subnational 2 tree cover loss')
    # Filter by standard tree cover threshold 30%
    df_30 = df[df['threshold'] == 30]
    
    # Identify loss columns by year
    loss_cols = [c for c in df_30.columns if str(c).startswith('tc_loss_ha_')]
    if not loss_cols:
        return None
    latest_year = max(int(col.split('_')[-1]) for col in loss_cols)
    latest_col = f'tc_loss_ha_{latest_year}'
    
    # Build records using mapped names
    mapped_records = {}
    for _, row in df_30.iterrows():
        raw_dept = str(row['subnational2']).strip()
        loss = row[latest_col]
        norm_dept = normalize_name(raw_dept)
        
        if norm_dept in norm_to_full:
            full_name = norm_to_full[norm_dept]
            mapped_records[full_name] = mapped_records.get(full_name, 0) + (loss if pd.notnull(loss) else 0)
            
    # Convert dict to DataFrame
    records = [{'departement': k, 'loss_ha': v} for k, v in mapped_records.items()]
    return pd.DataFrame(records)

# Pre‑load deforestation data
df_deforestation = load_deforestation()

# Pre‑load sea temperature data
df_sea_temp = load_data(SEA_TEMP_DATA_PATH)


def get_display_name(city):
    return CITY_TO_DEPT.get(city, city)

def get_france_aggregation(df):
    """Return original dataframe plus two aggregate rows for France.
    - "France (avec Outre‑mer)" : mean of all numeric columns per ANNEE (and MODEL_IA / FRAME if present).
    - "France (sans Outre‑mer)" : same mean after removing overseas departments (codes 97x/98x).
    The function preserves the original data and appends the two aggregates.
    """
    import numpy as np
    import re
    if df is None:
        return None
    # Identify numeric columns to aggregate (exclude identifier columns)
    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    # Remove ANNEE from aggregation columns if present
    group_cols = ['ANNEE']
    if 'MODEL_IA' in df.columns:
        group_cols.append('MODEL_IA')
    if 'FRAME' in df.columns:
        group_cols.append('FRAME')
    # Keep only numeric columns that are not part of the grouping keys
    agg_cols = [c for c in numeric_cols if c not in group_cols]
    # 1️⃣ France (avec Outre‑mer) – use all rows
    france_all = df.groupby(group_cols)[agg_cols].mean().reset_index()
    france_all['VILLE'] = 'France (avec Outre-mer)'
    # 2️⃣ France (sans Outre-mer) – filter out overseas departments
    is_overseas = df['VILLE'].str.contains(r'\((?:97\d|98\d)\)$', regex=True, na=False)
    df_metropolitan = df[~is_overseas]
    france_metro = df_metropolitan.groupby(group_cols)[agg_cols].mean().reset_index()
    france_metro['VILLE'] = 'France (sans Outre-mer)'
    # Concatenate original data with the two aggregates
    result = pd.concat([df, france_all, france_metro], ignore_index=True)
    # Normaliser les tirets typographiques en tiret ASCII
    if 'VILLE' in result.columns:
        result['VILLE'] = result['VILLE'].astype(str).str.replace('‑', '-', regex=False)
    return result


df_gold_global = get_france_aggregation(load_data(GOLD_DATA_PATH))
df_proj_global = get_france_aggregation(load_data(PROJECTION_DATA_PATH))

# Build a lookup dictionary for departments mapping
DEPT_LOOKUP = {}
try:
    mapping_path = "data_climat/dept_mapping.json"
    if os.path.exists(mapping_path):
        with open(mapping_path, 'r', encoding='utf-8') as f:
            mapping_data = json.load(f)
            for code, dept in mapping_data.items():
                display_name = f"{dept['dept_name']} ({dept['code']})"
                DEPT_LOOKUP[code.lower()] = display_name
                DEPT_LOOKUP[dept['code'].lower()] = display_name
                DEPT_LOOKUP[dept['dept_name'].lower()] = display_name
except Exception as e:
    print(f"Error initializing DEPT_LOOKUP: {e}")

def get_mentioned_department(query):
    query_lower = query.lower()
    
    # 1. Check for department codes (like 33, 01, 75, etc.) in the query
    import re
    # Match 2 to 3 digit numbers or 2A/2B (Corsica)
    code_matches = re.findall(r'\b\d{2,3}\b|\b2[abAB]\b', query_lower)
    for code in code_matches:
        if code.isdigit() and len(code) == 1:
            code_str = f"0{code}"
        else:
            code_str = code
            
        if code_str in DEPT_LOOKUP:
            return DEPT_LOOKUP[code_str]

    # 2. Check for department names
    for name_key, display_name in DEPT_LOOKUP.items():
        if not name_key.isdigit() and len(name_key) > 2 and name_key in query_lower:
            return display_name

    # 3. Check for specific historic cities
    city_to_dept = {
        "bedarieux": "Hérault (34)", "begrolles": "Maine-et-Loire (49)", "bordeaux": "Gironde (33)",
        "brest": "Finistère (29)", "charavines": "Isère (38)", "dijon": "Côte-d'Or (21)",
        "lille": "Nord (59)", "lyon": "Rhône (69)", "marseille": "Bouches-du-Rhône (13)",
        "nantes": "Loire-Atlantique (44)", "nice": "Alpes-Maritimes (06)", "nimes": "Gard (30)",
        "octeville": "Seine-Maritime (76)", "paris": "Paris (75)", "rennes": "Ille-et-Vilaine (35)",
        "sommesous": "Marne (51)", "st etienne": "Loire (42)", "strasbourg": "Bas-Rhin (67)",
        "toulon": "Var (83)", "toulouse": "Haute-Garonne (31)", "grenoble": "Isère (38)",
        "angers": "Maine-et-Loire (49)", "reims": "Marne (51)", "le havre": "Seine-Maritime (76)"
    }
    for city_key, display_name in city_to_dept.items():
        if city_key in query_lower:
            return display_name

    if "france" in query_lower:
        if any(w in query_lower for w in ["outre", "dom", "tom"]):
            return "France (avec Outre-mer)"
        return "France (sans Outre-mer)"

    return None

@app.route('/api/chat', methods=['POST'])
def chat():
    from flask import request
    from mistralai.client import Mistral
    import json
    
    api_key = os.getenv('MISTRAL_API_KEY')
    client = Mistral(api_key=api_key)
    model = "mistral-small-latest"  # fast, low latency model

    query = request.json.get('message', '').strip()
    if not query:
        return jsonify({"response": "Je n'ai pas reçu de message. Comment puis-je vous aider ?"})

    # 1. Load IGT data
    igt_data = {}
    if os.path.exists(IGT_DATA_PATH):
        try:
            with open(IGT_DATA_PATH, 'r', encoding='utf-8') as f:
                igt_data = json.load(f)
        except:
            pass

    # 2. RAG Retrieval: Check if the user is asking about a specific department/city
    mentioned_dept = get_mentioned_department(query)
    
    context_data = ""
    if mentioned_dept:
        # Get historical and projections data for this department
        dept_data = df_gold_global[df_gold_global['VILLE'] == mentioned_dept] if df_gold_global is not None else pd.DataFrame()
        dept_proj = df_proj_global[df_proj_global['VILLE'] == mentioned_dept] if df_proj_global is not None else pd.DataFrame()
        
        # Detect year/period in query
        import re
        years_found = [int(y) for y in re.findall(r'\b(19\d{2}|20\d{2})\b', query)]
        # Filter years within our dataset boundaries (e.g., 1990 to 2050)
        years_found = [y for y in years_found if 1990 <= y <= 2050]
        
        # If we have years, query them specifically
        period_context = ""
        if len(years_found) >= 2:
            y_min, y_max = min(years_found), max(years_found)
            hist_period = dept_data[(dept_data['ANNEE'] >= y_min) & (dept_data['ANNEE'] <= y_max)]
            proj_period = dept_proj[(dept_proj['ANNEE'] >= y_min) & (dept_proj['ANNEE'] <= y_max)]
            
            period_context = f"- **Données pour la période demandée ({y_min}-{y_max})** :\n"
            if not hist_period.empty:
                avg_tm = hist_period['TM'].mean()
                avg_rr = hist_period['RR_TOTAL'].mean()
                avg_frost = hist_period['DAYS_FROST'].mean()
                avg_canicule = hist_period['DAYS_CANICULE'].mean()
                period_context += f"  - [Historique local] Température moyenne : {round(avg_tm, 1)}°C, Précipitations : {round(avg_rr)} mm/an, Jours de gel : {round(avg_frost, 1)} j/an, Jours de canicule : {round(avg_canicule, 1)} j/an\n"
                
                # Year by year details
                years_details = []
                for _, row in hist_period.iterrows():
                    years_details.append(f"{int(row['ANNEE'])}: {round(row['TM'], 1)}°C")
                period_context += f"  - [Détail annuel] " + " | ".join(years_details) + "\n"
            if not proj_period.empty:
                avg_tm_med = proj_period['TM_MEDIAN'].mean()
                avg_canicule_proj = proj_period['DAYS_CANICULE'].mean()
                avg_frost_proj = proj_period['DAYS_FROST'].mean()
                period_context += f"  - [Projections IA] Température moyenne projetée : {round(avg_tm_med, 1)}°C, Canicules projetées : {round(avg_canicule_proj, 1)} j/an, Jours de gel : {round(avg_frost_proj, 1)} j/an\n"
        elif len(years_found) == 1:
            y = years_found[0]
            hist_year = dept_data[dept_data['ANNEE'] == y]
            proj_year = dept_proj[dept_proj['ANNEE'] == y]
            
            period_context = f"- **Données pour l'année demandée ({y})** :\n"
            if not hist_year.empty:
                row = hist_year.iloc[0]
                period_context += f"  - [Historique local] Température moyenne : {round(row['TM'], 1)}°C, Précipitations : {round(row['RR_TOTAL'])} mm, Jours de gel : {round(row['DAYS_FROST'])} j, Jours de canicule : {round(row['DAYS_CANICULE'])} j\n"
            elif not proj_year.empty:
                row = proj_year.iloc[0]
                period_context += f"  - [Projections IA] Température moyenne projetée : {round(row['TM_MEDIAN'], 1)}°C, Canicules : {round(row['DAYS_CANICULE'], 1)} j, Jours de gel : {round(row['DAYS_FROST'], 1)} j\n"

        # Get emissions data
        dept_emissions = igt_data.get(mentioned_dept)
        if mentioned_dept.startswith("France"):
            # Sum emissions for national total
            is_with_dom = (mentioned_dept == "France (avec Outre-mer)")
            import re
            dept_emissions = {
                "Residentiel": sum(d.get("Residentiel", 0) for k, d in igt_data.items() if (is_with_dom or not re.search(r'\((?:97\d|98\d)\)$', k))),
                "Routier": sum(d.get("Routier", 0) for k, d in igt_data.items() if (is_with_dom or not re.search(r'\((?:97\d|98\d)\)$', k))),
                "Industrie (hors prod. centr. d'énergie)": sum(d.get("Industrie (hors prod. centr. d'énergie)", 0) for k, d in igt_data.items() if (is_with_dom or not re.search(r'\((?:97\d|98\d)\)$', k))),
                "Tertiaire": sum(d.get("Tertiaire", 0) for k, d in igt_data.items() if (is_with_dom or not re.search(r'\((?:97\d|98\d)\)$', k))),
                "Agriculture": sum(d.get("Agriculture", 0) for k, d in igt_data.items() if (is_with_dom or not re.search(r'\((?:97\d|98\d)\)$', k))),
                "TOTAL_CO2e": sum(d.get("TOTAL_CO2e", 0) for k, d in igt_data.items() if (is_with_dom or not re.search(r'\((?:97\d|98\d)\)$', k))),
            }

        context_data = f"### BASE DE DONNÉES LOCALE - DONNÉES DISPONIBLES POUR : {mentioned_dept}\n"
        
        if dept_emissions:
            ind_key = "Industrie (hors prod. centr. d'énergie)"
            ind_val = dept_emissions.get(ind_key, 0)
            context_data += f"- **Émissions de CO2e totales (2021)** : {round(dept_emissions.get('TOTAL_CO2e', 0)):,} tonnes.\n"
            context_data += f"  - Secteurs : Résidentiel={round(dept_emissions.get('Residentiel', 0)):,} t | Transport routier={round(dept_emissions.get('Routier', 0)):,} t | Industrie={round(ind_val):,} t | Tertiaire={round(dept_emissions.get('Tertiaire', 0)):,} t | Agriculture={round(dept_emissions.get('Agriculture', 0)):,} t\n"
        
        if period_context:
            context_data += period_context
        
        if not dept_data.empty:
            # Aggregate and display decade-by-decade averages
            context_data += "- **Historique par décennies (Moyennes locales)** :\n"
            for start_y, end_y in [(1990, 1999), (2000, 2009), (2010, 2019), (2020, 2025)]:
                dec_data = dept_data[(dept_data['ANNEE'] >= start_y) & (dept_data['ANNEE'] <= end_y)]
                if not dec_data.empty:
                    avg_tm = dec_data['TM'].mean()
                    avg_rr = dec_data['RR_TOTAL'].mean()
                    avg_frost = dec_data['DAYS_FROST'].mean()
                    avg_canicule = dec_data['DAYS_CANICULE'].mean()
                    context_data += f"  - Période {start_y}-{end_y} : Temp. moyenne = {round(avg_tm, 1)}°C | Précipitations = {round(avg_rr)} mm/an | Jours de gel = {round(avg_frost, 1)} j/an | Jours de canicule = {round(avg_canicule, 1)} j/an\n"
                
        if not dept_proj.empty:
            proj_2030 = dept_proj[dept_proj['ANNEE'] == 2030]
            proj_2050 = dept_proj[dept_proj['ANNEE'] == 2050]
            context_data += f"- **Projections futures (Modèle Prophet / RCP)** :\n"
            if not proj_2030.empty:
                context_data += f"  - En 2030 : Temp. moyenne = {round(proj_2030.iloc[0]['TM_MEDIAN'], 1)}°C (min={round(proj_2030.iloc[0]['TM_OPTIMISTIC'], 1)}°C, max={round(proj_2030.iloc[0]['TM_PESSIMISTIC'], 1)}°C), Canicules = {round(proj_2030.iloc[0]['DAYS_CANICULE'], 1)} j/an, Jours de gel = {round(proj_2030.iloc[0]['DAYS_FROST'], 1)} j/an, Nuits tropicales = {round(proj_2030.iloc[0]['NIGHTS_TROPICAL'], 1)} j/an\n"
            if not proj_2050.empty:
                context_data += f"  - En 2050 : Temp. moyenne = {round(proj_2050.iloc[0]['TM_MEDIAN'], 1)}°C (min={round(proj_2050.iloc[0]['TM_OPTIMISTIC'], 1)}°C, max={round(proj_2050.iloc[0]['TM_PESSIMISTIC'], 1)}°C), Canicules = {round(proj_2050.iloc[0]['DAYS_CANICULE'], 1)} j/an, Jours de gel = {round(proj_2050.iloc[0]['DAYS_FROST'], 1)} j/an, Nuits tropicales = {round(proj_2050.iloc[0]['NIGHTS_TROPICAL'], 1)} j/an\n"
    else:
        # General overview RAG context
        cities = sorted(df_gold_global['VILLE'].unique().tolist()) if df_gold_global is not None else []
        sorted_igt = sorted(igt_data.items(), key=lambda x: x[1].get('TOTAL_CO2e', 0), reverse=True)
        top_emitters = [f"{k} ({round(v.get('TOTAL_CO2e', 0)/1e6, 2)}M tCO2e)" for k, v in sorted_igt[:5]]
        bottom_emitters = [f"{k} ({round(v.get('TOTAL_CO2e', 0)/1e3, 0)}k tCO2e)" for k, v in sorted_igt[-5:]]
        
        context_data = f"""### BASE DE DONNÉES LOCALE - APERÇU GÉNÉRAL (FRANCE ENTIÈRE)
- Nombre de départements suivis : {len(cities) - 1 if len(cities) > 0 else 0}
- Top 5 départements les plus émetteurs : {', '.join(top_emitters)}
- Top 5 départements les moins émetteurs : {', '.join(bottom_emitters)}
- Moyennes nationales (France) :
  - Température moyenne historique récente (2020-2025) : {round(df_gold_global[(df_gold_global['VILLE']=='France (sans Outre-mer)') & (df_gold_global['ANNEE']>=2020)]['TM'].mean(), 1) if df_gold_global is not None else 'N/A'}°C
  - Projection température moyenne en 2050 : {round(df_proj_global[(df_proj_global['VILLE']=='France (sans Outre-mer)') & (df_proj_global['ANNEE']==2050)]['TM_MEDIAN'].mean(), 1) if df_proj_global is not None else 'N/A'}°C
"""

    system_instruction = f"""
Tu es ClimaBot, un expert en climat et en émissions de CO2 en France. Ton rôle est d'aider les utilisateurs à analyser et comprendre les données de notre plateforme ClimaSphere.

CONTEXTE LOCAL DE LA BASE DE DONNÉES :
{context_data}

CONSIGNES STRICTES :
1. **Priorité absolue aux données locales** : Réponds en priorité en utilisant les chiffres exacts de la base de données locale fournis dans le contexte ci-dessus. Précise au début de la réponse que ces données proviennent de notre base de données locale (ex: "Selon nos données locales pour...").
2. **Projections Saisonnières Obligatoires** : Si l'utilisateur pose une question sur l'évolution des températures d'un département ou d'une ville (ou s'il demande un bilan climatique global), tu **DOIS OBLIGATOIREMENT** ajouter une section ou un tableau Markdown présentant une estimation ou projection des températures moyennes pour les **4 saisons** (Hiver, Printemps, Été, Automne) à horizon **2030** ou **2050** (en t'appuyant sur tes connaissances de l'évolution du climat régional en France).
3. **Plan d'Actions Écologiques Local** : Pour chaque département ou ville demandé, ajoute une section décrivant ce que la ville/le département met en œuvre concrètement pour atténuer le réchauffement et réduire les émissions (ex: végétalisation, limitation des voitures/ZFE, développement des pistes cyclables, énergies renouvelables). Sois spécifique à la ville (ex: Paris réduit le trafic et végétalise, Marseille électrifie les quais du port, Lyon crée des ZFE et voies lyonnaises).
4. **Gestion de l'absence de données (Recherche Web / Connaissances externes)** : Si la question de l'utilisateur porte sur un département non disponible dans la base de données locale, une autre région du monde, ou s'il demande des explications scientifiques générales :
   - Réponds en utilisant tes connaissances du web.
   - Tu **DOIS OBLIGATOIREMENT** ajouter une section spécifique intitulée "**Sources externes :**" à la fin de ta réponse et citer tes sources précises (ex: "Source : GIEC", "Source : Météo-France", "Source : ADEME").
   - Mentionne explicitement dans ton texte que cette information provient de recherches ou de connaissances externes et non de notre base locale.
5. **Formatage et Présentation Premium** :
   - **Met en gras** (`**chiffres**`) absolument tous les chiffres importants (températures, pourcentages, tonnes de CO2e, dates).
   - Utilise des listes à puces claires et des structures aérées pour rendre la réponse agréable à lire.
   - Si tu affiches des listes de chiffres ou de statistiques comparatives (notamment pour les projections saisonnières), privilégie l'utilisation de **tableaux Markdown**.
   - Utilise des emojis de manière pertinente pour dynamiser le texte.
   - Sois synthétique, précis et courtois.
"""

    try:
        # Safe logging to console (avoiding emoji encoding crashes on Windows)
        print(f"\n[CHAT] Query received for department: {mentioned_dept}")
        chat_response = client.chat.complete(
            model=model,
            messages=[
                {"role": "system", "content": system_instruction},
                {"role": "user", "content": query},
            ]
        )
        response_text = chat_response.choices[0].message.content
        print(f"[CHAT] Response received successfully.")
        return jsonify({"response": response_text})
    except Exception as e:
        print(f"Mistral Error: {e}")
        return jsonify({"response": "Désolé, j'ai une erreur de connexion avec Mistral AI. Je reviens vite !"})

if __name__ == '__main__':
    print("Dashboard accessible sur http://127.0.0.1:5000")
    app.run(debug=True, port=5000)
