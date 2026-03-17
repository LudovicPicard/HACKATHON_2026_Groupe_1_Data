from flask import Flask, render_template, jsonify, Response
from flask_cors import CORS
import pandas as pd
import os
import json

app = Flask(__name__)
CORS(app)

# Configuration
GOLD_DATA_PATH = "data_climat/gold_climate_indicators.parquet"
PROJECTION_DATA_PATH = "data_climat/gold_projections.parquet"
PERF_DATA_PATH = "data_climat/model_performance.parquet"
IGT_DATA_PATH = "data_climat/igt_emissions.json"

def load_data(path):
    if os.path.exists(path):
        return pd.read_parquet(path, engine='pyarrow')
    return None

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/data')
def get_data():
    df = load_data(GOLD_DATA_PATH)
    if df is not None:
        return Response(df.to_json(orient='records'), mimetype='application/json')
    return jsonify({"error": "Data not found"}), 404

@app.route('/api/projections')
def get_projections():
    df = load_data(PROJECTION_DATA_PATH)
    if df is not None:
        return Response(df.to_json(orient='records'), mimetype='application/json')
    return jsonify({"error": "Projections not found"}), 404

@app.route('/api/cities')
def get_cities():
    df = load_data(GOLD_DATA_PATH)
    if df is not None:
        cities = sorted(df['VILLE'].unique().tolist())
        return jsonify(cities)
    return jsonify({"error": "Data not found"}), 404

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

def get_display_name(city):
    return CITY_TO_DEPT.get(city, city)

# Pre-load data once at startup
df_gold_global = load_data(GOLD_DATA_PATH)
df_proj_global = load_data(PROJECTION_DATA_PATH)

@app.route('/api/chat', methods=['POST'])
def chat():
    from flask import request
    from mistralai.client import Mistral
    import json
    
    api_key = "NWiq8XxKoACZJYm3kAs52DTUXiLm3KAz"
    client = Mistral(api_key=api_key)
    model = "mistral-large-latest"

    query = request.json.get('message', '').strip()
    if not query:
        return jsonify({"response": "Je n'ai pas reçu de message. Comment puis-je vous aider ?"})

    # 1. Prepare Local Data Context (Using global pre-loaded data)
    igt_data = {}
    if os.path.exists(IGT_DATA_PATH):
        try:
            with open(IGT_DATA_PATH, 'r', encoding='utf-8') as f:
                igt_data = json.load(f)
        except:
            pass
        
    cities = sorted(df_gold_global['VILLE'].unique().tolist()) if df_gold_global is not None else []
    
    # Summary of IGT data for the prompt
    emissions_summary = ""
    for city in cities:
        if city in igt_data:
            c_data = igt_data[city]
            emissions_summary += f"- {get_display_name(city)}: {round(c_data.get('TOTAL_CO2e', 0))} tonnes CO2e.\n"

    # Summary of Projections (2050)
    proj_summary = ""
    if df_proj_global is not None:
        for city in cities:
            p_2050 = df_proj_global[(df_proj_global['VILLE'] == city) & (df_proj_global['ANNEE'] == 2050)]
            if not p_2050.empty:
                temp = round(p_2050.iloc[0]['TM_MEDIAN'], 1)
                proj_summary += f"- {get_display_name(city)} en 2050: env. {temp}°C.\n"

    context = f"""
Tu es ClimaBot, un expert en climat en France. Ton rôle est d'aider les utilisateurs à comprendre les données climatiques et les émissions de CO2 au niveau des départements.
Note: Dans notre système, chaque département est représenté par une station météo spécifique (ex: Gironde (33) correspond aux données de Bordeaux).

DÉPARTEMENTS DISPONIBLES : {', '.join([get_display_name(c) for c in cities])}

ÉMISSIONS CO2 (IGT 2021) :
{emissions_summary}

PROJECTIONS TEMPÉRATURE (2050) :
{proj_summary}

CONSIGNES :
1. Utilise TOUJOURS les données ci-dessus si elles répondent à la question.
2. Si l'utilisateur demande une donnée que nous n'avons pas (ex: une autre ville ou une année non listée), cherche sur Internet (ou utilise ta connaissance) et CITE tes sources.
3. Sois pédagogique, expert et utilise du HTML (<b>, <i>) pour mettre en gras les chiffres.
4. Si la question est hors sujet (ex: cuisine), redirige poliment vers le climat.
5. Indique clairement si une information vient de nos données locales ou d'une recherche externe.
"""

    try:
        chat_response = client.chat.complete(
            model=model,
            messages=[
                {"role": "system", "content": context},
                {"role": "user", "content": query},
            ]
        )
        response_text = chat_response.choices[0].message.content
        return jsonify({"response": response_text})
    except Exception as e:
        print(f"Mistral Error: {e}")
        return jsonify({"response": "Désolé, j'ai une erreur de connexion avec Mistral AI. Je reviens vite !"})

if __name__ == '__main__':
    print("Dashboard accessible sur http://127.0.0.1:5000")
    app.run(debug=True, port=5000)
