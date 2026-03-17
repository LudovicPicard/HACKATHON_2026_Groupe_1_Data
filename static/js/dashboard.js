
document.addEventListener('DOMContentLoaded', () => {
    let allData = [];
    let projectionData = [];
    let currentCity = "";
    let currentModel = "Prophet";
    let currentFrame = "RCP";
    
    // Charts instances
    let evolutionChart, extremeDaysChart, anomalyChart, projectionsChart;
    let ghgSectorChart;
    let map, markers = {};
    let simulationYear = 2026;
    let performanceData = [];
    let igtData = {};
    let meteoLayer, igtLayer;
    let currentLayer = "meteo";
    let simulationMode = "single"; // "single" or "range"
    let rangeStartYear = 1990;
    let rangeEndYear = 2025;
    
    // --- ClimaBot Chat Initialization (Priority) ---
    const chatBtn = document.getElementById('chat-button');
    const chatWin = document.getElementById('chat-window');
    const closeChat = document.getElementById('close-chat');
    const chatInput = document.getElementById('chat-input');
    const sendBtn = document.getElementById('send-chat');
    const chatMsgs = document.getElementById('chat-messages');

    if (chatBtn && chatWin) {
        chatBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            chatWin.classList.toggle('hidden');
            if (!chatWin.classList.contains('hidden') && chatInput) chatInput.focus();
        });

        if (closeChat) closeChat.addEventListener('click', () => chatWin.classList.add('hidden'));
        if (sendBtn) sendBtn.addEventListener('click', handleChat);
        if (chatInput) chatInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') handleChat(); });

        // --- Suggestion Buttons ---
        document.querySelectorAll('.suggest-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                if (chatInput) {
                    chatInput.value = btn.innerText.replace('?', '').trim();
                    handleChat();
                }
            });
        });
    }

    async function handleChat() {
        if (!chatInput) return;
        const msg = chatInput.value.trim();
        if (!msg) return;

        addMessage(msg, 'user');
        chatInput.value = '';

        const typingId = addMessage('...', 'bot typing');

        try {
            const res = await fetch('/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: msg })
            });
            const data = await res.json();
            
            const typingEl = document.getElementById(typingId);
            if (typingEl) typingEl.remove();
            
            addMessage(data.response, 'bot');
        } catch (err) {
            console.error("Chat Error:", err);
            const typingEl = document.getElementById(typingId);
            if (typingEl) typingEl.remove();
            addMessage("Désolé, j'ai un petit problème technique. Réessayez plus tard !", 'bot');
        }
    }

    const citySelect = document.getElementById('citySelect');
    const yearSlider = document.getElementById('yearSlider');
    const activeYearEl = document.getElementById('activeYear');
    
    // City Coordinates for Map
    const cityCoords = {
        "Bedarieux": [43.6167, 3.1667],
        "Begrolles": [47.1333, -0.9333],
        "Bordeaux": [44.8378, -0.5792],
        "Brest": [48.3900, -4.4900],
        "Charavines": [45.4264, 5.5161],
        "Dijon": [47.3167, 5.0167],
        "Lille": [50.6293, 3.0573],
        "Lyon": [45.7640, 4.8357],
        "Marseille": [43.2965, 5.3698],
        "Nantes": [47.2184, -1.5536],
        "Nice": [43.7031, 7.2661],
        "Nimes": [43.8380, 4.3610],
        "Octeville": [49.4938, 0.1077],
        "Paris": [48.8566, 2.3522],
        "Rennes": [48.1173, -1.6778],
        "Sommesous": [48.7411, 4.1953],
        "St etienne": [45.4347, 4.3903],
        "Strasbourg": [48.5800, 7.7500],
        "Toulon": [43.1258, 5.9306],
        "Toulouse": [43.6043, 1.4437]
    };

    const cityToDept = {
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
        "Toulouse": "Haute-Garonne (31)",
        "France": "🇫🇷 France (Moyenne)"
    };

    function getDisplayName(city) {
        return cityToDept[city] || city;
    }
    
    // 1. Initial Load
    Promise.all([
        fetch('/api/cities').then(res => res.json()),
        fetch('/api/data').then(res => res.json()),
        fetch('/api/projections').then(res => res.json()),
        fetch('/api/performance').then(res => res.json()),
        fetch('/api/igt').then(res => res.json())
    ])
    .then(([cities, data, projections, performance, igt]) => {
        console.log("Données chargées:", { cities: cities.length, data: data.length, projections: projections.length, performance: performance.length, igt: Object.keys(igt).length });
        citySelect.innerHTML = '<option value="">Sélectionnez un département...</option>' + 
                               cities.map(city => `<option value="${city}">${getDisplayName(city)}</option>`).join('');
        allData = data;
        if (data.length > 0) {
            currentCity = "France";
            citySelect.value = "France";
            updateDashboard("France");
            highlightMarker("France");
        }
        projectionData = projections;
        performanceData = performance;
        igtData = igt;

        initMap(cities);

        if (cities.length > 0) {
            currentCity = cities[0];
            citySelect.value = currentCity;
            updateDashboard(currentCity);
        }
    })
    .catch(err => {
        console.error("Erreur critique chargement dashboard:", err);
        alert("Erreur lors du chargement des données. Vérifiez la console browser.");
    });
        
    // 2. Event Listeners
    citySelect.addEventListener('change', (e) => {
        currentCity = e.target.value;
        updateDashboard(currentCity);
    });

    document.getElementById('modelSelect').addEventListener('change', (e) => {
        currentModel = e.target.value;
        updateDashboard(currentCity);
    });

    document.getElementById('frameSelect').addEventListener('change', (e) => {
        currentFrame = e.target.value;
        updateDashboard(currentCity);
    });

    // --- Integrated Range Slider Logic ---
    const rangeStart = document.getElementById('rangeStart');
    const rangeEnd = document.getElementById('rangeEnd');
    const rangeLabel = document.getElementById('rangeLabel');
    const sliderTrack = document.querySelector('.slider-track');

    function updateSliderTrack() {
        const min = parseInt(rangeStart.min);
        const max = parseInt(rangeStart.max);
        const v1 = parseInt(rangeStart.value);
        const v2 = parseInt(rangeEnd.value);
        
        const start = Math.min(v1, v2);
        const end = Math.max(v1, v2);
        
        rangeStartYear = start;
        rangeEndYear = end;

        const left = ((start - min) / (max - min)) * 100;
        const right = ((end - min) / (max - min)) * 100;
        
        sliderTrack.style.background = `linear-gradient(to right, 
            rgba(255, 255, 255, 0.1) ${left}%, 
            var(--accent-blue) ${left}%, 
            var(--accent-blue) ${right}%, 
            rgba(255, 255, 255, 0.1) ${right}%)`;

        if (start === end) {
            rangeLabel.textContent = start;
            simulationYear = start;
            simulationMode = "single";
        } else {
            rangeLabel.textContent = `${start} - ${end}`;
            simulationMode = "range";
        }
    }

    rangeStart.addEventListener('input', () => {
        updateSliderTrack();
        updateDashboard(currentCity);
    });

    rangeEnd.addEventListener('input', () => {
        updateSliderTrack();
        updateDashboard(currentCity);
    });

    // Initialize slider visual
    updateSliderTrack();

    document.getElementById('showMeteo').addEventListener('click', () => switchMapLayer('meteo'));
    document.getElementById('showEmissions').addEventListener('click', () => switchMapLayer('igt'));

    function switchMapLayer(layer) {
        currentLayer = layer;
        document.getElementById('showMeteo').classList.toggle('active', layer === 'meteo');
        document.getElementById('showEmissions').classList.toggle('active', layer === 'igt');
        
        if (layer === 'meteo') {
            map.removeLayer(igtLayer);
            meteoLayer.addTo(map);
        } else {
            map.removeLayer(meteoLayer);
            igtLayer.addTo(map);
        }
    }

    function initMap(cities) {
        map = L.map('map', {
            zoomControl: false,
            attributionControl: false,
            dragging: false,
            scrollWheelZoom: false,
            doubleClickZoom: false,
            boxZoom: false,
            touchZoom: false
        }).setView([46.4033, 2.3883], 4.7);

        L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
            maxZoom: 19
        }).addTo(map);
        meteoLayer = L.layerGroup().addTo(map);
        igtLayer = L.layerGroup();

        cities.forEach(city => {
            if (cityCoords[city]) {
                // 1. Meteo Marker
                const marker = L.circleMarker(cityCoords[city], {
                    radius: 8,
                    fillColor: "#00d2ff",
                    color: "#fff",
                    weight: 2,
                    opacity: 1,
                    fillOpacity: 0.8
                }).addTo(meteoLayer);
                
                marker.bindPopup(`<b>${getDisplayName(city)}</b>`);
                marker.on('click', () => {
                    currentCity = city;
                    citySelect.value = city;
                    updateDashboard(city);
                    highlightMarker(city);
                    showMapInfo(city, 'meteo');
                });

                // 2. IGT Marker
                const cityIgt = igtData[city];
                if (cityIgt) {
                    const total = cityIgt.TOTAL_CO2e || 0;
                    // Scale radius based on emissions - proportional to sqrt of area
                    const rad = Math.sqrt(total) / 40 + 4; 
                    
                    const igtMarker = L.circleMarker(cityCoords[city], {
                        radius: rad,
                        fillColor: "#ff4d4d",
                        color: "#fff",
                        weight: 2,
                        opacity: 0.8,
                        fillOpacity: 0.6
                    }).addTo(igtLayer);

                    igtMarker.bindPopup(`<b>${getDisplayName(city)}</b><br><small>Émissions CO2e</small>`);
                    igtMarker.on('click', () => {
                        currentCity = city;
                        citySelect.value = city;
                        updateDashboard(city);
                        showMapInfo(city, 'igt');
                    });
                }
            }
        });
        
        if (cities.length > 0) highlightMarker(cities[0]);
    }

    function highlightMarker(cityName) {
        Object.keys(markers).forEach(c => {
            const m = markers[c];
            const isActive = (c === cityName);
            m.setStyle({
                fillColor: isActive ? "#fbbf24" : "#00d2ff",
                radius: isActive ? 12 : 8
            });
        });
    }

    function showMapInfo(city, mode) {
        const panel = document.getElementById('map-info-panel');
        if (!panel) return;

        if (mode === 'meteo') {
            panel.innerHTML = `
                <h3><i class="fa-solid fa-location-dot"></i> ${getDisplayName(city)}</h3>
                <div class="igt-stats">
                    <p style="font-size: 0.9rem; color: var(--text-secondary);">Station météo active pour les simulations climatiques.</p>
                </div>
            `;
        } else {
            const cityIgt = igtData[city];
            if (!cityIgt) return;
            const total = cityIgt.TOTAL_CO2e || 0;
            const sectors = [
                { l: "🏠 Résidentiel", v: cityIgt.Residentiel },
                { l: "🚛 Transport Routier", v: cityIgt.Routier },
                { l: "🏭 Industrie", v: cityIgt["Industrie (hors prod. centr. d'énergie)"] },
                { l: "💼 Tertiaire", v: cityIgt.Tertiaire },
                { l: "🚜 Agriculture", v: cityIgt.Agriculture }
            ].sort((a,b) => b.v - a.v);

            const icon = (city === "France") ? "fa-solid fa-flag" : "fa-solid fa-industry";
            panel.innerHTML = `
                <h3><i class="${icon}"></i> ${getDisplayName(city)}</h3>
                <div class="igt-stats">
                    ${sectors.slice(0, 3).map(s => `
                        <div class="igt-stat-row">
                            <span class="igt-label">${s.l}</span>
                            <span class="igt-val" style="color: var(--text-primary)">${Math.round(s.v).toLocaleString()} t</span>
                        </div>
                    `).join('')}
                    <div class="igt-stat-row igt-total" style="border-top: 1px dashed rgba(255,255,255,0.2); margin-top: 10px; padding-top: 10px;">
                        <span class="igt-label">TOTAL (CO2e)</span>
                        <span class="igt-val" style="color: var(--accent-sun)">${Math.round(total).toLocaleString()} t</span>
                    </div>
                </div>
                <div class="igt-popup-footer" style="color: var(--text-muted)">Source Citepa (2021)</div>
            `;
        }

        panel.classList.remove('hidden');
    }

    // Hide panel on layer switch
    const oldSwitchMapLayer = switchMapLayer;
    switchMapLayer = function(layer) {
        document.getElementById('map-info-panel').classList.add('hidden');
        oldSwitchMapLayer(layer);
    };

    // Close panel on map click
    map.on('click', () => {
        document.getElementById('map-info-panel').classList.add('hidden');
    });
    
    function updateDashboard(city) {
        // Filter historical and current data
        const cityData = allData.filter(d => d.VILLE === city);
        
        // Find first year with TEMPERATURE data to avoid leading gaps in temperature line
        const firstValidIdx = cityData.findIndex(d => d.TM !== null);
        const filteredCityData = firstValidIdx !== -1 ? cityData.slice(firstValidIdx) : cityData;
        
        // Filter projections
        console.log("Filtrage projections pour:", { city, model: currentModel, frame: currentFrame });
        const cityProjections = (projectionData || []).filter(d => 
            d.VILLE === city && 
            String(d.MODEL_IA) === String(currentModel) && 
            String(d.FRAME) === String(currentFrame)
        ).sort((a,b) => Number(a.ANNEE) - Number(b.ANNEE));
        console.log("Projections filtrées:", cityProjections.length);
        
        // Update Indicator Cards: Handle Mode (Single vs Range)
        if (filteredCityData.length === 0) return;

        let displayData = {};
        let cardSubtext = "";

        if (simulationMode === "single") {
            const yearSim = cityData.find(d => Number(d.ANNEE) === Number(simulationYear));
            const projSim = cityProjections.find(p => Number(p.ANNEE) === Number(simulationYear));
            const year2025 = cityData.find(d => Number(d.ANNEE) === 2025) || filteredCityData[filteredCityData.length - 1];
            
            displayData = yearSim ? { ...yearSim } : (projSim ? { ...year2025 } : { ANNEE: simulationYear });
            
            if (!yearSim && projSim) {
                displayData.TM = projSim.TM_MEDIAN;
                displayData.ANOMALIE_TM = projSim.TM_MEDIAN - (year2025.TM - year2025.ANOMALIE_TM);
                displayData.DAYS_CANICULE = projSim.DAYS_CANICULE;
                displayData.NIGHTS_TROPICAL = projSim.NIGHTS_TROPICAL;
                displayData.DAYS_FROST = projSim.DAYS_FROST;
                displayData.DAYS_HOT_SEASON = projSim.DAYS_HOT_SEASON;
                displayData.RR_TOTAL = projSim.RR_TOTAL;
                displayData.DRY_SPELL_MAX = projSim.DRY_SPELL_MAX;
                displayData.isProjected = true;
            } else {
                displayData.isProjected = false;
            }
            cardSubtext = `Année ${simulationYear}`;
        } else {
            // RANGE MODE: Aggregate data across period
            const years = [];
            for (let y = rangeStartYear; y <= rangeEndYear; y++) {
                const hist = cityData.find(d => Number(d.ANNEE) === y);
                const proj = cityProjections.find(p => Number(p.ANNEE) === y);
                if (hist) years.push(hist);
                else if (proj) {
                    const year2025 = cityData.find(d => Number(d.ANNEE) === 2025) || filteredCityData[filteredCityData.length - 1];
                    years.push({
                        ...proj,
                        TM: proj.TM_MEDIAN,
                        ANOMALIE_TM: proj.TM_MEDIAN - (year2025.TM - year2025.ANOMALIE_TM), // Estimation
                        isProjected: true
                    });
                }
            }

            if (years.length > 0) {
                const count = years.length;
                displayData = {
                    TM: years.reduce((s, y) => s + (y.TM || 0), 0) / count,
                    ANOMALIE_TM: years.reduce((s, y) => s + (y.ANOMALIE_TM || 0), 0) / count,
                    DAYS_CANICULE: years.reduce((s, y) => s + (y.DAYS_CANICULE || 0), 0) / count,
                    NIGHTS_TROPICAL: years.reduce((s, y) => s + (y.NIGHTS_TROPICAL || 0), 0) / count,
                    DAYS_FROST: years.reduce((s, y) => s + (y.DAYS_FROST || 0), 0) / count,
                    DAYS_HOT_SEASON: years.reduce((s, y) => s + (y.DAYS_HOT_SEASON || 0), 0) / count,
                    RR_TOTAL: years.reduce((s, y) => s + (y.RR_TOTAL || 0), 0) / count,
                    DRY_SPELL_MAX: years.reduce((s, y) => s + (y.DRY_SPELL_MAX || 0), 0) / count,
                };
            }
            cardSubtext = `Moyenne ${rangeStartYear}-${rangeEndYear}`;
        }

        // Update Stats Cards with Context
        document.querySelectorAll('.stat-desc').forEach(el => el.textContent = cardSubtext);

        const tmValue = (displayData.TM || 0).toFixed(1) + " °C";
        const anomValue = (displayData.ANOMALIE_TM || 0).toFixed(2);
        updateStatCard("cardTM", tmValue, anomValue);
        
        updateStatCard("cardCanicule", Math.round(displayData.DAYS_CANICULE || 0));
        updateStatCard("cardTropical", Math.round(displayData.NIGHTS_TROPICAL || 0));
        updateStatCard("cardGel", Math.round(displayData.DAYS_FROST || 0));
        updateStatCard("cardHotSeason", Math.round(displayData.DAYS_HOT_SEASON || 0));
        updateStatCard("cardRR", (displayData.RR_TOTAL || 0).toFixed(0) + " mm");
        updateStatCard("cardDrySpell", Math.round(displayData.DRY_SPELL_MAX || 0) + " j");
        
        // CO2 aggregation (Mean of France CO2 over the period)
        let co2Val = 0;
        if (simulationMode === "single") {
             co2Val = displayData.CO2_FRANCE || 4.2; 
        } else {
             const years = [];
             for (let y = rangeStartYear; y <= rangeEndYear; y++) {
                 const d = allData.find(x => Number(x.ANNEE) === y);
                 if (d) years.push(d.CO2_FRANCE || 4.2);
             }
             co2Val = years.length > 0 ? years.reduce((a,b)=>a+b,0)/years.length : 4.2;
        }
        updateStatCard("cardCO2", co2Val.toFixed(1) + " T");
        updateStatCard("cardAnom", (displayData.ANOMALIE_TM || 0).toFixed(2) + " °C");
        
        // Update Charts
        renderEvolutionChart(filteredCityData);
        renderExtremeDaysChart(filteredCityData);
        renderAnomalyChart(filteredCityData); 
        renderProjectionsChart(filteredCityData, cityProjections);

        // Update Advice Engine (Step 5)
        updateAdvice(displayData);

        // Update Performance Table (Step 3)
        updatePerformanceTable(city);

        // Update Gauges (Step 4)
        updateGauges(displayData, city);

        // Update New Charts (Step 2)
        renderGHGSectorChart();
    }
    
    function updateStatCard(id, value, anomaly = null) {
        const card = document.getElementById(id);
        if (!card) return;
        const valEl = card.querySelector('.stat-value');
        if (valEl) valEl.textContent = value;
        
        if (anomaly !== null) {
            const anomEl = document.getElementById('anomTM');
            if (anomEl) {
                anomEl.textContent = `Anomalie: ${anomaly > 0 ? '+' : ''}${anomaly} °C`;
                anomEl.className = `stat-change ${anomaly > 0 ? 'positive' : 'negative'}`;
            }
        }
    }
    
    // Register Plugin
    console.log("Registering ChartDataLabels plugin...", typeof ChartDataLabels !== 'undefined' ? "Loaded" : "FAILED");
    if (typeof ChartDataLabels !== 'undefined') {
        Chart.register(ChartDataLabels);
    }

    function renderEvolutionChart(data) {
        const canvas = document.getElementById('evolutionChart');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');

        let labels = data.map(d => d.ANNEE);
        let tmData = data.map(d => d.TM);
        let rrData = data.map(d => d.RR_TOTAL);
        
        if (evolutionChart) evolutionChart.destroy();
        
        evolutionChart = new Chart(ctx, {
            type: 'line',
            plugins: [ChartDataLabels],
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Température Moyenne (°C)',
                        data: tmData,
                        borderColor: '#00d2ff',
                        backgroundColor: 'rgba(0, 210, 255, 0.1)',
                        fill: true,
                        tension: 0.4,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Précipitations (mm)',
                        data: rrData,
                        borderColor: '#3aedc4',
                        backgroundColor: 'rgba(58, 237, 196, 0.2)',
                        type: 'bar',
                        yAxisID: 'y1'
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { bottom: 20, left: 10, right: 10 } },
                scales: {
                    y: {
                        type: 'linear',
                        position: 'left',
                        grid: { color: 'rgba(255, 255, 255, 0.05)' },
                        ticks: { color: '#cbd5e1' }
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        ticks: { color: '#cbd5e1' }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { 
                            color: '#cbd5e1',
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 25,
                            display: true
                        },
                        title: {
                            display: true,
                            text: 'Année',
                            color: '#cbd5e1',
                            font: { size: 14 }
                        }
                    }
                },
                plugins: {
                    legend: { labels: { color: '#f8fafc', font: { family: 'Inter' } } },
                    datalabels: {
                        display: (context) => {
                            // Only show for Temperature (Dataset 0), hide for Precipitation (Dataset 1)
                            if (context.datasetIndex !== 0) return false;
                            const total = context.dataset.data.length;
                            // Show every 4 years and the final point to keep it clean
                            return context.dataIndex % 4 === 0 || context.dataIndex === total - 1;
                        },
                        color: '#ffffff',
                        font: { size: 10, weight: 'bold' },
                        align: 'top',
                        offset: 4,
                        backgroundColor: 'rgba(15, 23, 42, 0.85)',
                        borderRadius: 4,
                        padding: { top: 2, bottom: 2, left: 5, right: 5 },
                        formatter: (value) => value.toFixed(1) + "°"
                    }
                }
            }
        });
    }
    
    function renderExtremeDaysChart(data) {
        const canvas = document.getElementById('extremeDaysChart');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');

        let labels = data.map(d => d.ANNEE);
        let hotData = data.map(d => d.DAYS_HOT_SEASON);
        let frostData = data.map(d => d.DAYS_FROST);

        if (extremeDaysChart) extremeDaysChart.destroy();
        
        extremeDaysChart = new Chart(ctx, {
            type: 'bar',
            plugins: [ChartDataLabels],
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Saison chaude (>25°C)',
                        data: hotData,
                        backgroundColor: 'rgba(255, 154, 62, 0.7)',
                        borderRadius: 6
                    },
                    {
                        label: 'Jours de gel',
                        data: frostData,
                        backgroundColor: 'rgba(0, 210, 255, 0.5)',
                        borderRadius: 6
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { bottom: 20, left: 10, right: 10 } },
                scales: {
                    x: { 
                        stacked: true, 
                        grid: { display: false }, 
                        ticks: { 
                            color: '#cbd5e1',
                            autoSkip: true,
                            maxTicksLimit: 25,
                            display: true,
                            font: { weight: 'bold' }
                        } 
                    },
                    y: { stacked: true, grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#cbd5e1' } }
                },
                plugins: {
                    legend: { labels: { color: '#f8fafc' } },
                    datalabels: {
                        display: true,
                        color: '#ffffff',
                        font: { size: 12, weight: '900' },
                        anchor: 'center',
                        align: 'center',
                        backgroundColor: 'rgba(15, 23, 42, 0.6)',
                        borderRadius: 6,
                        formatter: (value) => value > 0 ? Math.round(value) : ""
                    }
                }
            }
        });
    }
    
    function renderAnomalyChart(data) {
        const canvas = document.getElementById('anomalyChart');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');

        let labels = data.map(d => d.ANNEE);
        let anomalyData = data.map(d => d.ANOMALIE_TM);

        if (anomalyChart) anomalyChart.destroy();
        
        anomalyChart = new Chart(ctx, {
            type: 'bar',
            plugins: [ChartDataLabels],
            data: {
                labels: labels,
                datasets: [{
                    label: 'Anomalie Thermique (°C)',
                    data: anomalyData,
                    backgroundColor: anomalyData.map(v => v > 0 ? 'rgba(255, 77, 77, 0.7)' : 'rgba(0, 210, 255, 0.7)'),
                    borderRadius: 4
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { bottom: 25, left: 10, right: 10 } },
                scales: {
                    x: { 
                        grid: { display: false }, 
                        position: 'bottom',
                        ticks: { 
                            color: '#cbd5e1',
                            autoSkip: true,
                            maxTicksLimit: 25,
                            display: true,
                            font: { weight: 'bold' }
                        } 
                    },
                    y: { grid: { color: 'rgba(255, 255, 255, 0.05)' }, ticks: { color: '#cbd5e1' } }
                },
                plugins: {
                    legend: { display: false },
                    datalabels: {
                        display: true,
                        color: '#ffffff',
                        font: { size: 10, weight: '800' },
                        anchor: (context) => context.dataset.data[context.dataIndex] > 0 ? 'end' : 'start',
                        align: (context) => context.dataset.data[context.dataIndex] > 0 ? 'top' : 'bottom',
                        backgroundColor: 'rgba(15, 23, 42, 0.8)',
                        borderRadius: 4,
                        padding: 3,
                        formatter: (value) => Math.abs(value) > 0.3 ? (value > 0 ? '+' : '') + value.toFixed(1) : ""
                    }
                }
            }
        });
    }

    function renderProjectionsChart(histData, projData) {
        const canvas = document.getElementById('projectionsChart');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');

        if (projectionsChart) projectionsChart.destroy();

        if (!projData || projData.length === 0) {
            console.warn("Pas de données de projection pour le graphique. City:", city, "Model:", currentModel, "Frame:", currentFrame);
            // On peut quand même afficher l'historique seul ou vider le chart
            if (projectionsChart) projectionsChart.destroy();
            return;
        }

        // Remove 2026 REAL data from this chart context to start projections from 2025 baseline
        const baseHistData = (histData || []).filter(d => Number(d.ANNEE) < 2026);
        if (baseHistData.length === 0) return;

        // Combine labels for full perspective
        const labels = baseHistData.map(d => d.ANNEE).concat(projData.map(d => d.ANNEE));
        
        // Find the last known temperature to use as the "zero" point for anomalies
        const validHistData = baseHistData.filter(d => d.TM !== null);
        const lastTM = validHistData.length > 0 ? validHistData[validHistData.length - 1].TM : 0;
        
        // Use historical data relative to the bridging point (last known value)
        const historicalDelta = baseHistData.map(d => d.TM - lastTM).concat(new Array(projData.length).fill(null));
        
        // Scenario data relative to bridging point
        const optData = new Array(baseHistData.length - 1).fill(null).concat([0]).concat(projData.map(d => d.TM_OPTIMISTIC - lastTM));
        const medData = new Array(baseHistData.length - 1).fill(null).concat([0]).concat(projData.map(d => d.TM_MEDIAN - lastTM));
        const pesData = new Array(baseHistData.length - 1).fill(null).concat([0]).concat(projData.map(d => d.TM_PESSIMISTIC - lastTM));

        projectionsChart = new Chart(ctx, {
            type: 'line',
            plugins: [ChartDataLabels],
            data: {
                labels: labels,
                datasets: [
                    {
                        label: 'Évolution Réelle (vs aujourd\'hui)',
                        data: historicalDelta,
                        borderColor: 'rgba(255, 255, 255, 0.5)',
                        borderWidth: 2,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: currentFrame === 'RCP' ? 'Scénario Optimiste (RCP 2.6)' : 'Scénario Durable (SSP1-2.6)',
                        data: optData,
                        borderColor: '#3aedc4',
                        borderDash: [5, 5],
                        borderWidth: 2,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: currentFrame === 'RCP' ? 'Scénario Median (RCP 4.5)' : 'Scénario Modéré (SSP2-4.5)',
                        data: medData,
                        borderColor: '#00d2ff',
                        borderWidth: 3,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: currentFrame === 'RCP' ? 'Scénario Pessimiste (RCP 8.5)' : 'Scénario Fossile (SSP5-8.5)',
                        borderColor: '#ff4d4d',
                        data: pesData,
                        borderWidth: 3,
                        pointRadius: 0,
                        fill: false
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: { padding: { bottom: 20, left: 10, right: 10 } },
                interaction: { mode: 'index', intersect: false },
                scales: {
                    x: { grid: { display: false }, ticks: { color: '#cbd5e1', maxRotation: 0 } },
                    y: { 
                        grid: { color: 'rgba(255, 255, 255, 0.05)' }, 
                        ticks: { color: '#cbd5e1' }, 
                        title: { display: true, text: 'Augmentation de Température (°C)', color: '#cbd5e1' } 
                    }
                },
                plugins: {
                    legend: { position: 'top', labels: { color: '#f8fafc' } },
                    tooltip: { 
                        backgroundColor: 'rgba(0,0,0,0.8)',
                        callbacks: {
                            label: (context) => {
                                const val = context.parsed.y;
                                if (val === null || val === undefined) return null;
                                return `${context.dataset.label}: ${val > 0 ? '+' : ''}${val.toFixed(1)}°C`;
                            }
                        }
                    },
                    datalabels: {
                        display: (context) => {
                            const year = Number(labels[context.dataIndex]);
                            if (context.datasetIndex === 0) return false; 
                            return [2030, 2050, 2100].includes(year);
                        },
                        color: '#ffffff',
                        font: { size: 12, weight: '900' },
                        backgroundColor: (context) => context.dataset.borderColor,
                        borderRadius: 6,
                        padding: 6,
                        align: (context) => {
                            if (context.datasetIndex === 1) return 'bottom'; 
                            if (context.datasetIndex === 3) return 'top';    
                            return 'center';
                        },
                        offset: 8,
                        formatter: (value) => (value >= 0 ? '+' : '') + value.toFixed(1) + "°C"
                    }
                }
            }
        });
    }

    function updateAdvice(data) {
        const container = document.getElementById('adviceContainer');
        if (!container) return;
        container.innerHTML = "";

        const advices = [];

        // Rules based on Step 5 requirements
        if (data.TM > 15 || (data.DAYS_CANICULE && data.DAYS_CANICULE > 2)) {
            advices.push({
                icon: "fa-solid fa-temperature-arrow-up",
                title: "Risque Caniculaire",
                items: [
                    "Végétalisation des façades et toitures",
                    "Installation de dispositifs de rafraîchissement urbain",
                    "Adaptation des horaires de travail"
                ]
            });
        }

        if (data.DRY_SPELL_MAX > 15 || (data.RR_TOTAL && data.RR_TOTAL < 600)) {
            advices.push({
                icon: "fa-solid fa-droplet-slash",
                title: "Stress Hydrique",
                items: [
                    "Réduction de la consommation d'eau",
                    "Installation de récupérateurs d'eau de pluie",
                    "Sélection de plantes résistantes"
                ]
            });
        }

        advices.push({
            icon: "fa-solid fa-car-side",
            title: "Empreinte Carbone",
            items: [
                "Privilégier les mobilités douces",
                "Transition vers une alimentation bas carbone",
                "Rénovation énergétique des bâtiments"
            ]
        });

        if ((data.DAYS_CANICULE && data.DAYS_CANICULE > 5) || data.DRY_SPELL_MAX > 20) {
            advices.push({
                icon: "fa-solid fa-fire",
                title: "Prévention Incendies",
                items: [
                    "Débroussaillage des abords habités",
                    "Aménagement de zones anti-incendie",
                    "Vigilance accrue en période de vent"
                ]
            });
        }

        advices.forEach(adv => {
            const div = document.createElement('div');
            div.className = "advice-item";
            div.innerHTML = `
                <i class="${adv.icon}"></i>
                <h4>${adv.title}</h4>
                <ul>
                    ${adv.items.map(item => `<li>${item}</li>`).join('')}
                </ul>
            `;
            container.appendChild(div);
        });
    }

    function updatePerformanceTable(city) {
        const tableBody = document.querySelector('#performanceTable tbody');
        if (!tableBody) return;
        tableBody.innerHTML = "";

        if (!performanceData || performanceData.length === 0) {
            console.warn("Pas de données de performance chargées.");
            return;
        }
        console.log("Performance Data Columns (first item):", Object.keys(performanceData[0]));
        const cityPerf = performanceData.filter(p => p.VILLE === city && p.MODEL === currentModel);
        
        cityPerf.forEach(p => {
            const precision = Math.max(0, 100 - p.MAPE_PCT).toFixed(1);
            const badgeClass = precision > 90 ? 'metric-good' : 'metric-fair';
            
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><b>${p.INDICATEUR}</b></td>
                <td>${p.MODEL}</td>
                <td>${p.RMSE}</td>
                <td>${p.MAE}</td>
                <td>${p.MAPE_PCT}%</td>
                <td><span class="metric-badge ${badgeClass}">${precision}%</span></td>
            `;
            tableBody.appendChild(tr);
        });
    }

    function updateGauges(data, city) {
        const tmFill = document.getElementById('tm_gauge');
        const anomFill = document.getElementById('anom_gauge');
        const anomText = document.getElementById('anom_val');
        
        if (tmFill) {
            // Temperature Gauge: 0 to 30°C range
            const tm = data.TM || 10;
            const tmPct = Math.min(100, Math.max(0, (tm / 30) * 100));
            tmFill.style.strokeDasharray = `${tmPct}, 100`;
            if (tm > 22) tmFill.style.stroke = "#ff4d4d";
            else if (tm > 15) tmFill.style.stroke = "#fbbf24";
            else tmFill.style.stroke = "#34d399";
        }

        if (anomFill) {
            // Anomaly Gauge: -1 to +3°C range for visual impact
            const anom = data.ANOMALIE_TM || 0;
            const anomPct = Math.min(100, Math.max(0, ((anom + 1) / 4) * 100));
            anomFill.style.strokeDasharray = `${anomPct}, 100`;
            if (anom > 2) anomFill.style.stroke = "#ff4d4d";
            else if (anom > 0.5) anomFill.style.stroke = "#fbbf24";
            else anomFill.style.stroke = "#00d2ff";
            
            if (anomText) anomText.textContent = `${anom > 0 ? '+' : ''}${anom.toFixed(2)}°C`;
        }
    }

    function renderGHGSectorChart() {
        const canvas = document.getElementById('ghgSectorChart');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');

        if (ghgSectorChart) ghgSectorChart.destroy();

        // Get data from IGT JSON for current city
        const cityIgt = igtData[currentCity];
        let labels = ['Aéro/Autres', 'Agriculture', 'Industrie', 'Résidentiel', 'Tertiaire', 'Routier'];
        let data = [0, 0, 0, 0, 0, 0];

        if (cityIgt) {
            labels = ['Transport', 'Agriculture', 'Industrie', 'Résidentiel', 'Tertiaire', 'Déchets'];
            const others = (cityIgt["Autres transports"] || 0) + (cityIgt["Autres transports internationaux"] || 0);
            data = [
                (cityIgt.Routier || 0) + others,
                cityIgt.Agriculture || 0,
                cityIgt["Industrie (hors prod. centr. d'énergie)"] || 0,
                cityIgt.Residentiel || 0,
                cityIgt.Tertiaire || 0,
                cityIgt.Dechets || 0
            ];
        } else {
            // Fallback to national stats if city not in IGT
            data = [34, 21, 17, 15, 9, 4];
        }

        const total = data.reduce((a, b) => a + b, 0);

        ghgSectorChart = new Chart(ctx, {
            type: 'doughnut',
            plugins: [ChartDataLabels],
            data: {
                labels: labels,
                datasets: [{
                    data: data,
                    backgroundColor: ['#3aedc4', '#f8fafc', '#00d2ff', '#fbbf24', '#ff4d4d', '#cbd5e1'],
                    borderWidth: 0
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: {
                        top: 10,
                        bottom: 30, // Extra space for labels
                        left: 10,
                        right: 20  // Extra space for legend
                    }
                },
                plugins: {
                    legend: {
                        position: 'right',
                        labels: {
                            color: '#ffffff',
                            font: { size: 11, weight: 'bold' },
                            padding: 15,
                            boxWidth: 15
                        }
                    },
                    datalabels: {
                        color: '#ffffff',
                        font: { weight: 'bold', size: 11 },
                        formatter: (value) => {
                            if (total === 0) return '';
                            const pct = (value / total * 100).toFixed(1);
                            return pct > 5 ? pct + '%' : ''; // Lower threshold for more details
                        },
                        textShadowColor: 'rgba(0,0,0,0.8)',
                        textShadowBlur: 6,
                        anchor: 'center',
                        align: 'center'
                    },
                    tooltip: {
                        callbacks: {
                            label: function(context) {
                                const val = context.raw;
                                const pct = total > 0 ? (val / total * 100).toFixed(1) : 0;
                                return ` ${context.label}: ${Math.round(val).toLocaleString()} t (${pct}%)`;
                            }
                        }
                    }
                }
            }
        });
    }

    function addMessage(text, type) {
        const id = 'msg-' + Date.now();
        const div = document.createElement('div');
        div.id = id;
        div.className = `message ${type}`;
        
        // Use marked to parse markdown if it's available and it's a bot message
        if (type.includes('bot') && typeof marked !== 'undefined') {
            div.innerHTML = marked.parse(text);
        } else {
            div.innerHTML = text; // Intentional for <b> tags or simple text
        }
        
        chatMsgs.appendChild(div);
        chatMsgs.scrollTop = chatMsgs.scrollHeight;
        return id;
    }
});
