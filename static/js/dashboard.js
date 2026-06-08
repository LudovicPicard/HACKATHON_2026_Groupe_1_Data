
document.addEventListener('DOMContentLoaded', () => {
    let allData = [];
    let projectionData = [];
    let currentCity = "";
    let currentModel = "Prophet";
    let currentFrame = "RCP";
    
    // Charts instances
    let evolutionChart, extremeDaysChart, anomalyChart, projectionsChart, seaTempChart;
    let ghgSectorChart;
    let map, markers = {}, deforLayer = null;
    let simulationYear = 2026;
    let performanceData = [];
    let igtData = {};
    let deforData = {};
    let seaTempData = [];
    let meteoLayer, igtLayer, seaTempLayer, neutralLandLayer;
    let currentLayer = "meteo";
    let currentProfile = "citoyen"; // "citoyen" or "collectivite"
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
    let cityCoords = {
        "France (avec Outre-mer)": [46.2276, 2.2137],
        "France (sans Outre-mer)": [46.2276, 2.2137]
    };

    let cityToDept = {
        "France (avec Outre-mer)": "🇫🇷 France (Moyenne avec DOM-TOM)",
        "France (sans Outre-mer)": "🇫🇷 France (Moyenne sans DOM-TOM)"
    };

    function getDisplayName(city) {
        return cityToDept[city] || city;
    }

    function mapGeoJsonCodeToDbCode(code) {
        if (code === '2A' || code === '2B') return '20';
        if (code === '976') return '985';
        return code;
    }

    function isCodeMatch(featureCode, activeCode) {
        if (activeCode === '20' && (featureCode === '2A' || featureCode === '2B')) return true;
        if (activeCode === '985' && featureCode === '976') return true;
        return featureCode === activeCode;
    }

    function isFrance(city) {
        return city === "France" || city === "France (avec Outre-mer)" || city === "France (sans Outre-mer)";
    }
    
    // 1. Initial Load (including department coordinates and GeoJSON boundaries)
    Promise.all([
        fetch('/api/deforestation').then(res => res.json()),
        fetch('/api/cities').then(res => res.json()),
        fetch('/api/data').then(res => res.json()),
        fetch('/api/projections').then(res => res.json()),
        fetch('/api/performance').then(res => res.json()),
        fetch('/api/igt').then(res => res.json()),
        fetch('/api/departments').then(res => res.json()),
        fetch('/static/js/departements_min.geojson').then(res => res.json()),
        fetch('/api/sea-temperature').then(res => res.json())
    ]).then(([deforRaw, cities, data, projections, performance, igt, departments, geojsonData, seaTempRaw]) => {
        // Transform deforestation data into lookup by department name
        const deforLookup = {};
        deforRaw.forEach(item => {
            const deptName = item.departement || item.dept || item.fullName || '';
            const loss = item.loss_ha || item.loss || 0;
            deforLookup[deptName] = loss;
        });
        // Assign to global variable for later use
        window.deforData = deforLookup;
        // also assign to local variable for convenience
        deforData = deforLookup;
        seaTempData = seaTempRaw;

        // Dynamically populate coordinates and display names from the backend mapping
        Object.keys(departments).forEach(code => {
            const dept = departments[code];
            const fullName = `${dept.dept_name} (${dept.code})`;
            cityCoords[fullName] = [dept.lat, dept.lon];
            cityToDept[fullName] = `${dept.dept_name} (${dept.code})`;
        });

        // Override overseas department coordinates with shifted map positions
        const shiftedCoords = {
            "Guadeloupe (971)": [51.023, -7.324],
            "Martinique (972)": [48.931, -7.043],
            "Guyane (973)": [46.823, -7.224],
            "La Réunion (974)": [44.945, -7.238],
            "Mayotte (985)": [42.995, -7.234]
        };
        Object.keys(shiftedCoords).forEach(fullName => {
            if (cityCoords[fullName]) {
                cityCoords[fullName] = shiftedCoords[fullName];
            }
        });

        console.log("Données chargées:", { cities: cities.length, data: data.length, projections: projections.length, performance: performance.length, igt: Object.keys(igt).length });
        citySelect.innerHTML = '<option value="">Sélectionnez un département...</option>' + 
                               cities.map(city => `<option value="${city}">${getDisplayName(city)}</option>`).join('');
        allData = data;
        projectionData = projections;
        performanceData = performance;
        igtData = igt;

        initMap(cities, geojsonData);

        if (cities.length > 0) {
            currentCity = "France (sans Outre-mer)";
            citySelect.value = "France (sans Outre-mer)";
            initProfileSwitcher();
            updateProfileContent(); 
            initOnboardingModal();
            highlightMarker("France (sans Outre-mer)");
            showMapInfo("France (sans Outre-mer)", currentLayer);
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
        highlightMarker(currentCity);
        if (currentCity) {
            showMapInfo(currentCity, currentLayer);
        } else {
            document.getElementById('map-info-panel').classList.add('hidden');
        }
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
    document.getElementById('showDeforMap').addEventListener('click', () => switchMapLayer('defor'));
    document.getElementById('showSeaTemp').addEventListener('click', () => switchMapLayer('sea'));


    function updateLegend() {
        const legend = document.getElementById('map-legend');
        if (!legend) return;
        
        if (currentLayer === 'meteo') {
            legend.querySelector('.legend-title').textContent = "Température Moyenne (°C)";
            legend.querySelector('.scale-bar').style.background = "linear-gradient(to right, #3b82f6, #2dd4bf, #10b981, #eab308, #ef4444)";
            legend.querySelector('.legend-labels').innerHTML = `
                <span>< 8°C (Frais)</span>
                <span>12°C (Tempéré)</span>
                <span>> 16°C (Chaud)</span>
            `;
            legend.classList.remove('hidden');
        } else if (currentLayer === 'igt') {
            legend.querySelector('.legend-title').textContent = "Émissions CO2e (millions de tonnes)";
            legend.querySelector('.scale-bar').style.background = "linear-gradient(to right, #34d399, #fbbf24, #ef4444)";
            legend.querySelector('.legend-labels').innerHTML = `
                <span>< 1.0M (Bas)</span>
                <span>4.2M (Médian)</span>
                <span>> 8.0M (Élevé)</span>
            `;
            legend.classList.remove('hidden');
        } else if (currentLayer === 'defor') {
            legend.querySelector('.legend-title').textContent = "Déforestation (ha)";
            legend.querySelector('.scale-bar').style.background = "linear-gradient(to right, #4ade80, #ef4444)";
            legend.querySelector('.legend-labels').innerHTML = `
                <span>0</span>
                <span>> 8000</span>
            `;
            legend.classList.remove('hidden');
        } else if (currentLayer === 'sea') {
            legend.querySelector('.legend-title').textContent = "Température de la Mer (°C)";
            legend.querySelector('.scale-bar').style.background = "linear-gradient(to right, #3b82f6, #2dd4bf, #10b981, #eab308, #ef4444)";
            legend.querySelector('.legend-labels').innerHTML = `
                <span>< 11°C</span>
                <span>16°C</span>
                <span>> 21°C</span>
            `;
            legend.classList.remove('hidden');
        }
    }

    function switchMapLayer(layer) {
        currentLayer = layer;
        document.getElementById('showMeteo').classList.toggle('active', layer === 'meteo');
        document.getElementById('showEmissions').classList.toggle('active', layer === 'igt');
        document.getElementById('showDeforMap').classList.toggle('active', layer === 'defor');
        document.getElementById('showSeaTemp').classList.toggle('active', layer === 'sea');
        const panel = document.getElementById('map-info-panel');
        const isPanelVisible = panel && !panel.classList.contains('hidden');
        updateLegend();
        
        // Remove all layers first
        if (meteoLayer && map.hasLayer(meteoLayer)) map.removeLayer(meteoLayer);
        if (igtLayer && map.hasLayer(igtLayer)) map.removeLayer(igtLayer);
        if (deforLayer && map.hasLayer(deforLayer)) map.removeLayer(deforLayer);
        if (seaTempLayer && map.hasLayer(seaTempLayer)) map.removeLayer(seaTempLayer);
        
        if (layer === 'meteo' && meteoLayer) {
            meteoLayer.addTo(map);
            updateMapStyles();
        } else if (layer === 'igt' && igtLayer) {
            igtLayer.addTo(map);
        } else if (layer === 'defor' && deforLayer) {
            deforLayer.addTo(map);
        } else if (layer === 'sea' && seaTempLayer) {
            seaTempLayer.addTo(map);
            updateMapStyles();
        }
        
        if (currentCity) {
            highlightMarker(currentCity);
            if (isPanelVisible) {
                showMapInfo(currentCity, layer);
            }
        }
    }

    function getDeforColor(val) {
        const maxLoss = 8000; // max ha for scaling
        const ratio = Math.min(1, Math.max(0, val / maxLoss));
        const hue = (1 - ratio) * 120; // green to red
        return `hsl(${hue}, 80%, 45%)`;
    }

    function getEmissionsColor(val) {
        const minVal = 1000000;
        const maxVal = 8000000;
        const ratio = Math.min(1, Math.max(0, (val - minVal) / (maxVal - minVal)));
        const hue = (1 - ratio) * 120; // 120 (green) to 0 (red)
        return `hsl(${hue}, 80%, 45%)`;
    }

    function getMeteoColor(temp) {
        if (temp === null || temp === undefined) return 'rgba(255, 255, 255, 0.05)';
        // Normalize between 8°C and 16°C for France
        const minT = 8;
        const maxT = 16;
        const ratio = Math.min(1, Math.max(0, (temp - minT) / (maxT - minT)));
        // HSL Hue: 240 (Blue/Cold) to 0 (Red/Hot)
        const hue = (1 - ratio) * 240;
        return `hsl(${hue}, 80%, 45%)`;
    }

    function getTempForCity(city) {
        const cityData = allData.filter(d => d.VILLE === city);
        if (cityData.length === 0) return null;
        
        if (simulationMode === "single") {
            const yearData = cityData.find(d => Number(d.ANNEE) === Number(simulationYear));
            return yearData ? yearData.TM : null;
        } else {
            const years = cityData.filter(d => Number(d.ANNEE) >= rangeStartYear && Number(d.ANNEE) <= rangeEndYear);
            if (years.length === 0) return null;
            return years.reduce((s, y) => s + (y.TM || 0), 0) / years.length;
        }
    }
    function updateMapStyles() {
        if (currentLayer === 'meteo' && meteoLayer) {
            meteoLayer.eachLayer(function(layer) {
                if (layer.feature && layer.feature.properties) {
                    const code = mapGeoJsonCodeToDbCode(layer.feature.properties.code);
                    const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                    if (fullName) {
                        const temp = getTempForCity(fullName);
                        layer.setStyle({
                            fillColor: getMeteoColor(temp)
                        });
                    }
                }
            });
        } else if (currentLayer === 'sea' && seaTempLayer) {
            seaTempLayer.eachLayer(function(layer) {
                if (layer.feature && layer.feature.properties) {
                    const code = layer.feature.properties.code;
                    const temp = getSeaTempForDept(code);
                    const hasData = temp !== null;
                    layer.setStyle({
                        fillColor: hasData ? getSeaTempColor(temp) : 'rgba(122, 122, 122, 0.15)'
                    });
                }
            });
        }
    }

    function getSeaTempForDept(deptCode) {
        if (!seaTempData || seaTempData.length === 0) return null;
        
        let startYear = simulationMode === "single" ? 1973 : rangeStartYear;
        let endYear = simulationMode === "single" ? simulationYear : rangeEndYear;
        
        if (startYear < 1973) startYear = 1973;
        if (endYear > 2026) endYear = 2026;
        if (endYear < startYear) endYear = startYear;
        
        let filtered;
        if (deptCode === "FRANCE") {
            filtered = seaTempData.filter(d => 
                d.ANNEE >= startYear && 
                d.ANNEE <= endYear
            );
        } else {
            filtered = seaTempData.filter(d => 
                String(d.DEPARTEMENT) === String(deptCode) && 
                d.ANNEE >= startYear && 
                d.ANNEE <= endYear
            );
        }
        
        if (filtered.length === 0) return null;
        const sum = filtered.reduce((s, d) => s + d.TEMPERATURE, 0);
        return sum / filtered.length;
    }

    function getSeaTempForZone(zoneName) {
        if (!seaTempData || seaTempData.length === 0) return null;
        
        let startYear = simulationMode === "single" ? 1973 : rangeStartYear;
        let endYear = simulationMode === "single" ? simulationYear : rangeEndYear;
        
        if (startYear < 1973) startYear = 1973;
        if (endYear > 2026) endYear = 2026;
        if (endYear < startYear) endYear = startYear;
        
        const filtered = seaTempData.filter(d => 
            d.ZONE_MARINE === zoneName && 
            d.ANNEE >= startYear && 
            d.ANNEE <= endYear
        );
        
        if (filtered.length === 0) return null;
        const sum = filtered.reduce((s, d) => s + d.TEMPERATURE, 0);
        return sum / filtered.length;
    }

    function getSeaTempColor(temp) {
        if (temp === null || temp === undefined) return 'rgba(122, 122, 122, 0.15)';
        const minT = 10;
        const maxT = 22;
        const ratio = Math.min(1, Math.max(0, (temp - minT) / (maxT - minT)));
        const hue = (1 - ratio) * 240; // 240 (blue) to 0 (red)
        return `hsl(${hue}, 80%, 45%)`;
    }

    function initMap(cities, geojsonData) {
        map = L.map('map', {
            zoomControl: true,
            attributionControl: false,
            dragging: true,
            scrollWheelZoom: true,
            doubleClickZoom: true,
            boxZoom: true,
            touchZoom: true
        }).setView([46.4033, 2.3883], 5.2);

        L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
            maxZoom: 19
        }).addTo(map);
        
        // 1. Meteo Layer as GeoJSON Choropleth Map (colored by temperature)
        meteoLayer = L.geoJSON(geojsonData, {
            style: function(feature) {
                const code = mapGeoJsonCodeToDbCode(feature.properties.code);
                const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                const temp = getTempForCity(fullName);
                
                return {
                    fillColor: getMeteoColor(temp),
                    weight: 1.5,
                    opacity: 0.8,
                    color: 'rgba(255, 255, 255, 0.25)',
                    fillOpacity: 0.55
                };
            },
            onEachFeature: function(feature, layer) {
                const code = mapGeoJsonCodeToDbCode(feature.properties.code);
                const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                
                if (fullName) {
                    layer.bindPopup(`<b>${fullName}</b>`);
                    
                    layer.on('click', () => {
                        currentCity = fullName;
                        citySelect.value = fullName;
                        updateDashboard(fullName);
                        highlightMarker(fullName);
                        showMapInfo(fullName, 'meteo');
                    });
                }
                
                layer.on('mouseover', function(e) {
                    this.setStyle({
                        weight: 2.5,
                        color: '#fff',
                        fillOpacity: 0.75
                    });
                });
                
                layer.on('mouseout', function(e) {
                    meteoLayer.resetStyle(this);
                    if (fullName === currentCity) {
                        this.setStyle({
                            weight: 3,
                            color: "#fbbf24",
                            fillOpacity: 0.75
                        });
                    }
                });
            }
        }).addTo(map);

        // Add small station dots inside meteoLayer for precision
        cities.forEach(city => {
            if (cityCoords[city] && !isFrance(city)) {
                L.circleMarker(cityCoords[city], {
                    radius: 3,
                    fillColor: "#fff",
                    color: "#000",
                    weight: 1,
                    opacity: 0.6,
                    fillOpacity: 0.8,
                    interactive: false
                }).addTo(meteoLayer);
            }
        });

        // 2. IGT Layer as GeoJSON Choropleth Map (colored by emissions)
        igtLayer = L.geoJSON(geojsonData, {
            style: function(feature) {
                const code = mapGeoJsonCodeToDbCode(feature.properties.code);
                const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                const cityIgt = igtData[fullName];
                const emissions = cityIgt ? (cityIgt.TOTAL_CO2e || 0) : 0;
                return {
                    fillColor: getEmissionsColor(emissions),
                    weight: 1.5,
                    opacity: 0.8,
                    color: 'rgba(255, 255, 255, 0.25)',
                    fillOpacity: 0.5
                };
            },
            onEachFeature: function(feature, layer) {
                const code = mapGeoJsonCodeToDbCode(feature.properties.code);
                const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                if (fullName) {
                    const cityIgt = igtData[fullName];
                    const totalEmissions = cityIgt ? Math.round(cityIgt.TOTAL_CO2e).toLocaleString() : '0';
                    layer.bindPopup(`<b>${fullName}</b><br>Émissions: <b>${totalEmissions} tCO2e</b>`);
                    layer.on('click', () => {
                        currentCity = fullName;
                        citySelect.value = fullName;
                        updateDashboard(fullName);
                        highlightMarker(fullName);
                        showMapInfo(fullName, 'igt');
                    });
                }
                layer.on('mouseover', function(e) {
                    this.setStyle({
                        weight: 2.5,
                        color: '#fff',
                        fillOpacity: 0.75
                    });
                });
                layer.on('mouseout', function(e) {
                    igtLayer.resetStyle(this);
                    if (fullName === currentCity) {
                        this.setStyle({
                            weight: 3,
                            color: "#fbbf24",
                            fillOpacity: 0.75
                        });
                    }
                });
            }
        });
        // 3. Deforestation Layer (colored by tree loss)
        deforLayer = L.geoJSON(geojsonData, {
            style: function(feature) {
                const code = mapGeoJsonCodeToDbCode(feature.properties.code);
                const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                const loss = (window.deforData && window.deforData[fullName]) || 0;
                return {
                    fillColor: getDeforColor(loss),
                    weight: 1.5,
                    opacity: 0.8,
                    color: 'rgba(255, 255, 255, 0.25)',
                    fillOpacity: 0.55
                };
            },
            onEachFeature: function(feature, layer) {
                const code = mapGeoJsonCodeToDbCode(feature.properties.code);
                const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                if (fullName) {
                    const loss = deforData[fullName] || 0;
                    layer.bindPopup(`<b>${fullName}</b><br>Déforestation: <b>${loss} ha</b>`);
                    layer.on('click', () => {
                        currentCity = fullName;
                        citySelect.value = fullName;
                        updateDashboard(fullName);
                        highlightMarker(fullName);
                        showMapInfo(fullName, 'defor');
                    });
                }
                layer.on('mouseover', function(e) {
                    this.setStyle({
                        weight: 2.5,
                        color: '#fff',
                        fillOpacity: 0.75
                    });
                });
                layer.on('mouseout', function(e) {
                    deforLayer.resetStyle(this);
                    if (fullName === currentCity) {
                        this.setStyle({
                            weight: 3,
                            color: "#fbbf24",
                            fillOpacity: 0.75
                        });
                    }
                });
            }
        });

        // 4. Sea Temperature Layer as GeoJSON Choropleth Map (only coastal departments colored, others grayed)
        seaTempLayer = L.geoJSON(geojsonData, {
            style: function(feature) {
                const code = feature.properties.code;
                const temp = getSeaTempForDept(code);
                const hasData = temp !== null;
                
                return {
                    fillColor: hasData ? getSeaTempColor(temp) : 'rgba(122, 122, 122, 0.15)',
                    weight: 1.5,
                    opacity: 0.8,
                    color: hasData ? 'rgba(255, 255, 255, 0.5)' : 'rgba(255, 255, 255, 0.15)',
                    fillOpacity: hasData ? 0.65 : 0.2
                };
            },
            onEachFeature: function(feature, layer) {
                const code = feature.properties.code;
                const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
                const temp = getSeaTempForDept(code);
                
                if (fullName) {
                    if (temp !== null) {
                        layer.bindPopup(`<b>${fullName}</b><br>Temp. de la mer: <b>${temp.toFixed(2)} °C</b>`);
                    } else {
                        layer.bindPopup(`<b>${fullName}</b><br>(Pas de données maritimes directes)`);
                    }
                    
                    layer.on('click', () => {
                        currentCity = fullName;
                        citySelect.value = fullName;
                        updateDashboard(fullName);
                        highlightMarker(fullName);
                        showMapInfo(fullName, 'sea');
                    });
                }
                
                layer.on('mouseover', function(e) {
                    this.setStyle({
                        weight: 2.5,
                        color: '#fff',
                        fillOpacity: temp !== null ? 0.85 : 0.3
                    });
                });
                
                layer.on('mouseout', function(e) {
                    seaTempLayer.resetStyle(this);
                    if (fullName === currentCity) {
                        this.setStyle({
                            weight: 3,
                            color: "#fbbf24",
                            fillOpacity: temp !== null ? 0.85 : 0.3
                        });
                    }
                });
            }
        });

        if (cities.length > 0) highlightMarker(currentCity);
        updateLegend();

        // Close panel on map click
        map.on('click', (e) => {
            // Only hide if the click is on the map background, not on features
            if (e.originalEvent.target.id === 'map') {
                document.getElementById('map-info-panel').classList.add('hidden');
            }
        });
    }

    function highlightMarker(cityName) {
        const match = cityName.match(/\((\w+)\)/);
        const activeCode = match ? match[1] : null;
        
        if (meteoLayer && currentLayer === 'meteo') {
            meteoLayer.eachLayer(function(layer) {
                if (layer.feature && layer.feature.properties) {
                    const isActive = isCodeMatch(layer.feature.properties.code, activeCode);
                    layer.setStyle({
                        weight: isActive ? 3 : 1.5,
                        color: isActive ? "#fbbf24" : 'rgba(255, 255, 255, 0.25)',
                        fillOpacity: isActive ? 0.75 : 0.55
                    });
                    if (isActive && typeof layer.bringToFront === 'function') {
                        layer.bringToFront();
                    }
                }
            });
        } else if (igtLayer && currentLayer === 'igt') {
            igtLayer.eachLayer(function(layer) {
                if (layer.feature && layer.feature.properties) {
                    const isActive = isCodeMatch(layer.feature.properties.code, activeCode);
                    layer.setStyle({
                        weight: isActive ? 3 : 1.5,
                        color: isActive ? "#fbbf24" : 'rgba(255, 255, 255, 0.25)',
                        fillOpacity: isActive ? 0.75 : 0.5
                    });
                    if (isActive && typeof layer.bringToFront === 'function') {
                        layer.bringToFront();
                    }
                }
            });
        } else if (deforLayer && currentLayer === 'defor') {
            deforLayer.eachLayer(function(layer) {
                if (layer.feature && layer.feature.properties) {
                    const isActive = isCodeMatch(layer.feature.properties.code, activeCode);
                    layer.setStyle({
                        weight: isActive ? 3 : 1.5,
                        color: isActive ? "#fbbf24" : 'rgba(255, 255, 255, 0.25)',
                        fillOpacity: isActive ? 0.75 : 0.55
                    });
                    if (isActive && typeof layer.bringToFront === 'function') {
                        layer.bringToFront();
                    }
                }
            });
        } else if (seaTempLayer && currentLayer === 'sea') {
            seaTempLayer.eachLayer(function(layer) {
                if (layer.feature && layer.feature.properties) {
                    const isActive = isCodeMatch(layer.feature.properties.code, activeCode);
                    const code = layer.feature.properties.code;
                    const temp = getSeaTempForDept(code);
                    const hasData = temp !== null;
                    layer.setStyle({
                        weight: isActive ? 3 : 1.5,
                        color: isActive ? "#fbbf24" : (hasData ? 'rgba(255, 255, 255, 0.5)' : 'rgba(255, 255, 255, 0.15)'),
                        fillOpacity: isActive ? 0.85 : (hasData ? 0.65 : 0.2)
                    });
                    if (isActive && typeof layer.bringToFront === 'function') {
                        layer.bringToFront();
                    }
                }
            });
        }
    }

    function showMapInfo(city, mode) {
        const panel = document.getElementById('map-info-panel');
        if (!panel) return;

        // Close button HTML
        const closeBtnHtml = `<button class="panel-close-btn" onclick="this.parentElement.classList.add('hidden')" style="position: absolute; top: 12px; right: 15px; background: none; border: none; color: var(--text-muted); cursor: pointer; font-size: 1rem; transition: color 0.2s;"><i class="fa-solid fa-xmark"></i></button>`;

        if (mode === 'meteo') {
            const cityData = allData.filter(d => d.VILLE === city);
            let tempVal = "--";
            let rainVal = "--";
            let frostVal = "--";
            let caniculeVal = "--";
            
            if (simulationMode === "single") {
                const yearData = cityData.find(d => Number(d.ANNEE) === Number(simulationYear));
                if (yearData) {
                    tempVal = (yearData.TM || 0).toFixed(1) + " °C";
                    rainVal = (yearData.RR_TOTAL || 0).toFixed(0) + " mm";
                    frostVal = Math.round(yearData.DAYS_FROST || 0) + " j";
                    caniculeVal = Math.round(yearData.DAYS_CANICULE || 0) + " j";
                }
            } else {
                const years = cityData.filter(d => Number(d.ANNEE) >= rangeStartYear && Number(d.ANNEE) <= rangeEndYear);
                if (years.length > 0) {
                    const count = years.length;
                    tempVal = (years.reduce((s, y) => s + (y.TM || 0), 0) / count).toFixed(1) + " °C";
                    rainVal = (years.reduce((s, y) => s + (y.RR_TOTAL || 0), 0) / count).toFixed(0) + " mm";
                    frostVal = Math.round(years.reduce((s, y) => s + (y.DAYS_FROST || 0), 0) / count) + " j/an";
                    caniculeVal = Math.round(years.reduce((s, y) => s + (y.DAYS_CANICULE || 0), 0) / count) + " j/an";
                }
            }
            
            panel.innerHTML = `
                ${closeBtnHtml}
                <h3><i class="fa-solid fa-cloud-sun"></i> ${getDisplayName(city)}</h3>
                <div class="igt-stats">
                    <div class="igt-stat-row">
                        <span class="igt-label">🌡️ Temp. Moyenne</span>
                        <span class="igt-val" style="color: var(--text-primary)">${tempVal}</span>
                    </div>
                    <div class="igt-stat-row">
                        <span class="igt-label">🌧️ Précipitations</span>
                        <span class="igt-val" style="color: var(--text-primary)">${rainVal}</span>
                    </div>
                    <div class="igt-stat-row">
                        <span class="igt-label">❄️ Jours de Gel</span>
                        <span class="igt-val" style="color: var(--text-primary)">${frostVal}</span>
                    </div>
                    <div class="igt-stat-row">
                        <span class="igt-label">🔥 Canicules</span>
                        <span class="igt-val" style="color: var(--text-primary)">${caniculeVal}</span>
                    </div>
                </div>
                <div class="igt-popup-footer" style="color: var(--text-muted)">Données pour ${simulationMode === "single" ? simulationYear : rangeStartYear + "-" + rangeEndYear}</div>
            `;
        } else if (mode === 'defor') {
            let loss = deforData[city] || 0;
            if (isFrance(city)) {
                loss = 0;
                const isWithDom = (city === "France (avec Outre-mer)");
                Object.keys(deforData).forEach(key => {
                    const isDom = /\((97\d|98\d)\)$/.test(key);
                    if (isWithDom || !isDom) {
                        loss += deforData[key] || 0;
                    }
                });
            }
            panel.innerHTML = `
                ${closeBtnHtml}
                <h3><i class="fa-solid fa-tree"></i> ${getDisplayName(city)}</h3>
                <div class="igt-stats">
                    <div class="igt-stat-row">
                        <span class="igt-label">🌳 Perte forestière</span>
                        <span class="igt-val" style="color: var(--accent-sun)">${Math.round(loss).toLocaleString()} ha</span>
                    </div>
                </div>
                <div class="igt-popup-footer" style="color: var(--text-muted)">Source Global Forest Watch (2025)</div>
            `;
        } else if (mode === 'sea') {
            const match = city.match(/\((\d{2,3}|2A|2B)\)/);
            const deptCode = match ? match[1] : null;
            
            const isDom = deptCode && (deptCode.startsWith('97') || deptCode.startsWith('98'));
            const isFranceVal = isFrance(city);
            
            let temp = null;
            let displayName = getDisplayName(city);
            
            if (isFranceVal || isDom) {
                temp = getSeaTempForDept("FRANCE");
                displayName = isFranceVal ? displayName : `${displayName} (Moyenne France)`;
            } else if (deptCode) {
                temp = getSeaTempForDept(deptCode);
            }
            
            if (temp === null) {
                panel.innerHTML = `
                    ${closeBtnHtml}
                    <h3><i class="fa-solid fa-water"></i> ${getDisplayName(city)}</h3>
                    <p style="font-size:0.85rem; margin-top: 15px; color: var(--text-secondary); line-height: 1.5;">Ce département (<b>${getDisplayName(city)}</b>) n'est pas sur le littoral ou ne dispose pas de données de température de mer dans le fichier.<br><br>Sélectionnez un département côtier coloré sur la carte.</p>
                `;
            } else {
                panel.innerHTML = `
                    ${closeBtnHtml}
                    <h3><i class="fa-solid fa-water"></i> ${displayName}</h3>
                    <div class="igt-stats">
                        <div class="igt-stat-row">
                            <span class="igt-label">🌡️ Temp. Mer (SST)</span>
                            <span class="igt-val" style="color: var(--accent-blue)">${temp.toFixed(2)} °C</span>
                        </div>
                    </div>
                    <div class="igt-popup-footer" style="color: var(--text-muted)">Historique de température côtière (${simulationMode === "single" ? simulationYear : rangeStartYear + "-" + rangeEndYear})</div>
                `;
            }
        } else {
            let cityIgt = igtData[city];
            if (isFrance(city)) {
                const isWithDom = (city === "France (avec Outre-mer)");
                // Aggregate departments for France total
                cityIgt = {
                    Residentiel: 0,
                    Routier: 0,
                    "Industrie (hors prod. centr. d'énergie)": 0,
                    Tertiaire: 0,
                    Agriculture: 0,
                    TOTAL_CO2e: 0
                };
                Object.keys(igtData).forEach(key => {
                    const dept = igtData[key];
                    const isDom = /\((97\d|98\d)\)$/.test(key);
                    if (isWithDom || !isDom) {
                        cityIgt.Residentiel += (dept.Residentiel || 0);
                        cityIgt.Routier += (dept.Routier || 0);
                        cityIgt["Industrie (hors prod. centr. d'énergie)"] += (dept["Industrie (hors prod. centr. d'énergie)"] || 0);
                        cityIgt.Tertiaire += (dept.Tertiaire || 0);
                        cityIgt.Agriculture += (dept.Agriculture || 0);
                        cityIgt.TOTAL_CO2e += (dept.TOTAL_CO2e || 0);
                    }
                });
            }

            if (!cityIgt) return;
            const total = cityIgt.TOTAL_CO2e || 0;
            const sectors = [
                { l: "🏠 Résidentiel", v: cityIgt.Residentiel },
                { l: "🚛 Transport Routier", v: cityIgt.Routier },
                { l: "🏭 Industrie", v: cityIgt["Industrie (hors prod. centr. d'énergie)"] },
                { l: "💼 Tertiaire", v: cityIgt.Tertiaire },
                { l: "🚜 Agriculture", v: cityIgt.Agriculture }
            ].sort((a,b) => b.v - a.v);

            const icon = isFrance(city) ? "fa-solid fa-flag" : "fa-solid fa-industry";
            panel.innerHTML = `
                ${closeBtnHtml}
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
    
    function updateDashboard(city) {
        // Coastal department auto-selection for marine zone
        if (city) {
            const match = city.match(/\((\d{2,3}|2A|2B)\)/);
            const deptCode = match ? match[1] : null;
            if (deptCode) {
                const DEPT_TO_SEA_ZONE = {
                    "13": "Méditerranée Occidentale",
                    "06": "Méditerranée Occidentale",
                    "83": "Méditerranée Occidentale",
                    "30": "Méditerranée Occidentale",
                    "34": "Méditerranée Occidentale",
                    "20": "Méditerranée Occidentale",
                    "2A": "Méditerranée Occidentale",
                    "2B": "Méditerranée Occidentale",
                    "33": "Golfe de Gascogne Sud",
                    "40": "Golfe de Gascogne Sud",
                    "64": "Golfe de Gascogne Sud",
                    "85": "Golfe de Gascogne Sud",
                    "17": "Golfe de Gascogne Sud",
                    "44": "Golfe de Gascogne Nord",
                    "56": "Golfe de Gascogne Nord",
                    "29": "Mer Celtique",
                    "22": "Mer Celtique",
                    "35": "Mer Celtique",
                    "76": "Manche / Mer du Nord",
                    "80": "Manche / Mer du Nord",
                    "62": "Manche / Mer du Nord",
                    "59": "Manche / Mer du Nord",
                    "14": "Manche / Mer du Nord",
                    "50": "Manche / Mer du Nord"
                };
                if (DEPT_TO_SEA_ZONE[deptCode]) {
                    const zoneSelect = document.getElementById('seaZoneSelect');
                    if (zoneSelect) {
                        zoneSelect.value = DEPT_TO_SEA_ZONE[deptCode];
                    }
                }
            }
        }

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
        updateAdvice(displayData, city);

        // Update Performance Table (Step 3)
        updatePerformanceTable(city);

        // Update Gauges (Step 4)
        updateGauges(displayData, city);

        // Update New Charts (Step 2)
        renderGHGSectorChart();
        renderSeaTempTable();

        // Update Map Styles dynamically
        updateMapStyles();

        // Update Map Info Panel if visible
        const panel = document.getElementById('map-info-panel');
        if (panel && !panel.classList.contains('hidden')) {
            showMapInfo(city, currentLayer);
        }
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
                        borderColor: '#3a8c6e',
                        backgroundColor: 'rgba(58, 140, 110, 0.06)',
                        fill: true,
                        tension: 0.4,
                        yAxisID: 'y'
                    },
                    {
                        label: 'Précipitations (mm)',
                        data: rrData,
                        borderColor: '#38bdf8',
                        backgroundColor: 'rgba(56, 189, 248, 0.15)',
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
                        grid: { color: '#e5e5e5' },
                        ticks: { color: '#161616' }
                    },
                    y1: {
                        type: 'linear',
                        position: 'right',
                        grid: { drawOnChartArea: false },
                        ticks: { color: '#161616' }
                    },
                    x: {
                        grid: { display: false },
                        ticks: { 
                            color: '#161616',
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 25,
                            display: true
                        },
                        title: {
                            display: true,
                            text: 'Année',
                            color: '#161616',
                            font: { size: 14 }
                        }
                    }
                },
                plugins: {
                    legend: { labels: { color: '#161616', font: { family: 'Inter' } } },
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
                        backgroundColor: 'rgba(239, 112, 37, 0.8)',
                        borderRadius: 4
                    },
                    {
                        label: 'Jours de gel',
                        data: frostData,
                        backgroundColor: 'rgba(56, 189, 248, 0.7)',
                        borderRadius: 4
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
                            color: '#161616',
                            autoSkip: true,
                            maxTicksLimit: 25,
                            display: true,
                            font: { weight: 'bold' }
                        } 
                    },
                    y: { stacked: true, grid: { color: '#e5e5e5' }, ticks: { color: '#161616' } }
                },
                plugins: {
                    legend: { labels: { color: '#161616' } },
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

    function renderSeaTempTable() {
        const table = document.getElementById('seaTempTable');
        if (!table) return;
        const tbody = table.querySelector('tbody');
        if (!tbody) return;

        if (!seaTempData || seaTempData.length === 0) {
            tbody.innerHTML = '<tr><td colspan="3" style="text-align:center; padding: 20px; color: var(--text-muted);">Données indisponibles</td></tr>';
            return;
        }

        const match = currentCity.match(/\((\d{2,3}|2A|2B)\)/);
        const selectedDeptCode = match ? match[1] : null;
        const isFranceVal = isFrance(currentCity);
        const isDom = selectedDeptCode && (selectedDeptCode.startsWith('97') || selectedDeptCode.startsWith('98'));

        // Helper to retrieve display names dynamically
        function getDeptNameByCode(code) {
            if (code === "FRANCE") return "France (Moyenne)";
            const fullName = Object.keys(cityToDept).find(key => key.endsWith(`(${code})`));
            if (fullName) return fullName;
            
            const fallbacks = {
                "2A": "Corse-du-Sud (2A)",
                "2B": "Haute-Corse (2B)"
            };
            return fallbacks[code] || `Département ${code}`;
        }

        // Sort departments numerically, placing Corsica (2A/2B) at code 20 position
        function getSortKey(code) {
            if (code === '2A') return 20.1;
            if (code === '2B') return 20.2;
            return parseFloat(code) || 999;
        }

        const uniqueCodes = [...new Set(seaTempData.map(d => String(d.DEPARTEMENT)))]
            .filter(code => code !== "FRANCE")
            .sort((a, b) => getSortKey(a) - getSortKey(b));

        const coastalDepts = [
            { code: "FRANCE", name: "France (Moyenne)" },
            ...uniqueCodes.map(code => ({ code: code, name: getDeptNameByCode(code) }))
        ];

        let html = '';
        coastalDepts.forEach(dept => {
            const currentTemp = getSeaTempForDept(dept.code);
            
            // Get baseline temperature (earliest year available, e.g. 1988)
            let baselineTemp = null;
            if (dept.code === "FRANCE") {
                const bData = seaTempData.filter(d => Number(d.ANNEE) === 1988 && d.TEMPERATURE !== null);
                if (bData.length > 0) {
                    baselineTemp = bData.reduce((s, d) => s + d.TEMPERATURE, 0) / bData.length;
                }
            } else {
                const bData = seaTempData.filter(d => String(d.DEPARTEMENT) === String(dept.code) && Number(d.ANNEE) === 1988 && d.TEMPERATURE !== null);
                if (bData.length > 0) {
                    baselineTemp = bData[0].TEMPERATURE;
                }
            }
            
            const tempStr = currentTemp ? `<strong>${currentTemp.toFixed(2)} °C</strong>` : '--';
            
            let diffHtml = '<span style="color: var(--text-muted)">--</span>';
            if (currentTemp && baselineTemp) {
                const diff = currentTemp - baselineTemp;
                const color = diff > 0 ? '#ef4444' : '#3b82f6';
                const sign = diff > 0 ? '+' : '';
                const arrow = diff > 0 ? '▲' : '▼';
                diffHtml = `<span style="color: ${color}; font-weight: 700; font-family: var(--font-heading);">${sign}${diff.toFixed(2)} °C ${arrow}</span>`;
            }

            const isFranceRow = dept.code === "FRANCE";
            const isHighlighted = (isFranceRow && (isFranceVal || isDom)) || (selectedDeptCode && dept.code === selectedDeptCode);
            const rowStyle = isHighlighted
                ? 'border-bottom: 2px solid var(--accent-blue); background-color: rgba(58, 140, 110, 0.22); font-weight: bold;' 
                : (isFranceRow ? 'border-bottom: 2px solid var(--accent-blue); background-color: rgba(58, 140, 110, 0.08); font-weight: bold;' : 'border-bottom: 1px solid var(--glass-border);');

            html += `
                <tr style="${rowStyle} transition: background-color 0.2s;">
                    <td style="padding: 12px 8px; font-weight: 600; color: var(--text-primary);"><i class="fa-solid fa-water" style="color: var(--accent-blue); margin-right: 8px; font-size: 0.8rem;"></i>${dept.name}</td>
                    <td style="padding: 12px 8px; color: var(--text-secondary);">${tempStr}</td>
                    <td style="padding: 12px 8px;">${diffHtml}</td>
                </tr>
            `;
        });

        tbody.innerHTML = html;
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
                    backgroundColor: anomalyData.map(v => v > 0 ? 'rgba(239, 68, 68, 0.8)' : 'rgba(56, 189, 248, 0.7)'),
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
                            color: '#161616',
                            autoSkip: true,
                            maxTicksLimit: 25,
                            display: true,
                            font: { weight: 'bold' }
                        } 
                    },
                    y: { grid: { color: '#e5e5e5' }, ticks: { color: '#161616' } }
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
                        borderColor: '#9ca3af',
                        borderWidth: 2,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: currentFrame === 'RCP' ? 'Scénario Optimiste (RCP 2.6)' : 'Scénario Durable (SSP1-2.6)',
                        data: optData,
                        borderColor: '#22c55e',
                        borderDash: [5, 5],
                        borderWidth: 2.5,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: currentFrame === 'RCP' ? 'Scénario Réaliste (RCP 4.5)' : 'Scénario Modéré (SSP2-4.5)',
                        data: medData,
                        borderColor: '#38bdf8',
                        borderWidth: 3,
                        pointRadius: 0,
                        fill: false
                    },
                    {
                        label: currentFrame === 'RCP' ? 'Scénario Pessimiste (RCP 8.5)' : 'Scénario Fossile (SSP5-8.5)',
                        borderColor: '#ef4444',
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
                    x: { grid: { display: false }, ticks: { color: '#161616', maxRotation: 0 } },
                    y: { 
                        grid: { color: '#e5e5e5' }, 
                        ticks: { color: '#161616' }, 
                        title: { display: true, text: 'Augmentation de Température (°C)', color: '#161616' } 
                    }
                },
                plugins: {
                    legend: { position: 'top', labels: { color: '#161616' } },
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

    function updateAdvice(data, city) {
        const container = document.getElementById('adviceContainer');
        if (!container) return;
        container.innerHTML = "";

        const advices = [];

        // Dynamic local ecological plan card
        let localAction = {};

        if (currentProfile === 'collectivite') {
            // COLLECTIVITE / DECIDEUR PUBLIC ACTIONS
            localAction = {
                icon: "fa-solid fa-map-pin",
                title: "Actions Locales (PCAET)",
                items: [
                    "Développement des énergies renouvelables locales",
                    "Végétalisation et lutte contre les îlots de chaleur",
                    "Rénovation énergétique des bâtiments publics"
                ]
            };

            if (isFrance(city)) {
                localAction = {
                    icon: "fa-solid fa-flag",
                    title: "Plan National (PNACC 3)",
                    items: [
                        "Stratégie nationale bas-carbone (Neutralité 2050)",
                        "Fonds Vert pour soutenir la transition locale",
                        "Développement des transports doux et ferroviaires"
                    ]
                };
            } else if (city.includes("Paris")) {
                localAction = {
                    icon: "fa-solid fa-tree",
                    title: "Transition à Paris",
                    items: [
                        "ZFE : Interdiction Crit'Air 4 et 5 étendue",
                        "Végétalisation : 500 cours d'écoles d'ici 2030",
                        "Déplacement : 1000 km de pistes cyclables d'ici 2026"
                    ]
                };
            } else if (city.includes("Marseille") || city.includes("Bouches-du-Rhône")) {
                localAction = {
                    icon: "fa-solid fa-anchor",
                    title: "Transition à Marseille",
                    items: [
                        "Électrification des quais de navires au port",
                        "Déploiement de la ZFE et transports décarbonés",
                        "Plan Écoles : Rénovation thermique globale"
                    ]
                };
            } else if (city.includes("Lyon") || city.includes("Rhône")) {
                localAction = {
                    icon: "fa-solid fa-bicycle",
                    title: "Transition à Lyon",
                    items: [
                        "Voies Lyonnaises : Réseau cyclable structurant",
                        "ZFE renforcée et logistique urbaine décarbonée",
                        "Plan Canopée : Plantation de 300 000 arbres"
                    ]
                };
            } else if (city.includes("Bordeaux") || city.includes("Gironde")) {
                localAction = {
                    icon: "fa-solid fa-droplet",
                    title: "Transition en Gironde",
                    items: [
                        "Réseau express vélo métropolitain",
                        "Déploiement ZFE et covoiturage obligatoire",
                        "Reboisement et aménagement des forêts"
                    ]
                };
            } else if (city.includes("Nantes") || city.includes("Loire-Atlantique")) {
                localAction = {
                    icon: "fa-solid fa-bus",
                    title: "Transition à Nantes",
                    items: [
                        "Expansion du réseau de chronobus et tramways",
                        "Pacte Vert : réduction de l'étalement urbain",
                        "Trame verte et bleue pour la biodiversité de la Loire"
                    ]
                };
            } else if (city.includes("Lille") || city.includes("Nord")) {
                localAction = {
                    icon: "fa-solid fa-building-shield",
                    title: "Transition dans le Nord",
                    items: [
                        "Rénovation thermique massive des logements du Nord",
                        "Décarbonation industrielle du port de Dunkerque",
                        "Réseau express grand Lille de transports en commun"
                    ]
                };
            }
            
            advices.push(localAction);

            // Rules based on weather data
            if (data.TM > 15 || (data.DAYS_CANICULE && data.DAYS_CANICULE > 2)) {
                advices.push({
                    icon: "fa-solid fa-temperature-arrow-up",
                    title: "Risque Caniculaire (Public)",
                    items: [
                        "Aménager des îlots de fraîcheur urbains (brumisateurs, parcs)",
                        "Végétaliser les cours d'école (cours Oasis)",
                        "Activer le registre nominatif d'alerte canicule"
                    ]
                });
            }

            if (data.DRY_SPELL_MAX > 15 || (data.RR_TOTAL && data.RR_TOTAL < 600)) {
                advices.push({
                    icon: "fa-solid fa-droplet-slash",
                    title: "Stress Hydrique (Public)",
                    items: [
                        "Moderniser les réseaux d'eau pour limiter les fuites",
                        "Mettre en place une tarification progressive de l'eau",
                        "Imposer le recyclage des eaux grises dans les constructions"
                    ]
                });
            }

            advices.push({
                icon: "fa-solid fa-car-side",
                title: "Empreinte Carbone (Public)",
                items: [
                    "Rénover énergétiquement les bâtiments publics et HLM",
                    "Développer le réseau de transports en commun propres",
                    "Soutenir la transition agroécologique locale"
                ]
            });

            if ((data.DAYS_CANICULE && data.DAYS_CANICULE > 5) || data.DRY_SPELL_MAX > 20) {
                advices.push({
                    icon: "fa-solid fa-fire",
                    title: "Prévention Incendies (Public)",
                    items: [
                        "Obliger et contrôler le débroussaillement réglementaire",
                        "Créer et entretenir des pistes d'accès pour les pompiers",
                        "Mettre en place des capteurs de détection précoce"
                    ]
                });
            }
        } else {
            // CITOYEN / PARTICULIER ACTIONS
            localAction = {
                icon: "fa-solid fa-map-pin",
                title: "Actions Citoyennes",
                items: [
                    "Participer aux chantiers locaux de végétalisation",
                    "Acheter local et de saison pour réduire l'impact transport",
                    "Sensibiliser ses proches aux éco-gestes quotidiens"
                ]
            };

            if (isFrance(city)) {
                localAction = {
                    icon: "fa-solid fa-flag",
                    title: "Sobriété Nationale",
                    items: [
                        "Privilégier le covoiturage et les transports en commun",
                        "Réduire le chauffage individuel à 19°C maximum",
                        "Réduire la consommation de viande et produits importés"
                    ]
                };
            } else if (city.includes("Paris")) {
                localAction = {
                    icon: "fa-solid fa-tree",
                    title: "Transition à Paris (Citoyen)",
                    items: [
                        "Utiliser le réseau Vélib' et les pistes cyclables",
                        "Participer aux projets de végétalisation citoyenne",
                        "Respecter les vignettes Crit'Air en cas de pic de pollution"
                    ]
                };
            } else if (city.includes("Marseille") || city.includes("Bouches-du-Rhône")) {
                localAction = {
                    icon: "fa-solid fa-anchor",
                    title: "Transition à Marseille (Citoyen)",
                    items: [
                        "Préférer les navettes maritimes et vélos en libre-service",
                        "Économiser l'eau lors des sécheresses estivales",
                        "Participer aux nettoyages citoyens des plages"
                    ]
                };
            } else if (city.includes("Lyon") || city.includes("Rhône")) {
                localAction = {
                    icon: "fa-solid fa-bicycle",
                    title: "Transition à Lyon (Citoyen)",
                    items: [
                        "Emprunter le réseau cyclable des Voies Lyonnaises",
                        "Isoler son logement avec l'aide des aides de la Métropole",
                        "Végétaliser sa rue via le permis de végétaliser lyonnais"
                    ]
                };
            } else if (city.includes("Bordeaux") || city.includes("Gironde")) {
                localAction = {
                    icon: "fa-solid fa-droplet",
                    title: "Transition en Gironde (Citoyen)",
                    items: [
                        "Utiliser le Réseau Express Vélo de la métropole",
                        "Privilégier le covoiturage sur les voies réservées",
                        "Respecter le calendrier d'interdiction des feux de forêt"
                    ]
                };
            } else if (city.includes("Nantes") || city.includes("Loire-Atlantique")) {
                localAction = {
                    icon: "fa-solid fa-bus",
                    title: "Transition à Nantes (Citoyen)",
                    items: [
                        "Utiliser les parkings relais et transports Naolib",
                        "Installer des composteurs individuels ou de quartier",
                        "Soutenir les AMAP et producteurs locaux de la Loire"
                    ]
                };
            } else if (city.includes("Lille") || city.includes("Nord")) {
                localAction = {
                    icon: "fa-solid fa-building-shield",
                    title: "Transition dans le Nord (Citoyen)",
                    items: [
                        "Réaliser un diagnostic énergétique de sa maison",
                        "Préférer le train TER pour les trajets régionaux",
                        "Récupérer et composter ses biodéchets à domicile"
                    ]
                };
            }

            advices.push(localAction);

            // Rules based on weather data
            if (data.TM > 15 || (data.DAYS_CANICULE && data.DAYS_CANICULE > 2)) {
                advices.push({
                    icon: "fa-solid fa-temperature-arrow-up",
                    title: "Risque Caniculaire (Personnel)",
                    items: [
                        "S'hydrater régulièrement et fermer les volets en journée",
                        "Prendre des nouvelles des voisins isolés ou âgés",
                        "Fréquenter les parcs arborés et zones ombragées"
                    ]
                });
            }

            if (data.DRY_SPELL_MAX > 15 || (data.RR_TOTAL && data.RR_TOTAL < 600)) {
                advices.push({
                    icon: "fa-solid fa-droplet-slash",
                    title: "Stress Hydrique (Personnel)",
                    items: [
                        "Installer un récupérateur d'eau pour arroser ses plantes",
                        "Couper l'eau pendant le brossage et préférer les douches courtes",
                        "Équiper ses robinets de mousseurs d'eau économiseurs"
                    ]
                });
            }

            advices.push({
                icon: "fa-solid fa-car-side",
                title: "Empreinte Carbone (Personnel)",
                items: [
                    "Privilégier le vélo ou la marche pour les trajets courts",
                    "Acheter des appareils de classe A et réparer au lieu de jeter",
                    "Privilégier le train à l'avion pour les vacances"
                ]
            });

            if ((data.DAYS_CANICULE && data.DAYS_CANICULE > 5) || data.DRY_SPELL_MAX > 20) {
                advices.push({
                    icon: "fa-solid fa-fire",
                    title: "Prévention Incendies (Personnel)",
                    items: [
                        "Ne jamais jeter de mégot en extérieur ni faire de barbecue près des bois",
                        "Débroussailler le terrain autour de son habitation principale",
                        "Signaler immédiatement tout départ de fumée au 18 ou 112"
                    ]
                });
            }
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

        let cityPerf = [];
        if (isFrance(city)) {
            const modelPerf = performanceData.filter(p => p.MODEL === currentModel);
            const indicators = [...new Set(modelPerf.map(p => p.INDICATEUR))];
            
            indicators.forEach(ind => {
                const indData = modelPerf.filter(p => p.INDICATEUR === ind);
                if (indData.length > 0) {
                    const count = indData.length;
                    const avgRMSE = indData.reduce((sum, p) => sum + (p.RMSE || 0), 0) / count;
                    const avgMAE = indData.reduce((sum, p) => sum + (p.MAE || 0), 0) / count;
                    const avgMAPE = indData.reduce((sum, p) => sum + (p.MAPE_PCT || 0), 0) / count;
                    cityPerf.push({
                        INDICATEUR: ind,
                        MODEL: currentModel,
                        RMSE: avgRMSE,
                        MAE: avgMAE,
                        MAPE_PCT: avgMAPE
                    });
                }
            });
        } else {
            cityPerf = performanceData.filter(p => p.VILLE === city && p.MODEL === currentModel);
        }
        
        cityPerf.forEach(p => {
            const precision = Math.max(0, 100 - p.MAPE_PCT).toFixed(1);
            const badgeClass = precision > 90 ? 'metric-good' : 'metric-fair';
            
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td><b>${p.INDICATEUR}</b></td>
                <td>${p.MODEL}</td>
                <td>${(p.RMSE || 0).toFixed(2)}</td>
                <td>${(p.MAE || 0).toFixed(2)}</td>
                <td>${(p.MAPE_PCT || 0).toFixed(1)}%</td>
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
                    backgroundColor: ['#3a8c6e', '#22c55e', '#ef4444', '#38bdf8', '#f59e0b', '#9ca3af'],
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
                            color: '#161616',
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
        
        if (type === 'bot') {
            // Scroll to the top of the new bot response smoothly
            setTimeout(() => {
                chatMsgs.scrollTo({
                    top: div.offsetTop - 10,
                    behavior: 'smooth'
                });
            }, 50);
        } else {
            chatMsgs.scrollTop = chatMsgs.scrollHeight;
        }
        
        return id;
    }

    // --- Tooltip & Profile Selector Helpers ---

    const TOOLTIP_TEXTS = {
        citoyen: {
            "temp-slider": "Faites glisser les curseurs pour définir la période d'années que vous souhaitez analyser (ex: de 1990 à 2025).",
            "city-select": "Sélectionnez votre département pour charger les observations historiques et les projections météo près de chez vous.",
            "card-tm": "Température moyenne annuelle sur le territoire sélectionné (moyenne filtrée des relevés de la station).",
            "card-canicule": "Nombre moyen de journées extrêmement chaudes où le thermomètre dépasse 35°C dans l'année.",
            "card-tropical": "Nombre de nuits où la température ne descend pas sous 20°C, rendant le sommeil difficile.",
            "card-gel": "Nombre de jours dans l'année où la température minimale est inférieure à 0°C (gel hivernal).",
            "card-anom": "Écart de température par rapport aux normales historiques. Si c'est positif (rouge), il fait anormalement chaud.",
            "card-rr": "Cumul total de pluie et neige mesuré sur l'année en millimètres (1 mm = 1 litre d'eau par m²).",
            "card-dryspell": "Le plus grand nombre de jours consécutifs avec moins de 1 mm de pluie, signalant le risque de sécheresse.",
            "card-hotseason": "Nombre de jours agréables ou chauds où la température maximale dépasse 25°C.",
            "map-layers": "Boutons pour basculer la carte entre les températures moyennes (Météo) ou les émissions de gaz à effet de serre du département (Émissions).",
            "chart-evolution": "Graphique combiné montrant la tendance des températures moyennes (ligne bleue) et des pluies (barres vertes) au fil des ans.",
            "chart-anomaly": "Visualisation de l'écart thermique annuel : les barres rouges indiquent les années plus chaudes que la normale de référence.",
            "chart-extreme": "Évolution comparée du nombre de jours de gel (froid, en bleu) et de jours de saison chaude (chaleur, en orange).",
            "chart-sector": "Répartition par secteurs d'activité de l'empreinte carbone annuelle de votre département (données IGT Citepa).",
            "chart-projection": "Simulation de la hausse de température jusqu'en 2100 selon 3 scénarios du GIEC (écologique en vert, modéré en bleu, polluant en rouge).",
            "performance-table": "Scores d'évaluation statistique montrant l'écart moyen entre les calculs de nos modèles d'IA et les mesures historiques réelles.",
            "methodology-section": "Explications sur la provenance de nos données (Open-Meteo, Citepa, GIEC) et les critères géographiques de notre étude.",
            "actions-section": "Suggestions concrètes d'initiatives à adopter à l'échelle individuelle ou locale pour lutter contre le dérèglement.",
            "climabot-chat": "Posez vos questions à ClimaBot pour obtenir des chiffres météo locaux précis ou des conseils d'actions écologiques.",
            "chart-sea-temp": "Suivi annuel de la température de surface de la mer (SST) dans les différentes façades maritimes françaises depuis 1973."
        },
        collectivite: {
            "temp-slider": "Bornage de la fenêtre temporelle d'analyse historique et projective pour le diagnostic climatique local et les rapports PCAET.",
            "city-select": "Sélection de la maille territoriale départementale de référence pour l'édition de bilans climatiques réglementaires.",
            "card-tm": "Température moyenne annuelle agrégée. Indicateur d'exposition thermique de référence pour l'analyse des vulnérabilités locales.",
            "card-canicule": "Indicateur de vagues de chaleur sévères (Tmax > 35°C). Indicateur réglementaire pour le Plan de Gestion des Vagues de Chaleur.",
            "card-tropical": "Indicateur d'îlots de chaleur urbains (ICU) nocturnes (Tmin >= 20°C). Indicateur clé d'impact sur la santé publique.",
            "card-gel": "Nombre de jours de gel (Tmin < 0°C). Indicateur d'aléa pour la viabilité hivernale, le secteur agricole et la demande de chauffage.",
            "card-anom": "Anomalie thermique locale calculée par rapport à la baseline historique locale. Indicateur d'intensité du changement climatique.",
            "card-rr": "Cumul annuel moyen des précipitations (en mm). Indicateur hydrologique pour la gestion de la ressource en eau et le risque de crues.",
            "card-dryspell": "Durée maximale consécutive sans précipitation (>1 mm/jour). Indicateur d'aléa sécheresse pour le stress agricole et le risque incendie.",
            "card-hotseason": "Nombre de jours avec température maximale supérieure à 25°C. Mesure de l'extension de la saison estivale et du confort d'été.",
            "map-layers": "Basculez entre la carte d'exposition aux aléas thermiques (Météo) et la carte d'inventaire d'émissions de GES de l'Indicateur IGT (Émissions).",
            "chart-evolution": "Chrono-série historique montrant la corrélation entre les tendances thermiques observées et les fluctuations de la pluviométrie locale.",
            "chart-anomaly": "Suivi des anomalies de température annuelles par rapport à la période de référence. Permet d'identifier la récurrence des extrêmes.",
            "chart-extreme": "Analyse croisée de la variabilité des extrêmes thermiques : diminution des jours de gel contre hausse des jours chauds (>25°C).",
            "chart-sector": "Bilan d'émissions de gaz à effet de serre ventilé par grands secteurs économiques (format PCAET compatible avec la méthodologie Citepa).",
            "chart-projection": "Trajectoires d'évolution thermique calées sur les profils de concentration du GIEC (RCP 2.6 optimiste, RCP 4.5 médian, RCP 8.5 pessimiste).",
            "performance-table": "Métriques de validation statistique (RMSE, MAE, MAPE) calculées en phase de backtesting historique sur les années de test 2020-2025.",
            "methodology-section": "Méthodologie de normalisation et d'agrégation spatio-temporelle des données brutes issues de Météo-France, Citepa et du GIEC.",
            "actions-section": "Mesures réglementaires d'atténuation et d'adaptation préconisées pour les collectivités (Transports décarbonés, Rénovation tertiaire, PCAET).",
            "climabot-chat": "Assistant conversationnel d'aide à la décision locale. Demandez des synthèses climatiques régionales ou des fiches d'action PCAET.",
            "chart-sea-temp": "Séries temporelles de la température moyenne de surface de la mer (SST) par façade maritime pour l'analyse d'impact sur la biodiversité marine."
        }
    };

    const globalTooltip = document.getElementById('global-tooltip');
    
    function showTooltip(element, text) {
        if (!globalTooltip || !text) return;
        globalTooltip.innerHTML = text;
        globalTooltip.classList.remove('hidden');
        
        // Position relative to target element
        const rect = element.getBoundingClientRect();
        const tooltipWidth = globalTooltip.offsetWidth || 280;
        const tooltipHeight = globalTooltip.offsetHeight || 80;
        
        // Align horizontally centered, and vertically above target
        let x = rect.left + window.scrollX + rect.width / 2;
        let y = rect.top + window.scrollY - 10;
        
        // Safety bounds checks to prevent clipping off screen
        const padding = 12;
        
        // Prevent going off left edge
        if (x - tooltipWidth / 2 < window.scrollX + padding) {
            x = window.scrollX + padding + tooltipWidth / 2;
        }
        
        // Prevent going off right edge
        const rightMax = window.innerWidth + window.scrollX - padding;
        if (x + tooltipWidth / 2 > rightMax) {
            x = rightMax - tooltipWidth / 2;
        }
        
        // Prevent going off top edge (flip to show below elements if clipped at the top)
        if (rect.top - tooltipHeight - padding < 0) {
            // Put below the trigger element instead of above
            y = rect.bottom + window.scrollY + 10 + tooltipHeight;
        }
        
        globalTooltip.style.left = `${x}px`;
        globalTooltip.style.top = `${y}px`;
    }
    
    function hideTooltip() {
        if (globalTooltip) {
            globalTooltip.classList.add('hidden');
        }
    }
    
    function setupTooltips() {
        document.querySelectorAll('.info-tooltip').forEach(el => {
            const tooltipId = el.getAttribute('data-tooltip-id');
            
            // Clean existing listeners to prevent leaks
            el.onmouseenter = null;
            el.onmouseleave = null;
            el.onclick = null;
            
            el.addEventListener('mouseenter', (e) => {
                const text = TOOLTIP_TEXTS[currentProfile][tooltipId] || "";
                showTooltip(el, text);
            });
            
            el.addEventListener('mouseleave', () => {
                hideTooltip();
            });
            
            // Toggle on click for mobile devices
            el.addEventListener('click', (e) => {
                e.stopPropagation();
                const text = TOOLTIP_TEXTS[currentProfile][tooltipId] || "";
                if (globalTooltip.classList.contains('hidden')) {
                    showTooltip(el, text);
                } else {
                    hideTooltip();
                }
            });
        });
    }
    
    // Hide tooltip on clicking anywhere outside
    document.addEventListener('click', (e) => {
        if (globalTooltip && !globalTooltip.classList.contains('hidden')) {
            if (!e.target.closest('.info-tooltip')) {
                hideTooltip();
            }
        }
    });

    function setProfile(profile) {
        const btnCitoyen = document.getElementById('profileCitoyen');
        const btnDecideur = document.getElementById('profileDecideur');
        if (!btnCitoyen || !btnDecideur) return;
        
        if (profile === 'citoyen') {
            if (currentProfile === 'citoyen') return;
            currentProfile = 'citoyen';
            btnCitoyen.classList.add('active');
            btnDecideur.classList.remove('active');
            document.body.classList.remove('decideur-mode');
        } else if (profile === 'collectivite') {
            if (currentProfile === 'collectivite') return;
            currentProfile = 'collectivite';
            btnDecideur.classList.add('active');
            btnCitoyen.classList.remove('active');
            document.body.classList.add('decideur-mode');
        }
        
        // Update tabs inside modal if it exists to match dashboard
        const tabCitoyen = document.getElementById('modalTabCitoyen');
        const tabDecideur = document.getElementById('modalTabDecideur');
        const contentCitoyen = document.getElementById('modalContentCitoyen');
        const contentDecideur = document.getElementById('modalContentDecideur');
        
        if (tabCitoyen && tabDecideur && contentCitoyen && contentDecideur) {
            if (profile === 'citoyen') {
                tabCitoyen.classList.add('active');
                tabDecideur.classList.remove('active');
                contentCitoyen.classList.add('active');
                contentDecideur.classList.remove('active');
            } else {
                tabDecideur.classList.add('active');
                tabCitoyen.classList.remove('active');
                contentDecideur.classList.add('active');
                contentCitoyen.classList.remove('active');
            }
        }
        
        updateProfileContent();
    }

    function initProfileSwitcher() {
        const btnCitoyen = document.getElementById('profileCitoyen');
        const btnDecideur = document.getElementById('profileDecideur');
        
        if (!btnCitoyen || !btnDecideur) return;
        
        btnCitoyen.addEventListener('click', () => setProfile('citoyen'));
        btnDecideur.addEventListener('click', () => setProfile('collectivite'));
    }
    
    function initOnboardingModal() {
        const modal = document.getElementById('infoModal');
        const openBtn = document.getElementById('openInfoModal');
        const closeBtn = document.getElementById('closeInfoModal');
        const startBtn = document.getElementById('modalStartBtn');
        const tabCitoyen = document.getElementById('modalTabCitoyen');
        const tabDecideur = document.getElementById('modalTabDecideur');
        const neverShowCheck = document.getElementById('modalNeverShowAgain');
        
        if (!modal) return;
        
        // Open Modal
        function showModal() {
            modal.classList.remove('hidden');
            // Sync modal active tab with current profile
            const activeProfile = currentProfile === 'citoyen' ? 'citoyen' : 'collectivite';
            
            const tabCitoyen = document.getElementById('modalTabCitoyen');
            const tabDecideur = document.getElementById('modalTabDecideur');
            const contentCitoyen = document.getElementById('modalContentCitoyen');
            const contentDecideur = document.getElementById('modalContentDecideur');
            
            if (tabCitoyen && tabDecideur && contentCitoyen && contentDecideur) {
                if (activeProfile === 'citoyen') {
                    tabCitoyen.classList.add('active');
                    tabDecideur.classList.remove('active');
                    contentCitoyen.classList.add('active');
                    contentDecideur.classList.remove('active');
                } else {
                    tabDecideur.classList.add('active');
                    tabCitoyen.classList.remove('active');
                    contentDecideur.classList.add('active');
                    contentCitoyen.classList.remove('active');
                }
            }
            
            // Check neverShow checkbox state from localStorage
            if (localStorage.getItem('climashere_onboarded') === 'true') {
                neverShowCheck.checked = true;
            } else {
                neverShowCheck.checked = false;
            }
        }
        
        // Close Modal
        function hideModal() {
            modal.classList.add('hidden');
            // Save state if checkbox is checked
            if (neverShowCheck && neverShowCheck.checked) {
                localStorage.setItem('climashere_onboarded', 'true');
            } else {
                localStorage.removeItem('climashere_onboarded');
            }
        }
        
        // Event Listeners for trigger buttons
        if (openBtn) openBtn.addEventListener('click', showModal);
        if (closeBtn) closeBtn.addEventListener('click', hideModal);
        if (startBtn) startBtn.addEventListener('click', hideModal);
        
        // Close on clicking outside the modal card
        modal.addEventListener('click', (e) => {
            if (e.target === modal) {
                hideModal();
            }
        });
        
        // Switch to Citoyen tab
        if (tabCitoyen) {
            tabCitoyen.addEventListener('click', () => setProfile('citoyen'));
        }
        
        // Switch to Decideur tab
        if (tabDecideur) {
            tabDecideur.addEventListener('click', () => setProfile('collectivite'));
        }
        
        // First load auto-display check
        const onboarded = localStorage.getItem('climashere_onboarded');
        if (onboarded !== 'true') {
            // Auto open modal on first visit
            setTimeout(showModal, 600);
        }
    }
    
    function updateProfileContent() {
        // 1. Update text labels of headers
        const labels = {
            citoyen: {
                "lblTM": "Température moyenne",
                "lblCanicule": "Canicule (>35°C)",
                "lblTropical": "Nuits Tropicales (>20°C)",
                "lblGel": "Jours de Gel (<0°C)",
                "lblAnom": "Réchauffement local",
                "lblRR": "Pluie cumulée",
                "lblDrySpell": "Sécheresse prolongée",
                "lblHotSeason": "Jours Chauds (>25°C)",
                "lblAgir": "Mes écogestes quotidiens"
            },
            collectivite: {
                "lblTM": "Temp. Moyenne locale",
                "lblCanicule": "Seuil de Canicule (Tmax > 35°C)",
                "lblTropical": "Nuits Tropicales (ICU - Tmin > 20°C)",
                "lblGel": "Jours de Gel (Tmin < 0°C)",
                "lblAnom": "Anomalie Thermique Baseline",
                "lblRR": "Pluviométrie annuelle",
                "lblDrySpell": "Séquence Sèche (Hydrométrie)",
                "lblHotSeason": "Saison Chaude (Tmax > 25°C)",
                "lblAgir": "Actions Publiques PCAET & PNACC"
            }
        };
        
        const currentLabels = labels[currentProfile];
        Object.keys(currentLabels).forEach(id => {
            const el = document.getElementById(id);
            if (el) {
                el.innerText = currentLabels[id];
            }
        });
        
        // 2. Refresh dynamic parts
        if (currentCity) {
            updateDashboard(currentCity);
        }
        
        // 3. Resetup tooltip content listeners (to bind currentProfile values)
        setupTooltips();
    }
});
