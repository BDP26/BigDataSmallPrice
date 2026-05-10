import re

with open("src/frontend/static/admin_dash.html", "r") as f:
    content = f.read()

# 1. Remove mock nav links
nav_pattern = r'<nav class="hidden md:flex items-center gap-6">.*?</nav>'
nav_replacement = '''<nav class="hidden md:flex items-center gap-6">
                        <!-- Links removed as part of cleanup -->
                    </nav>'''
content = re.sub(nav_pattern, nav_replacement, content, flags=re.DOTALL)

# 2. Remove header buttons
btn_pattern = r'<div class="flex gap-3">\s*<button.*?Export CSV\s*</button>\s*<button.*?Retrain Model\s*</button>\s*</div>'
btn_replacement = '''<div class="flex gap-3">
                    <!-- Buttons removed -->
                </div>'''
content = re.sub(btn_pattern, btn_replacement, content, flags=re.DOTALL)

# 3. Replace the grid content (Feature Importance & Pipeline Drift)
grid_pattern = r'<!-- Main Dashboard Content -->\s*<div class="grid grid-cols-1 lg:grid-cols-12 gap-6">\s*<!-- Feature Importance -->.*?<!-- DB Source Status -->'

grid_replacement = '''<!-- Real-Time Low Hanging Fruits -->
            <div class="grid grid-cols-1 lg:grid-cols-12 gap-6">
                <!-- Live Forecast Card -->
                <div class="lg:col-span-5 bg-white dark:bg-slate-900 p-6 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm flex flex-col">
                    <div class="flex items-center justify-between mb-4">
                        <h3 class="text-lg font-bold text-slate-900 dark:text-white flex items-center gap-2">
                            <span class="material-symbols-outlined text-primary">online_prediction</span>
                            Live Forecast
                        </h3>
                        <span class="text-xs text-slate-400 font-mono" id="live-forecast-time">—</span>
                    </div>
                    
                    <div class="bg-slate-50 dark:bg-slate-800/50 p-4 rounded-lg flex-1 flex flex-col justify-center">
                        <div class="text-center mb-4">
                            <div class="text-sm font-bold text-slate-500 uppercase tracking-wider mb-1">Vorhersage Gesamttarif</div>
                            <div class="text-4xl font-black text-slate-900 dark:text-white flex items-baseline justify-center gap-1">
                                <span id="live-forecast-price">—</span>
                                <span class="text-lg text-slate-400 font-normal ml-1">Rp/kWh</span>
                            </div>
                        </div>
                        <div class="flex justify-between items-center text-xs p-3 bg-white dark:bg-slate-900 rounded border border-slate-200 dark:border-slate-700">
                            <span class="text-slate-500">Day-Ahead EPEX</span>
                            <span class="font-mono font-bold" id="live-forecast-epex">— EUR/MWh</span>
                        </div>
                        <div class="mt-2 flex justify-between items-center text-xs p-3 bg-white dark:bg-slate-900 rounded border border-slate-200 dark:border-slate-700">
                            <span class="text-slate-500">Price Level</span>
                            <span class="font-bold px-2 py-0.5 rounded" id="live-forecast-level">—</span>
                        </div>
                    </div>
                </div>

                <!-- Price History Chart -->
                <div class="lg:col-span-7 bg-white dark:bg-slate-900 p-6 rounded-xl border border-slate-200 dark:border-slate-800 shadow-sm">
                    <div class="flex items-center justify-between mb-4">
                        <h3 class="text-lg font-bold text-slate-900 dark:text-white flex items-center gap-2">
                            <span class="material-symbols-outlined text-primary">history</span>
                            Price History (Last 24h)
                        </h3>
                        <div class="flex items-center gap-2">
                            <span class="text-[10px] font-black px-2 py-0.5 rounded bg-green-100 dark:bg-green-900/30 text-green-600">LIVE</span>
                        </div>
                    </div>
                    <div id="price-history-loading" class="h-64 flex flex-col items-center justify-center text-slate-400">
                        <span class="material-symbols-outlined text-2xl mb-2" style="animation:spin 1s linear infinite">refresh</span>
                        <span class="text-sm">Lade Historie...</span>
                    </div>
                    <div id="price-history-chart" style="height: 250px" class="hidden"></div>
                </div>
            </div>
            <!-- DB Source Status -->'''

content = re.sub(grid_pattern, grid_replacement, content, flags=re.DOTALL)

# 4. Insert the new JS functions
js_pattern = r'// ── Boot ─────────────────────────────────────────────────────────────'
js_replacement = '''// ── Real-Time Features ────────────────────────────────────────────────
        async function loadLiveForecast() {
            try {
                const res = await fetch('/api/forecast');
                if (!res.ok) throw new Error('Failed to fetch forecast');
                const data = await res.json();
                
                if (data.error) throw new Error(data.error);

                const timeStr = new Date(data.time).toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'});
                document.getElementById('live-forecast-time').textContent = "As of " + timeStr;
                document.getElementById('live-forecast-price').textContent = data.gesamttarif_rp_kwh != null ? data.gesamttarif_rp_kwh.toFixed(2) : '—';
                document.getElementById('live-forecast-epex').textContent = (data.predicted_price_eur_mwh != null ? data.predicted_price_eur_mwh.toFixed(2) : '—') + ' EUR/MWh';
                
                const levelBadge = document.getElementById('live-forecast-level');
                const lvl = (data.price_level || 'UNKNOWN');
                levelBadge.textContent = lvl.toUpperCase();
                
                if (lvl === 'low') {
                    levelBadge.className = 'font-bold px-2 py-0.5 rounded bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400';
                } else if (lvl === 'high') {
                    levelBadge.className = 'font-bold px-2 py-0.5 rounded bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400';
                } else {
                    levelBadge.className = 'font-bold px-2 py-0.5 rounded bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400';
                }
            } catch (e) {
                document.getElementById('live-forecast-time').textContent = 'Error';
                console.error(e);
            }
        }

        async function loadPriceHistory() {
            const chartDiv = document.getElementById('price-history-chart');
            const loadingDiv = document.getElementById('price-history-loading');
            try {
                const res = await fetch('/api/price-history?hours=24');
                if (!res.ok) throw new Error('Failed to fetch price history');
                const data = await res.json();
                
                if (data.error) throw new Error(data.error);

                loadingDiv.classList.add('hidden');
                chartDiv.classList.remove('hidden');

                const isDark = document.documentElement.classList.contains('dark');
                const gridColor = isDark ? '#1e293b' : '#e2e8f0';
                const fontColor = isDark ? '#94a3b8' : '#64748b';
                
                const trace = {
                    x: data.times,
                    y: data.prices,
                    type: 'scatter',
                    mode: 'lines',
                    fill: 'tozeroy',
                    fillcolor: isDark ? 'rgba(19, 127, 236, 0.1)' : 'rgba(19, 127, 236, 0.1)',
                    line: { color: '#137fec', width: 2 },
                    name: 'EPEX Price'
                };

                const layout = {
                    paper_bgcolor: 'transparent',
                    plot_bgcolor:  'transparent',
                    font:   { color: fontColor, size: 11, family: 'Inter, sans-serif' },
                    xaxis:  { gridcolor: gridColor, linecolor: gridColor, zerolinecolor: gridColor, fixedrange: true },
                    yaxis:  { gridcolor: gridColor, linecolor: gridColor, zerolinecolor: gridColor, title: 'EUR/MWh', fixedrange: true },
                    margin: { t: 10, r: 10, l: 40, b: 30 },
                    hovermode: 'x unified',
                };

                Plotly.react(chartDiv, [trace], layout, { responsive: true, displayModeBar: false });
            } catch (e) {
                loadingDiv.innerHTML = `<span class="text-red-500 text-xs">${e.message}</span>`;
                console.error(e);
            }
        }

        // ── Boot ─────────────────────────────────────────────────────────────'''

content = re.sub(js_pattern, js_replacement, content, flags=re.DOTALL)

# 5. Insert boot calls
boot_pattern = r'loadModelsStatus\(\);\n\s*checkBackfillState\(\);'
boot_replacement = '''loadModelsStatus();
            loadLiveForecast();
            loadPriceHistory();
            checkBackfillState();'''
content = re.sub(boot_pattern, boot_replacement, content, flags=re.DOTALL)

interval_pattern = r'setInterval\(loadModelsStatus, 60000\);'
interval_replacement = '''setInterval(loadModelsStatus, 60000);
            setInterval(loadLiveForecast, 60000);
            setInterval(loadPriceHistory, 300000);'''
content = re.sub(interval_pattern, interval_replacement, content, flags=re.DOTALL)

with open("src/frontend/static/admin_dash.html", "w") as f:
    f.write(content)

