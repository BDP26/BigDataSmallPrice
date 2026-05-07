import re

with open("src/frontend/static/user_dash.html", "r") as f:
    content = f.read()

# 1. Replace the HTML for the cheapest windows and device recommendations
html_pattern = r'<!-- Row 2: Cheapest windows -->.*?<!-- ── Mobile nav ─────────────────────────────────────────────────────────────── -->'

html_replacement = '''<!-- Row 2: Device Recommendations -->
    <div class="mb-2 flex items-center gap-2 px-2">
        <span class="material-symbols-outlined text-primary text-xl">smart_toy</span>
        <h2 class="font-bold text-base">Ideale Startzeiten für deine Geräte</h2>
    </div>
    <div class="grid grid-cols-2 sm:grid-cols-4 gap-4 mb-6">
        <div id="dev-card-0" class="bg-white dark:bg-card-dark rounded-2xl border border-slate-200 dark:border-border-dark shadow-sm p-4 flex items-center gap-3 opacity-60">
            <div class="p-2.5 bg-slate-100 dark:bg-slate-800 text-slate-400 rounded-xl">
                <span class="material-symbols-outlined">dishwasher_gen</span>
            </div>
            <div class="min-w-0">
                <p class="text-[10px] text-slate-400 font-medium truncate">Waschmaschine / Spüler</p>
                <p id="dev0-time" class="text-sm font-bold text-slate-500 truncate">Wird geladen…</p>
            </div>
        </div>
        <div id="dev-card-1" class="bg-white dark:bg-card-dark rounded-2xl border border-slate-200 dark:border-border-dark shadow-sm p-4 flex items-center gap-3 opacity-60">
            <div class="p-2.5 bg-slate-100 dark:bg-slate-800 text-slate-400 rounded-xl">
                <span class="material-symbols-outlined">ev_station</span>
            </div>
            <div class="min-w-0">
                <p class="text-[10px] text-slate-400 font-medium truncate">EV Ladestation</p>
                <p id="dev1-time" class="text-sm font-bold text-slate-500 truncate">Wird geladen…</p>
            </div>
        </div>
        <div id="dev-card-2" class="bg-white dark:bg-card-dark rounded-2xl border border-slate-200 dark:border-border-dark shadow-sm p-4 flex items-center gap-3 opacity-60">
            <div class="p-2.5 bg-slate-100 dark:bg-slate-800 text-slate-400 rounded-xl">
                <span class="material-symbols-outlined">heat_pump</span>
            </div>
            <div class="min-w-0">
                <p class="text-[10px] text-slate-400 font-medium truncate">Wärmepumpe</p>
                <p id="dev2-time" class="text-sm font-bold text-slate-500 truncate">Wird geladen…</p>
            </div>
        </div>
        <div id="dev-card-3" class="bg-white dark:bg-card-dark rounded-2xl border border-slate-200 dark:border-border-dark shadow-sm p-4 flex items-center gap-3 opacity-60">
            <div class="p-2.5 bg-slate-100 dark:bg-slate-800 text-slate-400 rounded-xl">
                <span class="material-symbols-outlined">water_heater</span>
            </div>
            <div class="min-w-0">
                <p class="text-[10px] text-slate-400 font-medium truncate">Boiler / Warmwasser</p>
                <p id="dev3-time" class="text-sm font-bold text-slate-500 truncate">Wird geladen…</p>
            </div>
        </div>
    </div>

    <!-- Row 3: Stromnetz-Wissen (Schon gewusst?) -->
    <div class="bg-gradient-to-r from-blue-50 to-indigo-50 dark:from-slate-800 dark:to-slate-800/80 rounded-2xl border border-blue-100 dark:border-slate-700 shadow-sm p-5 flex gap-4 items-start">
        <div class="flex items-center justify-center w-10 h-10 bg-white dark:bg-slate-700 rounded-full shadow-sm shrink-0 text-amber-500">
            <span class="material-symbols-outlined" style="font-variation-settings: 'FILL' 1;">lightbulb</span>
        </div>
        <div>
            <h3 class="font-bold text-sm text-slate-800 dark:text-slate-200 mb-1">Schon gewusst?</h3>
            <p id="grid-tip-text" class="text-xs text-slate-600 dark:text-slate-400 leading-relaxed transition-opacity duration-500">
                Stromnetz-Wissen wird geladen...
            </p>
        </div>
    </div>

</main>

<!-- ── Mobile nav ─────────────────────────────────────────────────────────────── -->'''

content = re.sub(html_pattern, html_replacement, content, flags=re.DOTALL)

# 2. Replace JS for rendering cheapest windows (since we removed the grid)
js_pattern = r'// ── Cheapest windows grid ──────────────────────────────────────────────────────.*?function renderDeviceCards\(windows\) \{'

js_replacement = '''// ── Stromnetz-Wissen Tipps ──────────────────────────────────────────────────
const GRID_TIPS = [
    "Mittags ist der Strom oft besonders günstig, weil Photovoltaik-Anlagen viel Sonnenenergie ins Netz einspeisen.",
    "Wenn die Tarif-Ampel rot zeigt, müssen oft teure (und CO2-intensive) Gaskraftwerke im Ausland einspringen.",
    "Nachts weht oft mehr Wind, weshalb Windkraftwerke in Europa den Strompreis senken können.",
    "Ein Großteil deines Stroms in der Schweiz stammt aus Wasserkraft – diese dient als Puffer, wenn Sonne und Wind fehlen.",
    "Durch das Verschieben deines Stromverbrauchs in grüne Phasen entlastest du das Stromnetz und verhinderst den teuren Netzausbau."
];

let currentTipIdx = 0;
function rotateTips() {
    const tipEl = document.getElementById('grid-tip-text');
    if (!tipEl) return;
    
    // Fade out
    tipEl.style.opacity = '0';
    
    setTimeout(() => {
        tipEl.textContent = GRID_TIPS[currentTipIdx];
        currentTipIdx = (currentTipIdx + 1) % GRID_TIPS.length;
        // Fade in
        tipEl.style.opacity = '1';
    }, 500);
}

// Initial call and interval
rotateTips();
setInterval(rotateTips, 15000);

// ── Device Recommendations ──────────────────────────────────────────────────────
function renderDeviceCards(windows) {'''

content = re.sub(js_pattern, js_replacement, content, flags=re.DOTALL)

# 3. Inside the Boot loading logic, make sure renderCheapWindows is removed if called.
boot_pattern = r'renderCheapWindows\(data\.cheapest_windows\);\s*renderDeviceCards\(data\.cheapest_windows\);'
boot_replacement = r'renderDeviceCards(data.cheapest_windows);'
content = re.sub(boot_pattern, boot_replacement, content, flags=re.DOTALL)


with open("src/frontend/static/user_dash.html", "w") as f:
    f.write(content)

