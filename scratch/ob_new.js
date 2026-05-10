// ── Onboarding ─────────────────────────────────────────────────────────────────
const OB_KEY = 'bdsp_appliances_v2';

const OB_DEVICES = [
    {
        key: 'washer', icon: 'dishwasher_gen',
        name: 'Waschmaschine / Spüler',
        q1: { label: 'Programmdauer', options: [
            { label: 'Kurz',     sub: '~1.5 h', duration_h: 1.5 },
            { label: 'Standard', sub: '~2 h',   duration_h: 2   },
            { label: 'Lang',     sub: '~3 h',   duration_h: 3   },
        ]},
        q2: { label: 'Wie oft pro Woche?', options: [
            { label: '1×',  sub: '1× / Woche', per_week: 1 },
            { label: '2×',  sub: '2× / Woche', per_week: 2 },
            { label: '3×',  sub: '3× / Woche', per_week: 3 },
            { label: '5×',  sub: '5× / Woche', per_week: 5 },
        ]},
    },
    {
        key: 'ev', icon: 'ev_station',
        name: 'EV Ladestation',
        q1: { label: 'Ladeleistung', options: [
            { label: '3,7 kW',  sub: 'Haushaltssteckdose', kw: 3.7 },
            { label: '7,4 kW',  sub: 'Einphasig',         kw: 7.4 },
            { label: '11 kW',   sub: 'Dreiphasig',        kw: 11  },
            { label: '22 kW',   sub: 'Schnelllader',      kw: 22  },
        ]},
        q2: { label: 'Energie pro Ladung', options: [
            { label: '10 kWh', sub: '~40 km', kwh_per_charge: 10 },
            { label: '20 kWh', sub: '~80 km', kwh_per_charge: 20 },
            { label: '40 kWh', sub: '~160 km', kwh_per_charge: 40 },
            { label: '60 kWh', sub: '~240 km', kwh_per_charge: 60 },
        ]},
    },
    {
        key: 'heatpump', icon: 'heat_pump',
        name: 'Wärmepumpe',
        q1: { label: 'Leistung', options: [
            { label: 'Klein',    sub: '2 kW',  kw: 2  },
            { label: 'Standard', sub: '5 kW',  kw: 5  },
            { label: 'Gross',    sub: '10 kW', kw: 10 },
        ]},
        q2: { label: 'Tägliche Laufzeit', options: [
            { label: '2 h',  sub: 'Übergangszeit', daily_hours: 2 },
            { label: '4 h',  sub: 'Normal',        daily_hours: 4 },
            { label: '6 h',  sub: 'Kalte Tage',    daily_hours: 6 },
            { label: '8 h',  sub: 'Winter',         daily_hours: 8 },
        ]},
    },
    {
        key: 'boiler', icon: 'water_heater',
        name: 'Boiler / Warmwasser',
        q1: { label: 'Kapazität', options: [
            { label: '100 L', sub: '2 kW · ~1 h',   kw: 2, heat_hours: 1   },
            { label: '200 L', sub: '3 kW · ~1.5 h', kw: 3, heat_hours: 1.5 },
            { label: '300 L', sub: '6 kW · ~2 h',   kw: 6, heat_hours: 2   },
        ]},
        q2: { label: 'Aufheizen bevorzugt', options: [
            { label: 'Morgens',  sub: '04–07 Uhr', pref_time: 'morning' },
            { label: 'Mittags',  sub: '11–14 Uhr', pref_time: 'midday'  },
            { label: 'Abends',   sub: '19–22 Uhr', pref_time: 'evening' },
            { label: 'Flexibel', sub: 'Günstigste', pref_time: 'flex'   },
        ]},
    },
];

let obStep = 0;
const obSelections = {};

function obLoad() {
    try { return JSON.parse(localStorage.getItem(OB_KEY)); } catch { return null; }
}

function obClose() {
    document.getElementById('ob-overlay').classList.add('hidden');
}

function _obBtn(label, isSelected, onclick) {
    return `<button onclick="${onclick}"
        class="flex flex-col items-center justify-center text-center p-3 rounded-xl border-2 transition-all duration-150 ${
          isSelected
            ? 'border-primary bg-primary/10 dark:bg-primary/15'
            : 'border-slate-200 dark:border-border-dark hover:border-primary/40 hover:bg-slate-50 dark:hover:bg-slate-800/40'
        }">
      <span class="font-bold text-xs ${isSelected ? 'text-primary' : 'text-slate-700 dark:text-slate-200'}">${label.label}</span>
      <span class="text-[10px] mt-0.5 leading-tight ${isSelected ? 'text-primary/70' : 'text-slate-400'}">${label.sub}</span>
    </button>`;
}

function obRender() {
    const card = document.getElementById('ob-card');

    // Step 0: Welcome
    if (obStep === 0) {
        card.innerHTML = `
          <div class="flex flex-col items-center text-center p-8 gap-6">
            <div class="w-16 h-16 bg-primary rounded-2xl flex items-center justify-center shadow-lg shadow-primary/30">
              <span class="material-symbols-outlined text-white text-[38px]" style="font-variation-settings:'FILL' 1">bolt</span>
            </div>
            <div class="flex flex-col gap-2">
              <h2 class="text-2xl font-black text-slate-900 dark:text-white">Willkommen bei<br>Swiss Energy Pulse</h2>
              <p class="text-sm text-slate-500 dark:text-slate-400 leading-relaxed">
                Richte in 4 kurzen Schritten deine Haushaltsgeräte ein –
                so zeigen wir dir, wann du am günstigsten Strom beziehst.
              </p>
            </div>
            <div class="flex flex-col gap-3 w-full">
              <button onclick="obStep=1;obRender()"
                      class="w-full bg-primary hover:bg-primary-dark text-white font-bold py-3 rounded-xl transition-colors shadow-sm">
                Jetzt einrichten →
              </button>
              <button onclick="obSkipAll()"
                      class="text-xs text-slate-400 hover:text-primary transition-colors py-1">
                Später einrichten
              </button>
            </div>
          </div>`;
        return;
    }

    // Step 5: Summary
    if (obStep === 5) {
        const active  = OB_DEVICES.filter(d => obSelections[d.key]?.enabled);
        const skipped = OB_DEVICES.filter(d => !obSelections[d.key]?.enabled);
        card.innerHTML = `
          <div class="flex flex-col items-center text-center p-8 gap-6">
            <div class="w-16 h-16 bg-emerald-500 rounded-2xl flex items-center justify-center shadow-lg shadow-emerald-500/30">
              <span class="material-symbols-outlined text-white text-[38px]" style="font-variation-settings:'FILL' 1">check</span>
            </div>
            <div class="flex flex-col gap-2">
              <h2 class="text-2xl font-black text-slate-900 dark:text-white">Alles bereit!</h2>
              <p class="text-sm text-slate-500 dark:text-slate-400">Deine Empfehlungen werden nun auf dich abgestimmt.</p>
            </div>
            <div class="w-full rounded-xl bg-slate-50 dark:bg-slate-800/60 border border-slate-100 dark:border-border-dark p-4 flex flex-col gap-2 text-left">
              ${active.map(d => `
                <div class="flex items-center gap-2">
                  <span class="material-symbols-outlined text-emerald-500 text-[18px]">check_circle</span>
                  <span class="text-sm text-slate-700 dark:text-slate-200">${d.name}</span>
                </div>`).join('')}
              ${skipped.map(d => `
                <div class="flex items-center gap-2">
                  <span class="material-symbols-outlined text-slate-300 dark:text-slate-600 text-[18px]">remove_circle</span>
                  <span class="text-sm text-slate-400">${d.name}</span>
                </div>`).join('')}
            </div>
            <button onclick="obFinish()"
                    class="w-full bg-primary hover:bg-primary-dark text-white font-bold py-3 rounded-xl transition-colors shadow-sm">
              Dashboard öffnen
            </button>
          </div>`;
        return;
    }

    // Steps 1–4: two-question device config
    const dev = OB_DEVICES[obStep - 1];
    const sel = obSelections[dev.key] || {};
    const hasQ1 = sel.q1idx !== undefined;
    const hasQ2 = sel.q2idx !== undefined;
    const canNext = hasQ1 && hasQ2;

    const dots = OB_DEVICES.map((_, i) => {
        if (i < obStep - 1)  return `<div class="w-2 h-2 rounded-full bg-primary/50"></div>`;
        if (i === obStep - 1) return `<div class="w-6 h-2 rounded-full bg-primary transition-all duration-300"></div>`;
        return `<div class="w-2 h-2 rounded-full bg-slate-200 dark:bg-slate-700"></div>`;
    }).join('');

    const cols1 = dev.q1.options.length === 4 ? 'grid-cols-4' : 'grid-cols-3';
    const cols2 = dev.q2.options.length === 4 ? 'grid-cols-4' : 'grid-cols-3';

    const q1btns = dev.q1.options.map((o, i) => _obBtn(o, hasQ1 && sel.q1idx === i, `obSetQ(1,${i})`)).join('');
    const q2btns = dev.q2.options.map((o, i) => _obBtn(o, hasQ2 && sel.q2idx === i, `obSetQ(2,${i})`)).join('');

    card.innerHTML = `
      <div class="flex flex-col gap-4 p-6">
        <div class="flex items-center justify-center gap-1.5">${dots}</div>
        <div class="flex flex-col items-center text-center gap-1">
          <div class="w-12 h-12 bg-slate-100 dark:bg-slate-800 rounded-2xl flex items-center justify-center border border-slate-200 dark:border-border-dark">
            <span class="material-symbols-outlined text-primary text-2xl">${dev.icon}</span>
          </div>
          <h3 class="font-black text-lg text-slate-900 dark:text-white">${dev.name}</h3>
        </div>
        <div>
          <p class="text-[11px] font-semibold text-slate-500 dark:text-slate-400 mb-1.5 uppercase tracking-wide">${dev.q1.label}</p>
          <div class="grid ${cols1} gap-1.5">${q1btns}</div>
        </div>
        <div>
          <p class="text-[11px] font-semibold text-slate-500 dark:text-slate-400 mb-1.5 uppercase tracking-wide">${dev.q2.label}</p>
          <div class="grid ${cols2} gap-1.5">${q2btns}</div>
        </div>
        <div class="flex flex-col gap-2 mt-1">
          <button onclick="obNext()" ${canNext ? '' : 'disabled'}
                  class="w-full font-bold py-3 rounded-xl transition-colors ${
                    canNext
                      ? 'bg-primary hover:bg-primary-dark text-white shadow-sm cursor-pointer'
                      : 'bg-slate-100 dark:bg-slate-800 text-slate-400 cursor-not-allowed'
                  }">
            ${obStep < 4 ? 'Weiter →' : 'Fertig ✓'}
          </button>
          <button onclick="obSkipDevice()"
                  class="text-xs text-slate-400 hover:text-slate-600 dark:hover:text-slate-300 transition-colors py-1">
            Ich habe dieses Gerät nicht
          </button>
        </div>
      </div>`;
}

function obSetQ(qNum, idx) {
    const dev = OB_DEVICES[obStep - 1];
    const sel = obSelections[dev.key] || { enabled: true };
    if (qNum === 1) {
        sel.q1idx = idx;
        Object.assign(sel, dev.q1.options[idx]);
    } else {
        sel.q2idx = idx;
        Object.assign(sel, dev.q2.options[idx]);
    }
    sel.enabled = true;
    obSelections[dev.key] = sel;
    obRender();
}

function obNext() {
    const dev = OB_DEVICES[obStep - 1];
    const sel = obSelections[dev.key];
    if (!sel?.enabled || sel.q1idx === undefined || sel.q2idx === undefined) return;
    obStep++;
    obRender();
}

function obSkipDevice() {
    const dev = OB_DEVICES[obStep - 1];
    obSelections[dev.key] = { enabled: false };
    obStep++;
    obRender();
}

function obSkipAll() {
    localStorage.setItem(OB_KEY, JSON.stringify({ configured: true, skipped: true, devices: {} }));
    obClose();
    if (_chartData) updateDeviceCards(_chartData.cheapest_windows);
}

function obFinish() {
    localStorage.setItem(OB_KEY, JSON.stringify({ configured: true, devices: obSelections }));
    obClose();
    if (_chartData) updateDeviceCards(_chartData.cheapest_windows);
}

// Show onboarding if not yet configured
if (!obLoad()) {
    document.getElementById('ob-overlay').classList.remove('hidden');
    obRender();
}
