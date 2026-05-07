import sys

with open("src/api/main.py", "r") as f:
    content = f.read()

# Add epex_lo_arr and epex_hi_arr
target_init = '''        gesamt_lo_arr: list[float | None] = []
        gesamt_hi_arr: list[float | None] = []
        net_load_lo_arr: list[float | None] = []
        net_load_hi_arr: list[float | None] = []'''

replacement_init = '''        gesamt_lo_arr: list[float | None] = []
        gesamt_hi_arr: list[float | None] = []
        net_load_lo_arr: list[float | None] = []
        net_load_hi_arr: list[float | None] = []
        epex_lo_arr: list[float | None] = []
        epex_hi_arr: list[float | None] = []'''

content = content.replace(target_init, replacement_init)

target_loop = '''            if ci_active:
                half = _CI_Z * sigma_eur
                e_lo = _ep(epex - half)
                e_hi = _ep(epex + half)
                gesamt_lo_arr.append(round(_gt(netz, e_lo), 2))
                gesamt_hi_arr.append(round(_gt(netz, e_hi), 2))
            else:
                gesamt_lo_arr.append(None)
                gesamt_hi_arr.append(None)'''

replacement_loop = '''            if ci_active:
                half = _CI_Z * sigma_eur
                e_lo = _ep(epex - half)
                e_hi = _ep(epex + half)
                gesamt_lo_arr.append(round(_gt(netz, e_lo), 2))
                gesamt_hi_arr.append(round(_gt(netz, e_hi), 2))
                epex_lo_arr.append(round(epex - half, 2))
                epex_hi_arr.append(round(epex + half, 2))
            else:
                gesamt_lo_arr.append(None)
                gesamt_hi_arr.append(None)
                epex_lo_arr.append(None)
                epex_hi_arr.append(None)'''

content = content.replace(target_loop, replacement_loop)

target_return = '''            "ci_available":       ci_active,
            "gesamttarif_ci_lower": gesamt_lo_arr,
            "gesamttarif_ci_upper": gesamt_hi_arr,
            "net_load_ci_lower":    net_load_lo_arr,
            "net_load_ci_upper":    net_load_hi_arr,'''

replacement_return = '''            "ci_available":       ci_active,
            "gesamttarif_ci_lower": gesamt_lo_arr,
            "gesamttarif_ci_upper": gesamt_hi_arr,
            "net_load_ci_lower":    net_load_lo_arr,
            "net_load_ci_upper":    net_load_hi_arr,
            "epex_ci_lower":        epex_lo_arr,
            "epex_ci_upper":        epex_hi_arr,'''

content = content.replace(target_return, replacement_return)

with open("src/api/main.py", "w") as f:
    f.write(content)

