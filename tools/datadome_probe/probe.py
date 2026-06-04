"""Sonde DataDome isolée — Phase 1 du plan "Débloquer LBC + SeLoger".

But : mesurer empiriquement le blocage et discriminer IP flaggée vs signature
de requête. NE MODIFIE RIEN en prod, n'importe rien du bot, aucun appel LLM.

Usage :
    python tools/datadome_probe/probe.py
"""
import json
import sys

try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

try:
    from curl_cffi import requests as ccffi
except ImportError:
    sys.exit("curl_cffi indisponible — `pip install curl_cffi`")

# ── Constantes copiées de scraper.py (autonome) ──────────────────────────────
LBC_API_URL = "https://api.leboncoin.fr/finder/search"
LBC_API_KEY = "ba0c2dad52b3ec"
LBC_UAS = [
    "leboncoin/8.10.0.0.0 iOS/17.0",
    "leboncoin/8.11.2.0.0 iOS/17.5",
    "leboncoin/8.10.5.0.0 Android/14",
]
LBC_FILTERS = {
    "category": {"id": "10"},
    "enums": {"real_estate_type": ["1", "2"], "furnished": ["1"]},
    "ranges": {"price": {"max": 1000}, "square": {"min": 25}},
    "location": {"locations": [
        {"locationType": "department", "department_id": "75", "label": "Paris"},
        {"locationType": "department", "department_id": "92", "label": "Hauts-de-Seine"},
        {"locationType": "department", "department_id": "93", "label": "Seine-Saint-Denis"},
        {"locationType": "department", "department_id": "94", "label": "Val-de-Marne"},
    ]},
}
SELOGER_URL = (
    "https://www.seloger.com/classified-search?distributionTypes=Rent"
    "&estateTypes=House,Apartment"
    "&locations=eyJwbGFjZUlkcyI6WyJTVFJURlI0NDA5MDQ1Il0sImR1cmF0aW9uIjoiNjAiLCJtb2RlIjoiVHJhbnNpdCJ9"
    "&priceMax=1000&projectTypes=Stock&spaceMin=25"
)


def line(t=""):
    print(t, flush=True)


def show_dd(r):
    """Détecte la signature DataDome dans une réponse."""
    body = (r.text or "")[:400]
    is_dd = "captcha-delivery" in body or "datadome" in body.lower()
    dd_cookie = None
    for k, v in r.headers.items():
        if k.lower() == "set-cookie" and "datadome" in v.lower():
            dd_cookie = v[:80]
    line(f"    status={r.status_code}  datadome_signature={is_dd}  set-cookie-dd={bool(dd_cookie)}")
    if is_dd:
        line(f"    body[:200]={body[:200]!r}")
    return is_dd


def test_ip():
    line("── [1] Identité IP / ASN ─────────────────────────────")
    ip_addr = "?"
    try:
        ip = ccffi.get("https://api.ipify.org?format=json", timeout=10).json()
        ip_addr = ip.get("ip") or "?"
        line(f"    IP publique : {ip_addr}")
    except Exception as e:
        line(f"    ipify échec : {e}")
        return ip_addr
    try:
        # ip-api gratuit, pas de clé, renvoie ASN/ISP
        info = ccffi.get(f"http://ip-api.com/json/{ip['ip']}?fields=isp,org,as,country,city",
                         timeout=10).json()
        line(f"    ISP : {info.get('isp')}")
        line(f"    ASN : {info.get('as')}")
        line(f"    Org : {info.get('org')}")
        line(f"    Loc : {info.get('city')}, {info.get('country')}")
        if "Orange" in (info.get("isp") or "") and "86.247.64.52" == ip_addr:
            line("    ⚠️ IP identique à l'IP flaggée (86.247.64.52) — pas changé.")
    except Exception as e:
        line(f"    ip-api échec : {e}")
    return ip_addr


def lbc_request(impersonate, ua, limit=1):
    headers = {
        "api_key": LBC_API_KEY, "User-Agent": ua,
        "Accept": "application/json", "Content-Type": "application/json",
        "Connection": "close",
    }
    body = {"limit": limit, "limit_alu": limit, "sort_by": "time",
            "sort_order": "desc", "filters": LBC_FILTERS}
    return ccffi.post(LBC_API_URL, headers=headers, json=body,
                      impersonate=impersonate, timeout=15)


def test_lbc():
    line("\n── [2] LBC API mobile — requête actuelle ─────────────")
    try:
        r = lbc_request("safari17_0", LBC_UAS[0])
        is_dd = show_dd(r)
        if r.status_code == 200:
            ads = (r.json() or {}).get("ads") or []
            line(f"    ✅ {len(ads)} annonce(s) renvoyée(s)")
            return True
    except Exception as e:
        line(f"    EXCEPTION : {e}")
    return False


def test_lbc_variants():
    line("\n── [4a] LBC — discriminateur (variantes sur IP constante) ──")
    blocked = 0
    total = 0
    for imp in ("safari17_0", "safari17_2", "chrome120", "chrome124"):
        for ua in (LBC_UAS[0], LBC_UAS[2]):
            total += 1
            try:
                r = lbc_request(imp, ua)
                ok = r.status_code == 200
                line(f"    imp={imp:12s} ua={ua[:28]:28s} -> {r.status_code}")
                if not ok:
                    blocked += 1
            except Exception as e:
                blocked += 1
                line(f"    imp={imp:12s} ua={ua[:28]:28s} -> EXC {e}")
    line(f"    => {blocked}/{total} variantes bloquées")
    return blocked, total


def test_seloger():
    line("\n── [3] SeLoger SSR — curl_cffi chrome120 ─────────────")
    try:
        r = ccffi.get(SELOGER_URL, impersonate="chrome120", timeout=30, allow_redirects=True)
        show_dd(r)
        has_fetcher = "__UFRN_FETCHER__" in (r.text or "")
        line(f"    __UFRN_FETCHER__ présent = {has_fetcher}  (len={len(r.text or '')})")
        return r.status_code == 200 and has_fetcher
    except Exception as e:
        line(f"    EXCEPTION : {e}")
    return False


def test_seloger_variants():
    line("\n── [4b] SeLoger — discriminateur (fingerprints sur IP constante) ──")
    blocked = 0
    total = 0
    for imp in ("chrome120", "chrome124", "safari17_0"):
        total += 1
        try:
            r = ccffi.get(SELOGER_URL, impersonate=imp, timeout=30, allow_redirects=True)
            ok = r.status_code == 200 and "__UFRN_FETCHER__" in (r.text or "")
            line(f"    imp={imp:12s} -> {r.status_code}  fetcher={'__UFRN_FETCHER__' in (r.text or '')}")
            if not ok:
                blocked += 1
        except Exception as e:
            blocked += 1
            line(f"    imp={imp:12s} -> EXC {e}")
    line(f"    => {blocked}/{total} variantes bloquées")
    return blocked, total


def main():
    line("=" * 60)
    line(" SONDE DATADOME — LBC + SeLoger")
    line(" (lance ce script branché en 4G pour comparer à l'IP Box)")
    line("=" * 60)
    ip_addr = test_ip()
    test_lbc()
    test_seloger()
    lbc_b, lbc_t = test_lbc_variants()
    sel_b, sel_t = test_seloger_variants()

    lbc_200, sel_200 = lbc_t - lbc_b, sel_t - sel_b
    total_200 = lbc_200 + sel_200

    line("\n" + "=" * 60)
    line(" VERDICT  (200 = passe ✓   |   403 = bloqué DataDome ✗)")
    line("=" * 60)
    line(f"  IP testée : {ip_addr}")
    line(f"  LBC      : {lbc_200} × 200   /   {lbc_b} × 403     (sur {lbc_t} tests)")
    line(f"  SeLoger  : {sel_200} × 200   /   {sel_b} × 403     (sur {sel_t} tests)")
    line("")
    if total_200 == 0:
        line("  ❌ ENCORE BLOQUÉ — 0 requête passe sur cette IP.")
        line("     → Si tu es en 4G : même la 4G est bloquée → passer au proxy.")
        line("     → Si tu es sur la Box : IP toujours flaggée.")
    elif lbc_200 > 0 and sel_200 > 0:
        line("  ✅ DÉBLOQUÉ — LBC ET SeLoger passent sur cette IP !")
        line("     → En 4G : CONFIRMÉ, l'IP était bien le seul problème.")
        line("     → Reste à pérenniser (proxy mobile/résidentiel ou 4G dédiée).")
    else:
        line("  ⚠️ PARTIEL — une source passe, pas l'autre (voir détails ci-dessus).")


if __name__ == "__main__":
    main()
