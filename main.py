import os
import re
import io
import time
import zipfile
from datetime import datetime
from urllib.parse import urljoin

import requests
import pandas as pd
import streamlit as st
from bs4 import BeautifulSoup

# ----------------------- Constants -----------------------
BASE_URL = "https://www.foxsports.com/nfl/schedule"
OURLADS_URL = "https://www.ourlads.com/nfldepthcharts/depthcharts.aspx"

EDGE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
)

TEAM_NAME = {
    "ARI":"Arizona Cardinals","ATL":"Atlanta Falcons","BAL":"Baltimore Ravens","BUF":"Buffalo Bills",
    "CAR":"Carolina Panthers","CHI":"Chicago Bears","CIN":"Cincinnati Bengals","CLE":"Cleveland Browns",
    "DAL":"Dallas Cowboys","DEN":"Denver Broncos","DET":"Detroit Lions","GB":"Green Bay Packers",
    "HOU":"Houston Texans","IND":"Indianapolis Colts","JAX":"Jacksonville Jaguars","KC":"Kansas City Chiefs",
    "LAC":"Los Angeles Chargers","LAR":"Los Angeles Rams","LV":"Las Vegas Raiders","MIA":"Miami Dolphins",
    "MIN":"Minnesota Vikings","NE":"New England Patriots","NO":"New Orleans Saints","NYG":"New York Giants",
    "NYJ":"New York Jets","PHI":"Philadelphia Eagles","PIT":"Pittsburgh Steelers","SEA":"Seattle Seahawks",
    "SF":"San Francisco 49ers","TB":"Tampa Bay Buccaneers","TEN":"Tennessee Titans","WAS":"Washington Commanders"
}
TEAM_HEX = {
    "ARI":"#97233F","ATL":"#A71930","BAL":"#241773","BUF":"#00338D","CAR":"#0085CA","CHI":"#0B162A",
    "CIN":"#FB4F14","CLE":"#FF3C00","DAL":"#041E42","DEN":"#FB4F14","DET":"#0076B6","GB":"#203731",
    "HOU":"#03202F","IND":"#002C5F","JAX":"#006778","KC":"#E31837","LAC":"#0080C6","LAR":"#003594",
    "LV":"#000000","MIA":"#008E97","MIN":"#4F2683","NE":"#002244","NO":"#D3BC8D","NYG":"#0B2265",
    "NYJ":"#125740","PHI":"#004C54","PIT":"#FFB612","SEA":"#002244","SF":"#AA0000","TB":"#D50A0A",
    "TEN":"#4B92DB","WAS":"#5A1414"
}
MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

# ----------------------- Fox Sports (HTTP) -----------------------
def _safe_txt(el):
    try:
        return el.get_text(strip=True)
    except Exception:
        return None

def fetch_fox_html_http(season_type: str, week: int) -> str:
    headers = {
        "User-Agent": EDGE_UA,
        "Accept-Language": "en-US,en;q=0.8",
        "Referer": "https://www.foxsports.com/",
        "Cache-Control": "no-cache",
    }
    params = {"seasonType": season_type, "week": str(int(week))}
    r = requests.get(BASE_URL, headers=headers, params=params, timeout=30)
    r.raise_for_status()
    return r.text

def scrape_foxsports_schedule_http(season_type="reg", week=2) -> pd.DataFrame:
    html = fetch_fox_html_http(season_type, week)
    soup = BeautifulSoup(html, "lxml")
    segments = soup.select(".scores-scorechips-container .table-segment")

    rows = []
    for seg in segments:
        date_label = _safe_txt(seg.select_one(".table-title"))
        table = seg.select_one("table.data-table")
        if not table:
            continue
        tbody = table.select_one("tbody")
        if not tbody:
            continue

        for tr in tbody.select("tr"):
            tds = tr.select("td")
            if len(tds) < 6:
                continue

            teams = tr.select(".table-entity-name")
            away_abbr = _safe_txt(teams[0]) if len(teams) > 0 else None
            home_abbr = _safe_txt(teams[1]) if len(teams) > 1 else None

            box_a = tr.select_one(".table-entity a[href*='/nfl/week-']")
            box_url = urljoin(BASE_URL, box_a.get("href")) if box_a else None

            status_td = tds[3]
            time_el = status_td.select_one(".table-result")
            time_str = _safe_txt(time_el)

            net_span = status_td.select_one(".table-subtext")
            net_img = status_td.select_one("img.tv-station")
            network = _safe_txt(net_span) if net_span else (net_img.get("alt") if net_img else None)

            venue_td = tds[4]
            venue_full = venue_td.get_text(" ", strip=True)
            city_span = venue_td.select_one(".table-subtext")
            city = _safe_txt(city_span) if city_span else None
            venue_name = venue_full.replace(city, "").rstrip(", ").strip() if city else venue_full

            odds_el = tds[5].select_one(".table-result")
            odds = _safe_txt(odds_el)

            rows.append({
                "season_type": season_type,
                "week": int(week),
                "date_label": date_label,
                "time_local": time_str,
                "network": network,
                "away": away_abbr,
                "home": home_abbr,
                "venue": venue_name,
                "city": city,
                "odds": odds,
                "boxscore_url": box_url
            })

    cols = ["season_type","week","date_label","time_local","network",
            "away","home","venue","city","odds","boxscore_url"]
    return pd.DataFrame(rows, columns=cols)

# ----------------------- Ourlads (offense depth) -----------------------
def fetch_ourlads_soup() -> BeautifulSoup:
    headers = {"User-Agent": EDGE_UA}
    r = requests.get(OURLADS_URL, headers=headers, timeout=30)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")

def _smart_title(s: str) -> str:
    t = s.strip()
    if not t:
        return t
    t = t.title()
    t = re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), t)
    t = re.sub(r"\bMac([a-z])", lambda m: "Mac" + m.group(1).upper(), t)
    t = re.sub(r"\bO'([a-z])", lambda m: "O'" + m.group(1).upper(), t)
    return t

def _normalize_player(raw: str) -> str:
    s = (raw or "").replace("\xa0", " ").strip()
    if not s:
        return ""
    toks = s.split()
    keep = []
    for tok in toks:
        if any(ch.isdigit() for ch in tok) or "/" in tok or tok.endswith("^"):
            break
        keep.append(tok)
    s2 = " ".join(keep).strip() or s
    if "," in s2:
        last, rest = s2.split(",", 1)
        name = f"{rest.strip()} {last.strip()}"
    else:
        name = s2
    name = re.sub(r"\s+", " ", name).strip()
    return _smart_title(name)

def parse_team_offense(soup: BeautifulSoup, team_full_name: str) -> dict:
    result = {"qb": [], "rb": [], "wr": [], "te": []}
    offense_cell = None
    for td in soup.select("td.dt-sh"):
        txt = td.get_text(" ", strip=True)
        if txt.lower().startswith("offense - ") and team_full_name.lower() in txt.lower():
            offense_cell = td
            break
    if not offense_cell:
        return result

    tr = offense_cell.find_parent("tr")
    lwr = rwr = swr = None
    while True:
        tr = tr.find_next_sibling("tr")
        if tr is None:
            break
        hdr = tr.select_one("td.dt-sh")
        if hdr:
            hdr_txt = hdr.get_text(" ", strip=True).lower()
            if any(hdr_txt.startswith(prefix) for prefix in
                   ["defense -", "special teams -", "practice squad -", "reserves -", "offense -"]):
                break

        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        pos = tds[1].get_text(strip=True)
        players = [_normalize_player(a.get_text()) for a in tr.select("a") if a.get_text(strip=True)]
        players = [p for p in players if p]

        if pos == "QB":
            result["qb"] = players
        elif pos == "RB":
            result["rb"] = players
        elif pos == "TE":
            result["te"] = players
        elif pos in ("LWR", "RWR", "SWR"):
            starter = players[0] if players else ""
            if pos == "LWR": lwr = starter
            if pos == "RWR": rwr = starter
            if pos == "SWR": swr = starter
        elif pos == "WR" and not any((lwr, rwr, swr)):
            if not result["wr"]:
                result["wr"] = players[:3]

    wrs = [w for w in (lwr, rwr, swr) if w]
    if wrs:
        result["wr"] = wrs[:3]
    return result

# ----------------------- HTML generation -----------------------
def format_kickoff(date_label: str, time_local: str, season_year: int = 2025) -> str:
    if not date_label:
        return f"TBD, {season_year}, {(time_local or 'TBD').replace('AM',' AM').replace('PM',' PM')} ET"
    parts = re.sub(r'^[A-Z]{3},\s*', '', date_label.strip(), flags=re.I)
    try:
        m_abbr, day = parts.split()
        month = MONTHS[m_abbr.upper()]
        year = season_year + (1 if month in (1, 2) else 0)
        dt = datetime(year, month, int(day))
        tm = (time_local or "TBD").replace("AM", " AM").replace("PM", " PM")
        return f"{dt.strftime('%A')}, {dt.strftime('%B')} {dt.day}, {year}, {tm} ET"
    except Exception:
        tm = (time_local or "TBD").replace("AM", " AM").replace("PM", " PM")
        return f"{date_label}, {season_year}, {tm} ET"

def location_line(venue: str, city: str) -> str:
    if venue and city:
        return f"{venue}, {city}"
    return venue or city or "TBD"

def _bold_lines(names: list[str], n: int) -> str:
    names = names[:n] + [""] * max(0, n - len(names))
    return "\n".join([f"<b>{nm}</b>" if nm else "<b></b>" for nm in names])

def render_game_html(row: pd.Series, away_off: dict, home_off: dict, season_year: int = 2025) -> str:
    away = (row.get("away") or "").strip()
    home = (row.get("home") or "").strip()
    away_name = TEAM_NAME.get(away, away or "Away Team")
    home_name = TEAM_NAME.get(home, home or "Home Team")
    away_hex = TEAM_HEX.get(away, "#000000")
    home_hex = TEAM_HEX.get(home, "#000000")

    kickoff = format_kickoff(row.get("date_label") or "", row.get("time_local") or "", season_year=season_year)
    loc = location_line(row.get("venue") or "", row.get("city") or "")
    network = row.get("network") or ""

    away_qb_block = _bold_lines(away_off.get("qb", []), 2)
    away_rb_block = _bold_lines(away_off.get("rb", []), 3)
    away_wr_block = _bold_lines(away_off.get("wr", []), 3)
    away_te_block = _bold_lines(away_off.get("te", []), 1)

    home_qb_block = _bold_lines(home_off.get("qb", []), 1)
    home_rb_block = _bold_lines(home_off.get("rb", []), 3)
    home_wr_block = _bold_lines(home_off.get("wr", []), 3)
    home_te_block = _bold_lines(home_off.get("te", []), 1)

    html = f"""
<p style="text-align: center;"><span style="color: #339966; font-size: 28pt;"><strong>Game Info</strong></span></p>
&nbsp;
<p style="text-align: center;"><strong>Kickoff</strong>: {kickoff}</p>
<p style="text-align: center;"><strong>Location</strong>: {loc}</p>
<p style="text-align: center;"><strong>Network</strong>: {network}</p>
&nbsp;
<p style="text-align: center;"><span style="color: {away_hex};"><strong><span style="font-size: 28pt;" data-preserver-spaces="true">{away_name}</span></strong></span></p>
&nbsp;

<span style="font-size: 20pt; color: #3366ff;"><strong>Quarterback</strong></span>

{away_qb_block}

<span data-preserver-spaces="true">DISCUSSION</span>

&nbsp;

<span style="font-size: 20pt; color: #993300;"><strong>Running Back</strong></span>

{away_rb_block}

<span data-preserver-spaces="true">DISCUSSION</span>

&nbsp;

<span style="font-size: 20pt; color: #008000;"><strong>Wide Receiver</strong></span>

{away_wr_block}

<span data-preserver-spaces="true">DISCUSSION</span>

&nbsp;

<span style="font-size: 20pt; color: #333399;"><strong>Tight End</strong></span>

{away_te_block}

<span data-preserver-spaces="true">DISCUSSION</span>

&nbsp;
<p style="text-align: center;"><span style="color: {home_hex};"><strong><span style="font-size: 28pt;">{home_name}</span></strong></span></p>
&nbsp;

<span style="font-size: 20pt; color: #3366ff;"><strong>Quarterback</strong></span>

{home_qb_block}

DISCUSSION

&nbsp;

<span style="font-size: 20pt; color: #993300;"><strong>Running Back</strong></span>

{home_rb_block}

DISCUSSION

&nbsp;

<span style="font-size: 20pt; color: #008000;"><strong>Wide Receiver</strong></span>

{home_wr_block}

DISCUSSION

&nbsp;

<span style="font-size: 20pt; color: #333399;"><strong>Tight End</strong></span>

{home_te_block}

DISCUSSION
""".strip()
    return html

def safe_slug(s: str) -> str:
    return re.sub(r'[^A-Za-z0-9._-]+', '_', (s or "")).strip('_')

def write_txt_templates_with_depth(df: pd.DataFrame, soup: BeautifulSoup,
                                   out_dir: str = "out_txt", season_year: int = 2025) -> list[str]:
    os.makedirs(out_dir, exist_ok=True)
    written = []
    team_offense_cache: dict[str, dict] = {}

    for _, row in df.iterrows():
        away_abbr = (row.get("away") or "").strip()
        home_abbr = (row.get("home") or "").strip()
        away_full = TEAM_NAME.get(away_abbr, "")
        home_full = TEAM_NAME.get(home_abbr, "")

        if away_full not in team_offense_cache:
            team_offense_cache[away_full] = parse_team_offense(soup, away_full)
        if home_full not in team_offense_cache:
            team_offense_cache[home_full] = parse_team_offense(soup, home_full)

        away_off = team_offense_cache.get(away_full, {"qb":[], "rb":[], "wr":[], "te":[]})
        home_off = team_offense_cache.get(home_full, {"qb":[], "rb":[], "wr":[], "te":[]})

        html = render_game_html(row, away_off, home_off, season_year=season_year)

        away = aw
