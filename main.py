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

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options as ChromeOptions
from webdriver_manager.chrome import ChromeDriverManager

# ----------------------- Constants -----------------------
BASE_URL = "https://www.foxsports.com/nfl/schedule"
OURLADS_URL = "https://www.ourlads.com/nfldepthcharts/depthcharts.aspx"
TIMEOUT = 30
EDGE_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/139.0.0.0 Safari/537.36 Edg/139.0.0.0"
)

# Fox (abbr) -> Full team name
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

# Team primary hex (for headers)
TEAM_HEX = {
    "ARI":"#97233F","ATL":"#A71930","BAL":"#241773","BUF":"#00338D","CAR":"#0085CA","CHI":"#0B162A",
    "CIN":"#FB4F14","CLE":"#FF3C00","DAL":"#041E42","DEN":"#FB4F14","DET":"#0076B6","GB":"#203731",
    "HOU":"#03202F","IND":"#002C5F","JAX":"#006778","KC":"#E31837","LAC":"#0080C6","LAR":"#003594",
    "LV":"#000000","MIA":"#008E97","MIN":"#4F2683","NE":"#002244","NO":"#D3BC8D","NYG":"#0B2265",
    "NYJ":"#125740","PHI":"#004C54","PIT":"#FFB612","SEA":"#002244","SF":"#AA0000","TB":"#D50A0A",
    "TEN":"#4B92DB","WAS":"#5A1414"
}

MONTHS = {"JAN":1,"FEB":2,"MAR":3,"APR":4,"MAY":5,"JUN":6,"JUL":7,"AUG":8,"SEP":9,"OCT":10,"NOV":11,"DEC":12}

# ----------------------- Selenium (Fox Sports) -----------------------
def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--remote-debugging-port=9222")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    opts.add_argument(f"--user-agent={EDGE_UA}")

    service = ChromeService(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=opts)
    driver.set_page_load_timeout(TIMEOUT)
    driver.implicitly_wait(0)
    return driver

def _click_cookie_if_present(driver):
    try:
        candidates = [
            "//button[normalize-space()='Accept']",
            "//button[normalize-space()='I Agree']",
            "//button[normalize-space()='Agree & Continue']",
            "//button[normalize-space()='AGREE']",
            "//button[normalize-space()='ACCEPT']",
        ]
        for xp in candidates:
            btns = driver.find_elements(By.XPATH, xp)
            if btns:
                btns[0].click()
                time.sleep(0.4)
                return
    except Exception:
        pass

def _wait_for_any_table(driver, timeout=TIMEOUT):
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, ".scores-scorechips-container table.data-table"))
    )

def _safe_txt(el):
    try:
        return el.get_text(strip=True)
    except Exception:
        return None

def scrape_foxsports_schedule(season_type="reg", week=2, headless=True) -> pd.DataFrame:
    """
    season_type in {'pre','reg','post'}; week: int
    Returns DataFrame with:
    ['season_type','week','date_label','time_local','network','away','home','venue','city','odds','boxscore_url']
    """
    driver = make_driver(headless=headless)
    try:
        driver.get(BASE_URL)
        _click_cookie_if_present(driver)
        target = f"{BASE_URL}?seasonType={season_type}&week={week}"
        driver.get(target)
        _click_cookie_if_present(driver)
        _wait_for_any_table(driver)

        soup = BeautifulSoup(driver.page_source, "lxml")
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
    finally:
        driver.quit()

# ----------------------- Ourlads scraping (Offense) -----------------------
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
    # fix common cases like "Mcbride" -> "McBride"
    t = re.sub(r"\bMc([a-z])", lambda m: "Mc" + m.group(1).upper(), t)
    t = re.sub(r"\bMac([a-z])", lambda m: "Mac" + m.group(1).upper(), t)
    t = re.sub(r"\bO'([a-z])", lambda m: "O'" + m.group(1).upper(), t)
    return t

def _normalize_player(raw: str) -> str:
    """
    Input like 'HARRISON JR., MARVIN 24/1' or 'Jones, Zay CC/Jax' or 'Murray, Kyler 19/1'
    -> 'Marvin Harrison Jr.' / 'Zay Jones' / 'Kyler Murray'
    """
    s = (raw or "").replace("\xa0", " ").strip()
    if not s:
        return ""
    # Drop trailing tokens that include digits or '/'
    toks = s.split()
    keep = []
    for tok in toks:
        if any(ch.isdigit() for ch in tok) or "/" in tok or tok.endswith("^"):
            break
        keep.append(tok)
    s2 = " ".join(keep).strip()
    if not s2:
        s2 = s  # fallback
    # If "Last, First Middle" -> reorder
    if "," in s2:
        last, rest = s2.split(",", 1)
        name = f"{rest.strip()} {last.strip()}"
    else:
        name = s2
    # squeeze whitespace and title-case smartly
    name = re.sub(r"\s+", " ", name).strip()
    return _smart_title(name)

def parse_team_offense(soup: BeautifulSoup, team_full_name: str) -> dict:
    """
    Returns dict with lists: {'qb':[], 'rb':[], 'wr':[], 'te':[]}
    WR list is starters from LWR/RWR/SWR (fallback to WR row).
    """
    result = {"qb": [], "rb": [], "wr": [], "te": []}

    # Find the "Offense - Team Name" marker cell
    offense_cell = None
    for td in soup.select("td.dt-sh"):
        txt = td.get_text(" ", strip=True)
        if txt.lower().startswith("offense - ") and team_full_name.lower() in txt.lower():
            offense_cell = td
            break
    if not offense_cell:
        return result  # team not found (fallback to blanks)

    # Walk forward through sibling rows until next section for that team
    table = offense_cell.find_parent("table")
    tr = offense_cell.find_parent("tr")
    # gather rows after the marker
    lwr = rwr = swr = None
    while True:
        tr = tr.find_next_sibling("tr")
        if tr is None:
            break
        # next section header or other team/section -> stop
        hdr = tr.select_one("td.dt-sh")
        if hdr:
            # Stop at Defense/Special Teams/Practice Squad/Reserves OR new team Offense
            hdr_txt = hdr.get_text(" ", strip=True).lower()
            if any(hdr_txt.startswith(prefix) for prefix in
                   ["defense -", "special teams -", "practice squad -", "reserves -", "offense -"]):
                break

        tds = tr.find_all("td")
        if len(tds) < 4:
            continue
        # columns: Team | Pos | No. | Player1 | No | Player2 | ...
        pos = tds[1].get_text(strip=True)
        # All player anchors in this row (Player 1..5)
        players = [_normalize_player(a.get_text()) for a in tr.select("a") if a.get_text(strip=True)]
        players = [p for p in players if p]  # drop empties

        if pos == "QB":
            result["qb"] = players
        elif pos == "RB":
            result["rb"] = players
        elif pos == "TE":
            result["te"] = players
        elif pos in ("LWR", "RWR", "SWR"):
            starter = players[0] if players else ""
            if pos == "LWR":
                lwr = starter
            elif pos == "RWR":
                rwr = starter
            elif pos == "SWR":
                swr = starter
        elif pos == "WR" and not any((lwr, rwr, swr)):
            # Some teams may list just 'WR' row(s); collect top 3 overall
            if not result["wr"]:
                result["wr"] = players[:3]

    # If we saw position-specific WRs, set them in L/R/S order
    wrs = [w for w in (lwr, rwr, swr) if w]
    if wrs:
        result["wr"] = wrs[:3]

    return result

# ----------------------- HTML rendering -----------------------
def format_kickoff(date_label: str, time_local: str, season_year: int = 2025) -> str:
    """
    date_label like 'SUN, SEP 14'; time_local like '1:00PM'
    Returns: 'Sunday, September 14, 2025, 1:00 PM ET'
    """
    if not date_label:
        return f"TBD, {season_year}, {(time_local or 'TBD').replace('AM',' AM').replace('PM',' PM')} ET"
    parts = re.sub(r'^[A-Z]{3},\s*', '', date_label.strip(), flags=re.I)
    try:
        m_abbr, day = parts.split()
        month = MONTHS[m_abbr.upper()]
        year = season_year
        if month in (1, 2):
            year = season_year + 1
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
    # exactly n lines; blanks if short
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

    # Build player blocks (respect original template counts)
    away_qb_block = _bold_lines(away_off.get("qb", []), 1)
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
    # Cache team offense dicts so we only parse each once
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

        away = away_abbr or "AWAY"
        home = home_abbr or "HOME"
        week = int(row.get("week", 0))
        fname = f"W{week:02d}_{safe_slug(away)}_at_{safe_slug(home)}.txt"
        path = os.path.join(out_dir, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
        written.append(path)

    return written

def zip_files(file_paths: list[str]) -> bytes:
    bio = io.BytesIO()
    with zipfile.ZipFile(bio, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in file_paths:
            arc = os.path.basename(p)
            zf.write(p, arcname=arc)
    bio.seek(0)
    return bio.read()

# ----------------------- Streamlit UI -----------------------
st.set_page_config(page_title="NFL Schedule ➜ TXT Templates w/ Depth Charts", page_icon="🏈", layout="wide")
st.title("🏈 NFL Game .txt Generator (Schedule + Ourlads Depth Charts)")
st.caption("Scrape Fox Sports for a week, pull Ourlads offensive depth charts, and output one HTML-in-.txt per game.")

left, right = st.columns([2, 1])

with left:
    season_pick = st.radio(
        "Season type",
        ["Regular Season", "Preseason", "Postseason"],
        index=0, horizontal=True
    )
    season_map = {"Regular Season": "reg", "Preseason": "pre", "Postseason": "post"}
    season_type = season_map[season_pick]

    max_week = 18 if season_type == "reg" else 4
    week = st.number_input("Week", min_value=1, max_value=max_week, value=2, step=1)

with right:
    headless = st.toggle("Headless browser", value=True, help="Turn off to watch the browser locally.")
    season_year = st.number_input("Season start year", min_value=2000, max_value=2100, value=2025,
                                  help="Use the season's starting calendar year (Jan/Feb postseason rolls to +1).")
    out_dir = st.text_input("Output folder", value="out_txt")
    cache_minutes = st.slider("Cache minutes (Fox page HTML)", 0, 120, 30)
    if st.button("Force clear cache"):
        st.cache_data.clear()
        st.success("Cache cleared.")

ttl_seconds = int(cache_minutes * 60) if cache_minutes else None

@st.cache_data(ttl=ttl_seconds, show_spinner=False)
def get_df_cached(season_type, week, headless):
    return scrape_foxsports_schedule(season_type=season_type, week=int(week), headless=headless)

run = st.button("Scrape & Generate .txt files")

if run:
    with st.spinner(f"Scraping {season_pick} – Week {int(week)} and generating .txt files..."):
        try:
            df = get_df_cached(season_type, week, headless)
            if df.empty:
                st.warning("No rows found. The site may have changed its markup or the week has no games.")
            else:
                # Fetch Ourlads once
                soup = fetch_ourlads_soup()

                file_paths = write_txt_templates_with_depth(
                    df, soup, out_dir=out_dir, season_year=int(season_year)
                )
                zip_bytes = zip_files(file_paths)

                st.success(f"Created {len(file_paths)} .txt files in '{out_dir}'.")
                st.download_button(
                    "Download all as ZIP",
                    data=zip_bytes,
                    file_name=f"week_{int(week)}_{season_type}_txt_templates.zip",
                    mime="application/zip"
                )

                with st.expander("Show generated filenames"):
                    st.write("\n".join(os.path.basename(p) for p in file_paths))
        except Exception as e:
            st.error(f"Generation failed: {e}")
            st.exception(e)

st.divider()
with st.expander("Notes"):
    st.markdown(
        "- **WRs**: we use *LWR, RWR, SWR* starters from Ourlads (fallback to top WRs if needed).\n"
        "- **QBs**: QBs shows 1. RB shows 3; TE shows 1.\n"
        "- Times are presented as provided by Fox (treated as ET in the template text).\n"
    )
