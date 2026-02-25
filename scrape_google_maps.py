"""
INTERSPORT SCRAPER v4 — Crash-proof
====================================
- Fresh Chrome per city (no memory leak)
- Auto-saves after each city (never lose data)
- Resume: skips cities already in the CSV
- Visible mode recommended

pip install selenium webdriver-manager beautifulsoup4
python scrape_google_maps.py --visible
"""

import csv, os, re, sys, time, hashlib
from datetime import datetime, timedelta
from bs4 import BeautifulSoup

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import *
except ImportError:
    print("Run: pip install selenium webdriver-manager beautifulsoup4")
    sys.exit(1)

try:
    from webdriver_manager.chrome import ChromeDriverManager
    HAS_WDM = True
except ImportError:
    HAS_WDM = False

OUTPUT = "data/raw/reviews_raw.csv"
os.makedirs("data/raw", exist_ok=True)

CITIES = [
    ("Paris",       48.8566,  2.3522),
    ("Creteil",     48.7835,  2.4598),
    ("Velizy",      48.7847,  2.1900),
    ("Grenoble",    45.1885,  5.7245),
    ("Lyon",        45.7640,  4.8357),
    ("Marseille",   43.2965,  5.3698),
    ("Bordeaux",    44.8378, -0.5792),
    ("Lille",       50.6292,  3.0573),
    ("Toulouse",    43.6047,  1.4442),
    ("Nice",        43.7102,  7.2620),
    ("Strasbourg",  48.5734,  7.7521),
    ("Rennes",      48.1173, -1.6778),
    ("Nantes",      47.2184, -1.5536),
    ("Annecy",      45.8992,  6.1294),
    ("Montpellier", 43.6108,  3.8767),
]

PARIS_LAT = (48.815, 48.902)
PARIS_LNG = (2.225, 2.420)

def guess_region(text):
    t = text.lower()
    for kw, reg in [("paris","Ile-de-France"),("75","Ile-de-France"),("92","Ile-de-France"),
        ("93","Ile-de-France"),("94","Ile-de-France"),("78","Ile-de-France"),
        ("creteil","Ile-de-France"),("velizy","Ile-de-France"),("rosny","Ile-de-France"),
        ("grenoble","Auvergne-Rhone-Alpes"),("lyon","Auvergne-Rhone-Alpes"),
        ("annecy","Auvergne-Rhone-Alpes"),("chambery","Auvergne-Rhone-Alpes"),
        ("marseille","PACA"),("nice","PACA"),("toulouse","Occitanie"),
        ("montpellier","Occitanie"),("bordeaux","Nouvelle-Aquitaine"),
        ("lille","Hauts-de-France"),("strasbourg","Grand Est"),
        ("rennes","Bretagne"),("nantes","Pays de la Loire")]:
        if kw in t: return reg
    return "France"

def classify_zone(lat, lng, region):
    if "Ile-de-France" in region:
        if PARIS_LAT[0]<=lat<=PARIS_LAT[1] and PARIS_LNG[0]<=lng<=PARIS_LNG[1]:
            return "Paris Intra Muros"
        return "Paris Extra Muros"
    return "Province"

def extract_city(addr):
    m = re.search(r'\d{5}\s+([A-Za-z\u00C0-\u017F\s-]+)', addr)
    return m.group(1).strip() if m else addr.split(",")[-2].strip() if "," in addr else addr

def parse_date(text):
    now = datetime.now()
    t = text.lower()
    nums = re.findall(r'(\d+)', t)
    n = int(nums[0]) if nums else 1
    if any(w in t for w in ["jour","day"]): return (now-timedelta(days=n)).strftime("%Y-%m-%d")
    if any(w in t for w in ["semaine","week"]): return (now-timedelta(weeks=n)).strftime("%Y-%m-%d")
    if any(w in t for w in ["mois","month"]):
        mo,yr = now.month-n, now.year
        while mo<=0: mo+=12; yr-=1
        return f"{yr}-{mo:02d}-15"
    if any(w in t for w in ["an","year"]): return f"{now.year-n}-06-15"
    return now.strftime("%Y-%m-%d")


def make_driver(headless):
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=fr-FR")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_experimental_option("excludeSwitches", ["enable-automation"])
    opts.add_experimental_option("useAutomationExtension", False)
    if HAS_WDM:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    else:
        driver = webdriver.Chrome(options=opts)
    driver.execute_script("Object.defineProperty(navigator,'webdriver',{get:()=>undefined})")
    return driver


def accept_cookies(driver):
    try:
        btn = WebDriverWait(driver, 5).until(EC.element_to_be_clickable(
            (By.XPATH, "//button[contains(.,'Tout accepter') or contains(.,'Accept all')]")))
        btn.click()
        time.sleep(2)
    except: pass


def get_gps(driver):
    m = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', driver.current_url)
    return (float(m.group(1)), float(m.group(2))) if m else (0.0, 0.0)


def find_stores(driver, city, lat, lng):
    url = f"https://www.google.com/maps/search/Intersport/@{lat},{lng},12z"
    print(f"  Search: Intersport near {city}")
    driver.get(url)
    time.sleep(5)
    accept_cookies(driver)
    time.sleep(2)

    # Scroll feed
    try:
        feed = driver.find_element(By.CSS_SELECTOR, 'div[role="feed"]')
        last = 0
        for _ in range(10):
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", feed)
            time.sleep(2)
            cur = len(driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc"))
            if cur == last: break
            last = cur
    except: pass

    results = []
    seen = set()
    for el in driver.find_elements(By.CSS_SELECTOR, "a.hfpxzc"):
        href = el.get_attribute("href") or ""
        label = (el.get_attribute("aria-label") or "").strip()
        if "intersport" in label.lower() and href and href not in seen:
            seen.add(href)
            results.append({"name": label, "href": href})

    print(f"  Found {len(results)} stores")
    return results


def scrape_reviews(driver, href, max_rev=50):
    driver.get(href)
    time.sleep(5)

    # Name
    name = ""
    try: name = driver.find_element(By.CSS_SELECTOR, "h1").text.strip()
    except: pass

    # Address
    address = ""
    try: address = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id="address"]').text.strip()
    except:
        try:
            els = driver.find_elements(By.XPATH, '//button[contains(@aria-label,"dresse")]')
            if els: address = els[0].get_attribute("aria-label").split(":")[-1].strip()
        except: pass

    lat, lng = get_gps(driver)

    # Open reviews tab
    opened = False
    # Try tab buttons
    try:
        for tab in driver.find_elements(By.CSS_SELECTOR, 'button[role="tab"]'):
            txt = (tab.get_attribute("aria-label") or tab.text or "").lower()
            if "avis" in txt or "review" in txt:
                tab.click()
                time.sleep(4)
                opened = True
                break
    except: pass

    # Try clicking review count
    if not opened:
        try:
            for el in driver.find_elements(By.TAG_NAME, "button"):
                if re.search(r'\d+\s*avis', el.text.lower()):
                    el.click()
                    time.sleep(4)
                    opened = True
                    break
        except: pass

    if not opened:
        return name, address, lat, lng, []

    # Sort newest
    try:
        sort = driver.find_element(By.CSS_SELECTOR, 'button[aria-label*="rier"]')
        sort.click()
        time.sleep(1)
        for item in driver.find_elements(By.CSS_SELECTOR, 'div[role="menuitemradio"]'):
            if "recent" in item.text.lower() or "nouveau" in item.text.lower():
                item.click()
                time.sleep(3)
                break
    except: pass

    # Scroll reviews
    scrollable = None
    for sel in ['div.m6QErb.DxyBCb.kA9KIf.dS8AEf', 'div.m6QErb.DxyBCb', 'div.m6QErb']:
        try: scrollable = driver.find_element(By.CSS_SELECTOR, sel); break
        except: continue

    if scrollable:
        last = 0
        for _ in range(max_rev//4 + 5):
            driver.execute_script("arguments[0].scrollTop = arguments[0].scrollHeight", scrollable)
            time.sleep(1.5)
            cnt = len(driver.find_elements(By.CSS_SELECTOR, "div.jftiEf"))
            if cnt >= max_rev or cnt == last: break
            last = cnt

    # Expand "More"
    for btn in driver.find_elements(By.CSS_SELECTOR, "button.w8nwRe.kyuRq"):
        try: driver.execute_script("arguments[0].click();", btn)
        except: pass
    time.sleep(0.5)

    # Parse with BeautifulSoup
    soup = BeautifulSoup(driver.page_source, "html.parser")
    reviews = []
    for div in soup.find_all("div", class_="jftiEf"):
        try:
            nel = div.find("div", class_="d4r55")
            reviewer = nel.get_text(strip=True) if nel else "Anonyme"

            rating = 0
            sel = div.find("span", class_="kvMYJc")
            if sel:
                m = re.search(r'(\d)', sel.get("aria-label",""))
                if m: rating = int(m.group(1))
            if not rating:
                for ae in div.find_all(attrs={"aria-label": True}):
                    m = re.search(r'(\d)\s*(?:star|toile|sur|/)', ae["aria-label"], re.I)
                    if m: rating = int(m.group(1)); break

            del_ = div.find("span", class_="rsqaWe")
            date = parse_date(del_.get_text(strip=True)) if del_ else ""

            tel = div.find("span", class_="wiI7pd")
            text = tel.get_text(strip=True) if tel else ""

            if text and len(text) >= 5 and rating > 0:
                reviews.append({"review_text": text, "rating": rating,
                                "date": date, "reviewer_name": reviewer})
        except: continue

    return name, address, lat, lng, reviews


def load_existing():
    """Load already-scraped data so we can resume."""
    if not os.path.exists(OUTPUT):
        return [], set()
    existing = []
    done_cities = set()
    with open(OUTPUT, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            existing.append(row)
            # Track which search_city was already done
            if "search_city" in row:
                done_cities.add(row["search_city"])
    return existing, done_cities


def save_all(reviews):
    """Save all reviews to CSV."""
    if not reviews: return
    fields = ["review_id","review_text","rating","date","reviewer_name",
              "store_name","address","city","zone","region",
              "latitude","longitude","source","search_city"]
    with open(OUTPUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(reviews)


def main():
    print("="*60)
    print("INTERSPORT SCRAPER v4 — Crash-proof")
    print("="*60)

    headless = "--visible" not in sys.argv
    max_rev = 50
    for i, a in enumerate(sys.argv):
        if a == "--max" and i+1 < len(sys.argv):
            try: max_rev = int(sys.argv[i+1])
            except: pass

    print(f"Mode: {'headless' if headless else 'VISIBLE'}")
    print(f"Max reviews/store: {max_rev}")

    # Load existing data to resume
    all_reviews, done_cities = load_existing()
    if all_reviews:
        print(f"Resuming: {len(all_reviews)} reviews already saved")
        print(f"Cities done: {done_cities}")
    counter = len(all_reviews)

    scraped_hrefs = set(r.get("address","") for r in all_reviews)

    for city_name, city_lat, city_lng in CITIES:
        if city_name in done_cities:
            print(f"\n  SKIP {city_name} (already done)")
            continue

        print(f"\n{'='*50}")
        print(f"CITY: {city_name}")
        print(f"{'='*50}")

        # Fresh Chrome for each city (prevents memory/session issues)
        driver = None
        try:
            driver = make_driver(headless)
            stores = find_stores(driver, city_name, city_lat, city_lng)

            for i, store in enumerate(stores):
                print(f"  Store {i+1}/{len(stores)}: {store['name']}")

                try:
                    name, address, lat, lng, reviews = scrape_reviews(driver, store["href"], max_rev)
                    if not name: name = store["name"]
                    region = guess_region(address or city_name)
                    zone = classify_zone(lat, lng, region)
                    scity = extract_city(address) if address else city_name

                    for r in reviews:
                        counter += 1
                        r.update({
                            "review_id": f"REV-{counter:05d}",
                            "store_name": name, "address": address,
                            "city": scity, "zone": zone, "region": region,
                            "latitude": lat, "longitude": lng,
                            "source": "google_maps", "search_city": city_name
                        })
                    all_reviews.extend(reviews)
                    print(f"    -> {len(reviews)} reviews")

                except Exception as e:
                    print(f"    -> ERROR: {e}")
                    # If session died, break out of stores loop
                    if "session" in str(e).lower():
                        print(f"    Chrome crashed. Moving to next city.")
                        break

                time.sleep(2)

        except Exception as e:
            print(f"  City error: {e}")

        finally:
            # Always close Chrome
            if driver:
                try: driver.quit()
                except: pass

        # Save after EACH city (never lose data)
        save_all(all_reviews)
        print(f"  Saved: {len(all_reviews)} total reviews so far")
        time.sleep(3)

    # Final stats
    if all_reviews:
        stores = set(r["store_name"] for r in all_reviews)
        zones = {}
        for r in all_reviews:
            z = r.get("zone","?")
            zones[z] = zones.get(z,0)+1
        print(f"\n{'='*60}")
        print(f"DONE: {OUTPUT}")
        print(f"  {len(all_reviews)} reviews | {len(stores)} stores")
        for z, n in sorted(zones.items()): print(f"    {z}: {n}")
    else:
        print("\n0 reviews. Try: python scrape_google_maps.py --visible")


if __name__ == "__main__":
    main()