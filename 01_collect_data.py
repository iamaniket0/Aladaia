"""
================================================================================
STEP 1: Data Collection
================================================================================
Three modes:
  1. OUTSCRAPER API — scrapes Google Maps reviews at scale ($3/1K reviews)
  2. GOOGLE PLACES API — official, limited to 5 reviews/place
  3. SIMULATED — realistic demo data when no API keys

Handles: multiple stores per city, deduplication, rate limits, GPS, zones.
Usage:
  OUTSCRAPER_API_KEY=xxx python 01_collect_data.py
  GOOGLE_API_KEY=xxx python 01_collect_data.py
  python 01_collect_data.py   # simulated fallback
================================================================================
"""
import csv, os, re, json, time, hashlib, random
from datetime import datetime, timedelta
try:
    import requests
    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

OUTPUT_DIR = "data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

PARIS_LAT = (48.815, 48.902)
PARIS_LNG = (2.225, 2.420)

def is_paris_intra(lat, lng):
    return PARIS_LAT[0] <= lat <= PARIS_LAT[1] and PARIS_LNG[0] <= lng <= PARIS_LNG[1]

def classify_zone(lat, lng, region):
    if "France" in region and ("Ile" in region or "le-de" in region):
        return "Paris Intra Muros" if is_paris_intra(lat, lng) else "Paris Extra Muros"
    return "Province"

def dedup(reviews):
    seen = set()
    out = []
    for r in reviews:
        # Include store so same review text at different stores is kept
        key = r.get("review_text","") + "|" + r.get("store_name","")
        h = hashlib.md5(key.encode()).hexdigest()
        if h not in seen:
            seen.add(h)
            out.append(r)
    return out

# ---- OUTSCRAPER ----
class OutscraperScraper:
    def __init__(self, key):
        self.s = requests.Session()
        self.s.headers["X-API-KEY"] = key

    def find_stores(self, query, location, limit=20):
        print(f"  [OUTSCRAPER] Search: {query} near {location}")
        try:
            r = self.s.get("https://api.app.outscraper.com/maps/search-v3",
                params={"query": query, "location": location, "limit": limit, "language": "fr"}, timeout=30)
            r.raise_for_status()
            stores = []
            for place in r.json().get("data", []):
                items = place if isinstance(place, list) else [place]
                for item in items:
                    if not isinstance(item, dict): continue
                    if "intersport" not in item.get("name","").lower(): continue
                    stores.append({"name": item.get("name",""), "address": item.get("full_address",""),
                        "lat": item.get("latitude",0), "lng": item.get("longitude",0),
                        "place_id": item.get("place_id","")})
            print(f"  Found {len(stores)} stores")
            return stores
        except Exception as e:
            print(f"  Error: {e}")
            return []

    def get_reviews(self, place_id, limit=100):
        print(f"  [OUTSCRAPER] Reviews for {place_id[:20]}...")
        try:
            r = self.s.get("https://api.app.outscraper.com/maps/reviews-v3",
                params={"query": place_id, "reviewsLimit": limit, "language": "fr", "sort": "newest"}, timeout=60)
            r.raise_for_status()
            reviews = []
            for place in r.json().get("data", []):
                items = place if isinstance(place, list) else [place]
                for item in items:
                    if not isinstance(item, dict): continue
                    for rev in item.get("reviews_data", []):
                        text = rev.get("review_text","")
                        if len(text) < 10: continue
                        reviews.append({"review_text": text, "rating": rev.get("review_rating",0),
                            "date": rev.get("review_datetime_utc",""), "reviewer_name": rev.get("author_title","Anonyme")})
            print(f"  Got {len(reviews)} reviews")
            return reviews
        except Exception as e:
            print(f"  Error: {e}")
            return []

    def scrape_area(self, area, per_store=50):
        stores = self.find_stores("Intersport", area)
        all_revs = []
        for i, store in enumerate(stores):
            print(f"  Store {i+1}/{len(stores)}: {store['name']}")
            revs = self.get_reviews(store["place_id"], per_store)
            city = re.search(r'\d{5}\s+([A-Za-z\s-]+)', store["address"])
            city = city.group(1).strip() if city else store["address"].split(",")[-2].strip() if "," in store["address"] else ""
            region = self._region(store["address"])
            zone = classify_zone(store["lat"], store["lng"], region)
            for rev in revs:
                rev.update({"store_name": store["name"], "address": store["address"],
                    "city": city, "zone": zone, "region": region,
                    "latitude": store["lat"], "longitude": store["lng"], "source": "outscraper"})
            all_revs.extend(revs)
            time.sleep(1)
        return dedup(all_revs)

    def _region(self, addr):
        a = addr.lower()
        for k, v in {"paris":"Ile-de-France","75":"Ile-de-France","92":"Ile-de-France",
            "93":"Ile-de-France","94":"Ile-de-France","78":"Ile-de-France",
            "grenoble":"Auvergne-Rhone-Alpes","lyon":"Auvergne-Rhone-Alpes","38":"Auvergne-Rhone-Alpes",
            "69":"Auvergne-Rhone-Alpes","marseille":"PACA","nice":"PACA","13":"PACA",
            "toulouse":"Occitanie","bordeaux":"Nouvelle-Aquitaine","lille":"Hauts-de-France",
            "strasbourg":"Grand Est","rennes":"Bretagne","nantes":"Pays de la Loire"}.items():
            if k in a: return v
        return "France"

# ---- SIMULATED ----
STORES = [
    {"n":"Intersport Rivoli","a":"30 Rue de Rivoli, 75004 Paris","c":"Paris 4e","r":"Ile-de-France","lat":48.8566,"lng":2.3522},
    {"n":"Intersport Republique","a":"5 Place de la Republique, 75003 Paris","c":"Paris 3e","r":"Ile-de-France","lat":48.8676,"lng":2.3636},
    {"n":"Intersport Montparnasse","a":"CC Montparnasse, 75015 Paris","c":"Paris 15e","r":"Ile-de-France","lat":48.8422,"lng":2.3219},
    {"n":"Intersport Italie 2","a":"CC Italie 2, 75013 Paris","c":"Paris 13e","r":"Ile-de-France","lat":48.8283,"lng":2.3558},
    {"n":"Intersport Creteil Soleil","a":"CC Creteil Soleil, 94000 Creteil","c":"Creteil","r":"Ile-de-France","lat":48.7835,"lng":2.4598},
    {"n":"Intersport Velizy 2","a":"CC Velizy 2, 78140 Velizy","c":"Velizy","r":"Ile-de-France","lat":48.7847,"lng":2.1900},
    {"n":"Intersport Rosny 2","a":"CC Rosny 2, 93110 Rosny","c":"Rosny-sous-Bois","r":"Ile-de-France","lat":48.8728,"lng":2.4837},
    {"n":"Intersport Parly 2","a":"CC Parly 2, 78150 Le Chesnay","c":"Le Chesnay","r":"Ile-de-France","lat":48.8219,"lng":2.1278},
    {"n":"Intersport Grand Place","a":"CC Grand Place, 38100 Grenoble","c":"Grenoble","r":"Auvergne-Rhone-Alpes","lat":45.1608,"lng":5.7350},
    {"n":"Intersport Caserne de Bonne","a":"Caserne de Bonne, 38000 Grenoble","c":"Grenoble","r":"Auvergne-Rhone-Alpes","lat":45.1856,"lng":5.7264},
    {"n":"Intersport Part-Dieu","a":"CC Part-Dieu, 69003 Lyon","c":"Lyon","r":"Auvergne-Rhone-Alpes","lat":45.7604,"lng":4.8590},
    {"n":"Intersport Confluence","a":"CC Confluence, 69002 Lyon","c":"Lyon","r":"Auvergne-Rhone-Alpes","lat":45.7420,"lng":4.8184},
    {"n":"Intersport Annecy Courier","a":"CC Courier, 74000 Annecy","c":"Annecy","r":"Auvergne-Rhone-Alpes","lat":45.9009,"lng":6.1198},
    {"n":"Intersport Chamnord","a":"CC Chamnord, 73000 Chambery","c":"Chambery","r":"Auvergne-Rhone-Alpes","lat":45.5751,"lng":5.9189},
    {"n":"Intersport Labege","a":"CC Labege 2, 31670 Labege","c":"Labege","r":"Occitanie","lat":43.5356,"lng":1.5069},
    {"n":"Intersport Blagnac","a":"CC Blagnac, 31700 Blagnac","c":"Blagnac","r":"Occitanie","lat":43.6377,"lng":1.3756},
    {"n":"Intersport Odysseum","a":"CC Odysseum, 34000 Montpellier","c":"Montpellier","r":"Occitanie","lat":43.6045,"lng":3.9193},
    {"n":"Intersport Lac","a":"CC Bordeaux Lac, 33300 Bordeaux","c":"Bordeaux","r":"Nouvelle-Aquitaine","lat":44.8752,"lng":-0.5673},
    {"n":"Intersport Meriadeck","a":"CC Meriadeck, 33000 Bordeaux","c":"Bordeaux","r":"Nouvelle-Aquitaine","lat":44.8378,"lng":-0.5792},
    {"n":"Intersport Euralille","a":"CC Euralille, 59000 Lille","c":"Lille","r":"Hauts-de-France","lat":50.6365,"lng":3.0701},
    {"n":"Intersport Englos","a":"CC Englos, 59320 Englos","c":"Englos","r":"Hauts-de-France","lat":50.6431,"lng":2.9812},
    {"n":"Intersport Valentine","a":"CC La Valentine, 13011 Marseille","c":"Marseille","r":"PACA","lat":43.2926,"lng":5.4841},
    {"n":"Intersport Grand Littoral","a":"CC Grand Littoral, 13016 Marseille","c":"Marseille","r":"PACA","lat":43.3619,"lng":5.3520},
    {"n":"Intersport Nice TNL","a":"CC TNL, 06200 Nice","c":"Nice","r":"PACA","lat":43.7046,"lng":7.2630},
    {"n":"Intersport Rivetoile","a":"CC Rivetoile, 67100 Strasbourg","c":"Strasbourg","r":"Grand Est","lat":48.5692,"lng":7.7716},
    {"n":"Intersport Alma","a":"CC Alma, 35000 Rennes","c":"Rennes","r":"Bretagne","lat":48.1147,"lng":-1.6794},
    {"n":"Intersport Atlantis","a":"CC Atlantis, 44800 Saint-Herblain","c":"Saint-Herblain","r":"Pays de la Loire","lat":47.2263,"lng":-1.6215},
]

TEMPLATES = {
    "accueil_pos": ["Tres bon accueil. Le personnel est souriant et disponible.","Accueil chaleureux, le vendeur m'a aide des mon arrivee.","On se sent bienvenu des la porte. Equipe sympathique."],
    "conseil_pos": ["Le vendeur m'a tres bien conseille sur le modele adapte a ma foulee.","Excellents conseils pour mes chaussures de randonnee. Vrai pro.","Super conseil personnalise pour mon velo. Technicien patient.","Vendeur passionne de trail, test de foulee offert."],
    "reparation_pos": ["Reparation rapide et soignee. Le technicien a verifie chaque detail.","Mon ski repare et teste avant de me le rendre. Impeccable.","Excellent service atelier. Teste apres reparation, tout marche."],
    "reparation_neg": ["Reparation baclee. Mon velo faisait encore du bruit. Pas teste.","Produit rendu sans test. Ne fonctionnait toujours pas.","3 semaines d'attente pour une reparation mal faite.","Le technicien n'a pas teste le produit apres reparation."],
    "attente_neg": ["Attente trop longue en caisse. 2 caisses ouvertes un samedi.","25 minutes d'attente. Magasin en sous-effectif.","File d'attente interminable le week-end."],
    "prix_pos": ["Bon rapport qualite-prix. Promotions interessantes.","Prix corrects. La carte fidelite offre de vrais avantages."],
    "prix_neg": ["Prix plus eleves qu'en ligne pour le meme produit.","Trop cher par rapport a Decathlon."],
    "choix_pos": ["Grand choix de running. Toutes les marques.","Rayon ski complet pour tous niveaux.","Beau magasin bien agence, large choix."],
    "choix_neg": ["Peu de choix en grandes tailles. Reparti les mains vides.","Rupture de stock sur le modele voulu."],
    "magasin_pos": ["Magasin propre et bien organise.","Bel espace lumineux, parking facile."],
    "magasin_neg": ["Magasin en desordre. Tailles melangees.","Parking trop petit."],
    "online_neg": ["Commande en ligne annulee sans explication.","Click and collect: produit pas pret a l'heure.","Site lent et peu intuitif."],
    "fidelite_pos": ["Carte fidelite avantageuse. 20% de reduction. Merci Sophie Martin !","Programme fidelite top. Points cumules vite."],
    "sav_neg": ["SAV desastreux. Aucun suivi depuis 2 semaines. Appelez-moi au 06 12 34 56 78.","Retour complique. Procedure dissuasive.","SAV injoignable par telephone."],
    "personnel_neg": ["Vendeur desagreable et condescendant.","Personnel peu aimable. Pas un sourire."],
    "ski": ["Location ski parfaite. Materiel recent.","Rayon ski au top. Bon conseil sur les batons."],
    "velo": ["Nakamura e-Summit achete, ravi. Montage soigne.","Tres bon conseil VTT. Technicien passionne."],
}

BIAS = {"Intersport Rivoli":-0.3,"Intersport Republique":0.1,"Intersport Montparnasse":-0.1,
    "Intersport Italie 2":0.0,"Intersport Creteil Soleil":0.2,"Intersport Velizy 2":0.3,
    "Intersport Rosny 2":-0.2,"Intersport Parly 2":0.1,"Intersport Grand Place":0.4,
    "Intersport Caserne de Bonne":0.3,"Intersport Part-Dieu":0.1,"Intersport Confluence":0.2,
    "Intersport Annecy Courier":0.5,"Intersport Chamnord":0.3,"Intersport Valentine":-0.4,
    "Intersport Grand Littoral":-0.3,"Intersport Nice TNL":-0.1,"Intersport Alma":0.4,
    "Intersport Atlantis":0.2,"Intersport Englos":-0.2}

FNAMES = ["Marie","Thomas","Sophie","Pierre","Julie","Nicolas","Camille","Francois","Emilie","Antoine",
          "Claire","Julien","Isabelle","Maxime","Nathalie","Alexandre","Celine","Vincent","Sandrine"]
LNAMES = ["Martin","Bernard","Dubois","Thomas","Robert","Richard","Petit","Durand","Leroy","Moreau","Simon"]

def generate_simulated(per_store=25):
    random.seed(42)
    revs = []
    cnt = 0
    for idx, st in enumerate(STORES):
        bias = BIAS.get(st["n"], 0.0)
        zone = classify_zone(st["lat"], st["lng"], st["r"])
        n = random.randint(max(15, per_store-8), per_store+8)
        for _ in range(n):
            cnt += 1
            r = random.random() + bias*0.3
            if r > 0.55:
                pool = random.choice(["accueil_pos","conseil_pos","reparation_pos","prix_pos","choix_pos","magasin_pos","fidelite_pos","ski","velo"])
                rating = random.choice([4,4,5,5,5])
            elif r > 0.25:
                pool = random.choice(["reparation_neg","attente_neg","prix_neg","choix_neg","magasin_neg","online_neg","sav_neg","personnel_neg"])
                rating = random.choice([1,1,2,2,3])
            else:
                pool = random.choice(list(TEMPLATES.keys()))
                rating = random.choice([2,3,3,4])
            text = random.choice(TEMPLATES[pool])
            date = datetime(2023,1,1) + timedelta(days=random.randint(0,760))
            revs.append({"review_id":f"REV-{cnt:05d}","review_text":text,"rating":rating,
                "date":date.strftime("%Y-%m-%d"),"reviewer_name":f"{random.choice(FNAMES)} {random.choice(LNAMES)}",
                "store_id":f"S{idx:02d}","store_name":st["n"],"address":st["a"],"city":st["c"],
                "zone":zone,"region":st["r"],"latitude":st["lat"],"longitude":st["lng"],"source":"simulated"})
    return dedup(revs)

def main():
    print("="*60)
    print("STEP 1: Data Collection")
    print("="*60)
    okey = os.environ.get("OUTSCRAPER_API_KEY","")
    use_selenium = os.environ.get("USE_SELENIUM","").lower() in ("1","true","yes")
    reviews = []

    # Mode 1: Selenium scraper (no API key needed)
    if use_selenium:
        print("[MODE] Selenium + BeautifulSoup (scraping Google Maps)")
        try:
            from scrape_google_maps import GoogleMapsScraper
            scraper = GoogleMapsScraper(headless=True)
            cities = ["Paris","Creteil","Velizy-Villacoublay","Grenoble","Lyon",
                      "Marseille","Bordeaux","Lille","Toulouse","Nice"]
            counter = 0
            try:
                for city in cities:
                    print(f"\n--- {city} ---")
                    city_revs = scraper.scrape_city(city, max_reviews_per_store=50)
                    for r in city_revs:
                        counter += 1
                        r["review_id"] = f"REV-{counter:05d}"
                    reviews.extend(city_revs)
            finally:
                scraper.close()
        except ImportError as e:
            print(f"Selenium not available: {e}")
            print("Install: pip install selenium webdriver-manager")

    # Mode 2: Outscraper API
    elif okey and HAS_REQUESTS:
        print("[MODE] Outscraper API")
        sc = OutscraperScraper(okey)
        for area in ["Paris France","Lyon France","Grenoble France","Marseille France","Bordeaux France","Lille France"]:
            print(f"\n--- {area} ---")
            reviews.extend(sc.scrape_area(area, per_store=50))
    if not reviews:
        print("[MODE] Simulated (set OUTSCRAPER_API_KEY for real data)")
        reviews = generate_simulated(25)
    fp = os.path.join(OUTPUT_DIR, "reviews_raw.csv")
    fields = reviews[0].keys()
    with open(fp,"w",newline="",encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(reviews)
    cities = {}
    for r in reviews:
        c = r.get("city","")
        s = r.get("store_name","")
        if c not in cities: cities[c] = set()
        cities[c].add(s)
    print(f"\n{len(reviews)} reviews, {sum(len(v) for v in cities.values())} stores, {len(cities)} cities")
    for c, stores in sorted(cities.items()):
        if len(stores) > 1:
            print(f"  {c}: {len(stores)} stores ({', '.join(stores)})")
    zones = {}
    for r in reviews:
        z = r.get("zone","")
        zones[z] = zones.get(z,0)+1
    for z, n in zones.items():
        print(f"  {z}: {n} avis")
    print(f"Saved: {fp}")

if __name__ == "__main__":
    main()
