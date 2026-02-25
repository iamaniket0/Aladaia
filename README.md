# 🔮 Aladaia — Gem Project: Intersport Verbatim Analysis

> **Trajectory T2** — Simulated data + clear limits  
> Reliable algorithm. No pretty dashboard. Just a chatbot.

---

## What This Is

A verbatim analysis pipeline for Intersport customer reviews that prioritizes **algorithmic reliability** over visual polish. Data is analyzed at the **store level** (not city), anonymized with **spaCy NER** (names replaced, not suppressed), and served through a **conversational chatbot** — not a dashboard.

---

## Key Design Decisions

| Feedback | Implementation |
|----------|----------------|
| Reliable ALGORITHM, not pretty dashboard | Chatbot interface, data-driven responses |
| Add quality check to NLP | Sentiment-rating agreement rate: **94.9%** |
| Add store latitudes and longitude | 27 stores with GPS coordinates |
| Not suppress names — replace with spaCy | spaCy `fr_core_news_sm` NER → `[PERSONNE_1]` replacement |
| Data per store not city | `store_id` is primary key, multiple stores per city |
| Tagging plan for the data | Tags generated FROM data frequency, not pre-defined |
| Paris Intra Muros vs Extra Muros | Zone field: Intra Muros / Extra Muros / Province |

---

## Results (641 reviews, 27 stores)

| Metric | Value |
|--------|-------|
| Reviews | 641 across 27 stores |
| spaCy NER redactions | 774 (6 unique persons replaced) |
| NLP agreement rate | **94.9%** (sentiment matches rating) |
| Lexicon coverage | 79.9% |
| Tag coverage | 100% (16 tags from data) |
| Pipeline runtime | 7.2 seconds |
| Paris Intra Muros | 3.40★ (NPS -17) |
| Paris Extra Muros | 3.49★ (NPS -10) |
| Province | 3.42★ (NPS -17) |
| Best store | Intersport Alma, Rennes (4.12★) |
| Worst store | Intersport Grand Littoral, Marseille (2.64★) |

---

## Project Structure

```
gem_project/
├── 01_collect_data.py          # Step 1: Per-store simulated data with GPS
├── 02_anonymize_spacy.py       # Step 2: spaCy NER anonymization
├── 03_analyze.py               # Step 3: Sentiment + tagging + quality checks
├── run_pipeline.py             # Run all 3 steps
├── chatbot.py                  # Streamlit chatbot interface
├── requirements.txt
├── .gitignore
├── data/
│   ├── raw/reviews_raw.csv
│   ├── anonymized/reviews_anonymized.csv
│   ├── analysis/
│   │   ├── reviews_tagged.csv
│   │   ├── sentiment_quality.json     # NLP quality metrics
│   │   ├── tagging_plan.json          # Data-driven tagging plan
│   │   ├── store_stats.json           # Per-store analysis
│   │   └── zone_comparison.json       # Intra vs Extra Muros
│   └── audit/
│       └── anonymization_audit.json   # spaCy redaction log
└── deliverables/                      # D1, D2, D3 PDFs
```

---

## Quick Start

```bash
git clone git@github.com:iamaniket0/Aladaia.git
cd Aladaia
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m spacy download fr_core_news_sm

# Run pipeline
python run_pipeline.py

# Launch chatbot
streamlit run chatbot.py
```

---

## Chatbot Commands

| Query | What it returns |
|-------|----------------|
| `résumé` | Global overview: avg rating, NPS, best/worst store |
| `Paris intra vs extra muros` | Zone comparison with NPS |
| `meilleurs magasins` | Top 5 stores by rating |
| `pires magasins` | Bottom 5 stores by rating |
| `réparation` / `conseil` / `prix` / `ski` | Tag deep-dive with affected stores |
| `quality check` | NLP quality metrics: agreement, coverage, grade |
| `plan de tags` | Full tagging plan with coverage stats |
| `Intersport Rivoli` | Specific store stats |
| `Auvergne-Rhône-Alpes` | Region breakdown |
| `verbatim réparation` | Sample reviews matching topic |
| `aide` | Full help menu |

---

## NLP Quality Checks

The pipeline validates its own output:

- **Agreement rate** (94.9%): Does the sentiment label match the star rating? (4-5★ → positive, 1-2★ → negative)
- **Lexicon coverage** (79.9%): What % of reviews had at least one sentiment word detected?
- **Neutral rate** (10.3%): How many reviews couldn't be classified? (lower = better)
- **Quality grade**: GOOD (>80% agreement), NEEDS_REVIEW (60-80%), POOR (<60%)

---

## spaCy Anonymization

Names are **replaced**, not deleted:

```
Input:  "Merci Sophie Martin pour le conseil !"
Output: "Merci [PERSONNE_1] pour le conseil !"
```

Method: `spacy.load("fr_core_news_sm")` → NER label `PER` → consistent pseudonym mapping. Also redacts emails (`[EMAIL]`), phones (`[TELEPHONE]`), URLs (`[URL]`).

---

## Tech Stack

- **Python 3.9+** · **spaCy** (French NER) · **Streamlit** (chatbot UI)
- **pandas** (data) · **BeautifulSoup** (scraping, not used in T2)
- Zero LLM dependency for core pipeline

---

## License

Proprietary — Aladaia. For demonstration purposes only.
