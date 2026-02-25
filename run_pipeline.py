"""
================================================================================
RUN PIPELINE — Execute all 3 steps
================================================================================
python run_pipeline.py
================================================================================
"""

import time
import importlib


def run():
    print("╔══════════════════════════════════════════════════════╗")
    print("║  ALADAIA GEM PROJECT — Intersport Analysis Pipeline ║")
    print("║  v2 — Store-level · spaCy · NLP Quality · Chatbot   ║")
    print("╚══════════════════════════════════════════════════════╝\n")

    total_start = time.time()

    # Step 1
    t = time.time()
    print("▶ STEP 1: Data Collection (per store, with GPS)")
    import importlib
    step1 = importlib.import_module("01_collect_data")
    step1.main()
    print(f"  ⏱ {time.time()-t:.1f}s\n")

    # Step 2
    t = time.time()
    print("▶ STEP 2: spaCy Anonymization")
    step2 = importlib.import_module("02_anonymize_spacy")
    step2.main()
    print(f"  ⏱ {time.time()-t:.1f}s\n")

    # Step 3
    t = time.time()
    print("▶ STEP 3: Analysis + NLP Quality Checks")
    step3 = importlib.import_module("03_analyze")
    step3.main()
    print(f"  ⏱ {time.time()-t:.1f}s\n")

    total = time.time() - total_start
    print("═" * 55)
    print(f"✅ Pipeline complete in {total:.1f}s")
    print(f"   Launch chatbot: streamlit run chatbot.py")
    print("═" * 55)


if __name__ == "__main__":
    run()
