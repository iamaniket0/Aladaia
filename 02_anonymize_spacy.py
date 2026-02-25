"""
================================================================================
STEP 2: Anonymization with spaCy NER
================================================================================
Uses spaCy French NER model to DETECT person names in text and REPLACE them
with pseudonyms (not suppress). This preserves readability while ensuring RGPD.

Key change from v1: Names are REPLACED with [PERSONNE_1], [PERSONNE_2] etc.
— not deleted. This keeps the review readable and analyzable.
================================================================================
"""

import spacy
import csv
import os
import re
import json
import hashlib
from datetime import datetime
from typing import List, Dict, Tuple

INPUT_DIR = "data/raw"
OUTPUT_DIR = "data/anonymized"
AUDIT_DIR = "data/audit"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(AUDIT_DIR, exist_ok=True)


class SpacyAnonymizer:
    """
    spaCy-based anonymization:
    - NER to detect PER (person names) in review text → replace with [PERSONNE_N]
    - Regex for emails, phones, addresses
    - SHA-256 hash for reviewer_name field
    """

    def __init__(self):
        self.nlp = spacy.load("fr_core_news_sm")
        self.salt = "aladaia_gem_2026"
        self.audit = []
        self.name_map = {}  # consistent pseudonyms across reviews

        self.email_re = re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b')
        self.phone_re = re.compile(r'\b(?:(?:\+33|0033|0)\s*[1-9](?:[\s.-]*\d{2}){4})\b')
        self.url_re = re.compile(r'https?://\S+|www\.\S+')

    def _hash(self, value: str) -> str:
        return hashlib.sha256(f"{self.salt}:{value.lower().strip()}".encode()).hexdigest()[:12]

    def _get_pseudonym(self, real_name: str) -> str:
        """Get consistent pseudonym for a name."""
        key = real_name.lower().strip()
        if key not in self.name_map:
            idx = len(self.name_map) + 1
            self.name_map[key] = f"[PERSONNE_{idx}]"
        return self.name_map[key]

    def anonymize_text(self, text: str, review_id: str) -> Tuple[str, List[Dict]]:
        """
        Use spaCy NER to find person names and REPLACE them.
        Also redact emails, phones, URLs.
        """
        if not text:
            return text, []

        redactions = []
        result = text

        # 1. spaCy NER — find PER entities and replace
        doc = self.nlp(result)
        # Process entities in reverse order to preserve character positions
        persons = [(ent.start_char, ent.end_char, ent.text) for ent in doc.ents if ent.label_ == "PER"]
        for start, end, name in sorted(persons, reverse=True):
            pseudo = self._get_pseudonym(name)
            result = result[:start] + pseudo + result[end:]
            redactions.append({"type": "PER_spacy", "replaced_with": pseudo, "review_id": review_id})

        # 2. Emails
        for m in self.email_re.finditer(result):
            redactions.append({"type": "email", "review_id": review_id})
        result = self.email_re.sub("[EMAIL]", result)

        # 3. Phones
        for m in self.phone_re.finditer(result):
            redactions.append({"type": "phone", "review_id": review_id})
        result = self.phone_re.sub("[TELEPHONE]", result)

        # 4. URLs
        result = self.url_re.sub("[URL]", result)

        return result, redactions

    def anonymize_review(self, review: Dict) -> Dict:
        anon = review.copy()
        rid = review.get("review_id", "")

        # Anonymize reviewer name field → hash
        name = review.get("reviewer_name", "")
        if name and name.lower() not in ("anonymous", "anonyme", ""):
            anon["reviewer_id"] = f"R_{self._hash(name)}"
            anon["reviewer_name"] = "[ANONYMIZED]"
            self.audit.append({"type": "name_field_hashed", "review_id": rid})
        else:
            anon["reviewer_id"] = "ANONYMOUS"

        # Anonymize text content with spaCy
        text = review.get("review_text", "")
        anon_text, redactions = self.anonymize_text(text, rid)
        anon["review_text"] = anon_text
        self.audit.extend(redactions)

        return anon

    def anonymize_batch(self, reviews: List[Dict]) -> List[Dict]:
        print(f"[ANON] Processing {len(reviews)} reviews with spaCy NER...")
        result = []
        for i, r in enumerate(reviews):
            result.append(self.anonymize_review(r))
            if (i + 1) % 100 == 0:
                print(f"  {i+1}/{len(reviews)}...")
        print(f"[ANON] Done. {len(self.audit)} redactions, {len(self.name_map)} unique persons detected.")
        return result

    def save_audit(self, filepath: str):
        summary = {}
        for a in self.audit:
            t = a.get("type", "other")
            summary[t] = summary.get(t, 0) + 1

        with open(filepath, "w", encoding="utf-8") as f:
            json.dump({
                "date": datetime.now().isoformat(),
                "method": "spaCy fr_core_news_sm NER + regex",
                "total_redactions": len(self.audit),
                "unique_persons_detected": len(self.name_map),
                "summary": summary,
                "details": self.audit,
            }, f, indent=2, ensure_ascii=False)
        print(f"[AUDIT] {filepath}")


def main():
    print("=" * 60)
    print("STEP 2: spaCy Anonymization")
    print("=" * 60)

    # Load
    raw_path = os.path.join(INPUT_DIR, "reviews_raw.csv")
    reviews = []
    with open(raw_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            reviews.append(dict(row))
    print(f"Loaded {len(reviews)} reviews")

    # Anonymize
    anon = SpacyAnonymizer()
    anon_reviews = anon.anonymize_batch(reviews)

    # Save
    out_path = os.path.join(OUTPUT_DIR, "reviews_anonymized.csv")
    fields = anon_reviews[0].keys()
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(anon_reviews)
    print(f"[SAVED] {out_path}")

    anon.save_audit(os.path.join(AUDIT_DIR, "anonymization_audit.json"))

    # Show examples
    print("\n--- Example redactions ---")
    for r in anon_reviews[:5]:
        if "[PERSONNE_" in r["review_text"] or "[EMAIL]" in r["review_text"] or "[TELEPHONE]" in r["review_text"]:
            print(f"  {r['review_id']}: {r['review_text'][:120]}...")


if __name__ == "__main__":
    main()
