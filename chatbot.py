"""
ALADAIA CHATBOT — Intersport Voice of Customer
Run: streamlit run chatbot.py
"""
import streamlit as st
import pandas as pd
import json
import os

try:
    from anthropic import Anthropic
    HAS_API = True
except ImportError:
    HAS_API = False

st.set_page_config(page_title="Aladaia Chatbot", page_icon="🔮", layout="centered")

# --- Load data ---
@st.cache_data
def load_data():
    base = os.path.dirname(os.path.abspath(__file__))
    d = os.path.join(base, "data", "analysis")
    df = pd.read_csv(os.path.join(d, "reviews_tagged.csv"))
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["tags"] = df["tags"].fillna("").astype(str)
    j = {}
    for name in ["store_stats", "tagging_plan", "zone_comparison", "sentiment_quality"]:
        with open(os.path.join(d, f"{name}.json")) as f:
            j[name] = json.load(f)
    return df, j

df, J = load_data()
SS, TP, ZC, NQ = J["store_stats"], J["tagging_plan"], J["zone_comparison"], J["sentiment_quality"]

# --- Data context ---
def get_data_summary():
    total = len(df)
    avg = df["rating"].mean()
    nps = round(((df["rating"]==5).sum() - (df["rating"]<=3).sum()) / total * 100, 1)
    text = f"DATA: {total} avis, {len(SS)} magasins\n"
    text += f"Note moyenne: {avg:.2f}/5, NPS: {nps:+.0f}\n"
    text += f"Positif: {(df['sentiment']=='positive').mean()*100:.1f}%, Negatif: {(df['sentiment']=='negative').mean()*100:.1f}%\n"
    text += f"\nNLP QUALITY: agreement {NQ['agreement_rate']}%, couverture {NQ['lexicon_coverage']}%, grade {NQ['quality_grade']}\n"
    text += "\nZONES:\n"
    for z, s in ZC.items():
        text += f"  {z}: {s['avg_rating']} etoiles, NPS {s['nps']:+.0f}, n={s['count']}\n"
    text += "\nMAGASINS:\n"
    for sid, s in sorted(SS.items(), key=lambda x: x[1]["avg_rating"]):
        tags = ", ".join(f"{t}({c})" for t, c in list(s["top_tags"].items())[:3])
        text += f"  {s['store_name']} | {s['city']} | {s['zone']} | {s['region']} | {s['avg_rating']} etoiles | NPS {s['nps']:+.0f} | n={s['review_count']} | pos={s['pct_positive']}% neg={s['pct_negative']}% | {tags}\n"
    text += f"\nTAGS ({TP['total_tags']} tags, couverture {TP['quality']['tag_coverage_pct']}%):\n"
    for t, info in sorted(TP["tags"].items(), key=lambda x: -x[1]["count"]):
        text += f"  {t} [{info['category']}]: {info['count']} mentions ({info['pct_reviews']}%), {info['avg_rating']} etoiles, impact={info['rating_vs_overall']:+.2f}\n"
    text += "\nVERBATIMS:\n"
    for tag in list(TP["tags"].keys())[:8]:
        tag_revs = df[df["tags"].str.contains(tag, na=False)]
        if len(tag_revs) == 0: continue
        text += f"  [{tag}]\n"
        for _, r in tag_revs.head(4).iterrows():
            text += f'    {int(r["rating"])} etoiles {r["store_name"]} ({r["city"]}): "{r["review_text"]}"\n'
    return text

DATA = get_data_summary()

SYSTEM = f"""Tu es l'assistant data Aladaia pour Intersport.

{DATA}

REGLES STRICTES:
1. Reponds UNIQUEMENT a la question posee. Rien de plus. Pas de comparaisons non demandees.
   Exemple: si on demande "combien de magasins a Paris", reponds SEULEMENT le nombre de magasins a Paris. Ne parle PAS de Marseille ou d'autres villes.
2. Sois PRECIS: donne les noms exacts, les chiffres exacts du contexte.
3. Cite 1-2 verbatims clients quand c'est pertinent (pas toujours).
4. Reponds en francais.
5. Si la question est simple (oui/non, un chiffre), donne une reponse courte.
6. Si la question est complexe (analyse, comparaison), structure ta reponse avec du markdown.
7. Ne repete JAMAIS les donnees completes. Extrais seulement ce qui repond a la question.
8. Utilise le format markdown: **gras** pour les chiffres cles, listes pour les classements."""

# --- Sidebar ---
with st.sidebar:
    st.title("Aladaia 🔮")
    st.caption("Voice of Customer - Intersport")
    st.divider()
    api_key = st.text_input("Cle API Anthropic", type="password",
                            value=os.environ.get("ANTHROPIC_API_KEY", ""))
    if api_key:
        st.success("Claude connecte")
    else:
        st.warning("Entrez votre cle API")
    st.divider()
    st.subheader("Exemples")
    suggestions = [
        "Combien de magasins a Paris ?",
        "Resume global",
        "Compare Paris intra et extra muros",
        "Les 5 pires magasins",
        "Les meilleurs magasins",
        "Analyse le theme reparation",
        "Impact du test apres reparation",
        "Problemes de temps d attente",
        "Top 3 actions prioritaires",
        "Detail Intersport Grand Littoral",
        "Qualite du NLP",
        "Plan de tagging",
    ]
    for s in suggestions:
        if st.button(s, use_container_width=True):
            st.session_state["pending"] = s

# --- KPIs ---
avg = df["rating"].mean()
total = len(df)
nps = round(((df["rating"]==5).sum()-(df["rating"]<=3).sum())/total*100, 1)
c1, c2, c3, c4 = st.columns(4)
c1.metric("Avis", total)
c2.metric("Note moy.", f"{avg:.2f}")
c3.metric("NPS", f"{nps:+.0f}")
c4.metric("Magasins", len(SS))
st.divider()

# --- Chat ---
if "messages" not in st.session_state:
    st.session_state.messages = []

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.write(msg["content"])

pending = st.session_state.pop("pending", None)
user_input = st.chat_input("Posez votre question...")
question = pending or user_input

if question:
    st.session_state.messages.append({"role": "user", "content": question})
    with st.chat_message("user"):
        st.write(question)

    if api_key:
        try:
            import time as _time
            client = Anthropic(api_key=api_key)
            claude_msgs = [{"role": m["role"], "content": m["content"]}
                          for m in st.session_state.messages[-10:]]
            with st.chat_message("assistant"):
                with st.spinner("Analyse en cours..."):
                    # Retry up to 3 times with backoff
                    answer = None
                    models = ["claude-sonnet-4-20250514", "claude-haiku-4-5-20251001"]
                    for model in models:
                        for attempt in range(3):
                            try:
                                resp = client.messages.create(
                                    model=model,
                                    max_tokens=2048,
                                    system=SYSTEM,
                                    messages=claude_msgs,
                                )
                                answer = resp.content[0].text
                                break
                            except Exception as api_err:
                                if "529" in str(api_err) or "overloaded" in str(api_err).lower():
                                    _time.sleep(2 * (attempt + 1))
                                    continue
                                raise
                        if answer:
                            break
                    if not answer:
                        answer = "API surchargee. Reessayez dans quelques secondes."
                    st.write(answer)
                    st.session_state.messages.append({"role": "assistant", "content": answer})
        except Exception as e:
            with st.chat_message("assistant"):
                msg = f"Erreur: {str(e)}"
                st.write(msg)
                st.session_state.messages.append({"role": "assistant", "content": msg})
    else:
        with st.chat_message("assistant"):
            msg = "Entrez votre cle API Anthropic dans la barre laterale."
            st.write(msg)
            st.session_state.messages.append({"role": "assistant", "content": msg})