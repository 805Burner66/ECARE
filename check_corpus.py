import sqlite3
conn = sqlite3.connect("data/raw/rhowardstone/full_text_corpus.db")
tables = conn.execute("SELECT name, type FROM sqlite_master WHERE type IN ('table', 'view') ORDER BY name").fetchall()
for name, t in tables:
    print(f"  {t}: {name}")