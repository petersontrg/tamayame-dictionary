import pandas as pd
from sqlalchemy import create_engine
import unicodedata

# Normalize apostrophes
def normalize(text):
    if pd.isna(text):
        return None
    return unicodedata.normalize("NFC", str(text)).replace("'", "ʼ").strip()

# Load Excel
df = pd.read_excel("Davis_simplified.xlsx")

# Construct ur_gloss using Leipzig glossing conventions
def format_gloss(row):
    gloss = normalize(row['davis_gloss'])
    role = normalize(row['role'])
    return f"{gloss}.{role}" if role else gloss

df['form'] = df['affix'].apply(normalize)
df['ur_gloss'] = df.apply(format_gloss, axis=1)
df['davis_id'] = df['Davis'].apply(normalize)

# Keep only necessary columns and drop rows with empty forms
upload_df = df[['form', 'ur_gloss', 'davis_id']].dropna(subset=['form'])

# Connect to PostgreSQL
engine = create_engine("postgresql+psycopg2://trpete13@localhost:5432/postgres")

# Upload
upload_df.to_sql("allomorphs", engine, schema="tamayame_dictionary", if_exists="append", index=False)

print(f"✅ Inserted {len(upload_df)} allomorphs into the database.")
