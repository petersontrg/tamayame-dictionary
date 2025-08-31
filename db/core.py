# db/core.py
import os
import unicodedata
import psycopg2

# If you’re using a .env file, you can enable these:
# from dotenv import load_dotenv
# load_dotenv()

DEFAULT_DBNAME  = os.getenv("PGDATABASE", "postgres")  # ← default DB
DEFAULT_USER    = os.getenv("PGUSER", "postgres")
DEFAULT_PASS    = os.getenv("PGPASSWORD", "")
DEFAULT_HOST    = os.getenv("PGHOST", "localhost")
DEFAULT_PORT    = int(os.getenv("PGPORT", "5432"))

# Schema we want first on the search_path
DEFAULT_SCHEMA  = os.getenv("TAMAYAME_SCHEMA", "tamayame_dictionary")

def get_connection(
    dbname: str | None = None,
    user: str | None = None,
    password: str | None = None,
    host: str | None = None,
    port: int | None = None,
    schema: str | None = None,
):
    """
    Open a psycopg2 connection and set the search_path so unqualified
    table names resolve to our project schema first.
    """
    conn = psycopg2.connect(
        dbname   = dbname   or DEFAULT_DBNAME,
        user     = user     or DEFAULT_USER,
        password = password or DEFAULT_PASS,
        host     = host     or DEFAULT_HOST,
        port     = port     or DEFAULT_PORT,
    )
    # Ensure our schema is first on the path (but keep public as fallback)
    try:
        with conn.cursor() as cur:
            cur.execute("SET search_path TO %s, public;", (schema or DEFAULT_SCHEMA,))
        conn.commit()
    except Exception:
        # If SET fails (e.g., schema doesn’t exist), don’t block connecting.
        conn.rollback()
    return conn


def normalize_morpheme(s: str | None) -> str:
    """
    Normalize a morpheme string for consistent storage/compare:
      - NFC normalize
      - Replace straight apostrophe ' with modifier letter apostrophe U+02BC (ʼ)
      - Trim surrounding whitespace
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFC", s)
    s = s.replace("'", "ʼ")
    return s.strip()