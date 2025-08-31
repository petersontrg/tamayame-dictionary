import unicodedata
import psycopg2
from psycopg2.extras import DictCursor

def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="trpete13",
        host="localhost",
        port="5432",
        cursor_factory=DictCursor
    )

def inspect_entries(entry_type="root"):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT entry_id, headword, type
        FROM tamayame_dictionary.entries
        WHERE type = %s
        ORDER BY headword
    """, (entry_type,))
    rows = cur.fetchall()

    print(f"\n🧾 Entries of type '{entry_type}':\n")
    for row in rows:
        norm = unicodedata.normalize('NFC', row['headword'])
        print(f"entry_id={row['entry_id']:<4}  raw={repr(row['headword'])}  norm={repr(norm)}")

    cur.close()
    conn.close()

if __name__ == "__main__":
    inspect_entries("root")