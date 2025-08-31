# one_off_insert.py

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

headword = unicodedata.normalize('NFC', "sukʼutsa")

conn = get_connection()
cur = conn.cursor()

try:
    cur.execute("""
        INSERT INTO tamayame_dictionary.entries
        (headword, type, morpheme_break, pos, gloss_en,
         translation_en, definition_tamayame, notes, source, status,
         bound_status, affix_position, template_id, voice_class)
        VALUES (%s, %s, '', '', '', '', '', '', '', 'draft',
                'bound', 'unknown', NULL, NULL)
        RETURNING entry_id
    """, (headword, 'root'))

    entry_id = cur.fetchone()[0]
    conn.commit()
    print(f"✅ Inserted: entry_id = {entry_id}")

except psycopg2.IntegrityError as e:
    conn.rollback()
    print("🚫 IntegrityError:", e)

except Exception as e:
    print("🚨 General Error:", e)

finally:
    cur.close()
    conn.close()