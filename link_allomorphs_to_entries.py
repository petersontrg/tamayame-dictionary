import psycopg2
import unicodedata

def normalize(text):
    if not text:
        return ''
    return unicodedata.normalize("NFC", text.strip())

def link_allomorphs():
    conn = psycopg2.connect(
        dbname="postgres",
        user="trpete13",
        host="localhost",
        port="5432"
    )
    cur = conn.cursor()

    # Fetch all allomorphs that are not yet linked
    cur.execute("""
        SELECT allomorph_id, ur_gloss
        FROM tamayame_dictionary.allomorphs
        WHERE entry_id IS NULL
    """)
    allomorphs = cur.fetchall()

    updated = 0
    for allomorph_id, ur_gloss in allomorphs:
        ur_gloss = normalize(ur_gloss)

        # Attempt to find an entry with matching normalized definition_en
        cur.execute("""
            SELECT entry_id FROM tamayame_dictionary.entries
            WHERE normalize(definition_en) = %s
            LIMIT 1
        """, (ur_gloss,))
        match = cur.fetchone()

        if match:
            entry_id = match[0]
            cur.execute("""
                UPDATE tamayame_dictionary.allomorphs
                SET entry_id = %s
                WHERE allomorph_id = %s
            """, (entry_id, allomorph_id))
            updated += 1

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Linked {updated} allomorphs to matching entries.")

if __name__ == "__main__":
    link_allomorphs()
