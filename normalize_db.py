import psycopg2
from db import get_connection, normalize_morpheme

def normalize_all_tables():
    conn = get_connection()
    cur = conn.cursor()

    # Normalize entries
    cur.execute("SELECT entry_id, headword, morpheme_break FROM tamayame_dictionary.entries")
    for entry_id, headword, morph_break in cur.fetchall():
        new_hw = normalize_morpheme(headword)
        new_mb = normalize_morpheme(morph_break)
        cur.execute("""
            UPDATE tamayame_dictionary.entries
            SET headword = %s, morpheme_break = %s
            WHERE entry_id = %s
        """, (new_hw, new_mb, entry_id))

    # Normalize morphemes
    cur.execute("SELECT entry_id, segment, gloss FROM tamayame_dictionary.morphemes")
    for entry_id, segment, gloss in cur.fetchall():
        new_seg = normalize_morpheme(segment)
        new_gloss = normalize_morpheme(gloss)
        cur.execute("""
            UPDATE tamayame_dictionary.morphemes
            SET segment = %s, gloss = %s
            WHERE entry_id = %s AND segment = %s AND gloss = %s
        """, (new_seg, new_gloss, entry_id, segment, gloss))

    # Normalize examples
    cur.execute("SELECT entry_id, tamayame_text, gloss_text FROM tamayame_dictionary.examples")
    for entry_id, tam, gloss in cur.fetchall():
        new_tam = normalize_morpheme(tam)
        new_gloss = normalize_morpheme(gloss)
        cur.execute("""
            UPDATE tamayame_dictionary.examples
            SET tamayame_text = %s, gloss_text = %s
            WHERE entry_id = %s AND tamayame_text = %s AND gloss_text = %s
        """, (new_tam, new_gloss, entry_id, tam, gloss))

    conn.commit()
    cur.close()
    conn.close()
    print("Normalization complete.")

if __name__ == "__main__":
    normalize_all_tables()
