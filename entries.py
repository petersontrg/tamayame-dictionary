from db import get_connection, normalize_morpheme
import logging
from psycopg2 import IntegrityError

# Log duplicate insert attempts
logging.basicConfig(filename='duplicate_attempts.log', level=logging.INFO)

def insert_entry(headword, entry_type, morpheme_break, pos,
                 gloss_en, translation_en, definition_tamayame,
                 notes, source, status,
                 bound_status='unknown', affix_position='unknown',
                 template_id=None):
    conn = get_connection()
    cur = conn.cursor()

    # Normalize apostrophes and preserve accents
    headword = normalize_morpheme(headword)
    morpheme_break = normalize_morpheme(morpheme_break)
    gloss_en = normalize_morpheme(gloss_en)
    translation_en = normalize_morpheme(translation_en)
    definition_tamayame = normalize_morpheme(definition_tamayame)
    notes = normalize_morpheme(notes)
    source = normalize_morpheme(source)
    # Check for existing entry before insert
    cur.execute("""
        SELECT * FROM tamayame_dictionary.entries
        WHERE headword = %s AND type = %s
    """, (headword, entry_type))
    existing = cur.fetchone()
    if existing:
        logging.info(f"Pre-check found entry: headword='{headword}', type='{entry_type}'")
    else:
        logging.info(f"Pre-check: no existing entry for headword='{headword}', type='{entry_type}'")
    try:
        cur.execute("""
            INSERT INTO tamayame_dictionary.entries
            (headword, type, morpheme_break, pos, gloss_en,
             translation_en, definition_tamayame, notes, source, status,
             bound_status, affix_position, template_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING entry_id
        """, (
            headword, entry_type, morpheme_break, pos, gloss_en,
            translation_en, definition_tamayame, notes, source, status,
            bound_status, affix_position, template_id
        ))
        entry_id = cur.fetchone()[0]
        conn.commit()
        return entry_id
    except IntegrityError:
        conn.rollback()
        logging.info(f"Duplicate attempt: headword='{headword}', type='{entry_type}'")
        return None
    finally:
        cur.close()
        conn.close()

def fetch_entry(entry_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT entry_id, headword, type, bound_status, morpheme_break,
               pos, gloss_en, translation_en, definition_tamayame,
               notes, affix_position, template_id
        FROM tamayame_dictionary.entries
        WHERE entry_id = %s
    """, (entry_id,))
    entry = cur.fetchone()

    cur.execute("""
        SELECT segment, gloss, position, ordering
        FROM tamayame_dictionary.morphemes
        WHERE entry_id = %s
        ORDER BY ordering
    """, (entry_id,))
    morphemes = cur.fetchall()

    cur.execute("""
        SELECT tamayame_text, gloss_text, translation_en
        FROM tamayame_dictionary.examples
        WHERE entry_id = %s
    """, (entry_id,))
    examples = cur.fetchall()

    cur.execute("""
        SELECT form, category, davis_id
        FROM tamayame_dictionary.allomorphs
        WHERE entry_id = %s
        ORDER BY category, form
    """, (entry_id,))
    allomorphs = cur.fetchall()

    cur.execute("""
        SELECT template_id, name, description, template_type, slots
        FROM tamayame_dictionary.templates
        WHERE template_id = %s
    """, (entry[11],))
    template = cur.fetchone()

    cur.close()
    conn.close()
    return entry, morphemes, examples, allomorphs, template

def fetch_entries(search_term=None, entry_type=None, pos=None, status=None, startswith=None):
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT DISTINCT e.entry_id, e.headword, e.type, e.pos, e.gloss_en, e.translation_en
        FROM tamayame_dictionary.entries e
        LEFT JOIN tamayame_dictionary.morphemes m ON e.entry_id = m.entry_id
        WHERE 1=1
    """
    params = []

    if search_term:
        query += " AND (e.headword ILIKE %s OR e.gloss_en ILIKE %s OR e.translation_en ILIKE %s OR m.segment ILIKE %s OR m.gloss ILIKE %s)"
        like = f"%{search_term}%"
        params.extend([like, like, like, like, like])

    if entry_type:
        query += " AND e.type = %s"
        params.append(entry_type)
    if pos:
        query += " AND e.pos = %s"
        params.append(pos)
    if status:
        query += " AND e.status = %s"
        params.append(status)
    if startswith:
        query += " AND e.headword ILIKE %s"
        params.append(f"{startswith}%")

    query += " ORDER BY e.headword ASC"
    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results