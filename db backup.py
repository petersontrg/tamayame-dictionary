import psycopg2
from psycopg2 import IntegrityError
import unicodedata
import logging
import json

# Logging for duplicate insert attempts
logging.basicConfig(filename='duplicate_attempts.log', level=logging.INFO)

def normalize_morpheme(s):
    if not s:
        return ''
    s = unicodedata.normalize('NFC', s)
    s = s.replace("'", "'")
    return s.strip()

def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="trpete13",
        host="localhost",
        port="5432"
    )

def fetch_entry(entry_id):
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT entry_id, headword, type, bound_status, morpheme_break, pos,
               gloss_en, translation_en, definition_tamayame, notes, template_id
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

    template = None
    if entry[10]:
        cur.execute("""
            SELECT template_id, name, description, template_type, slots
            FROM tamayame_dictionary.templates
            WHERE template_id = %s
        """, (entry[10],))
        result = cur.fetchone()
        if result:
            template = {
                'template_id': result[0],
                'name': result[1],
                'description': result[2],
                'template_type': result[3],
                'slots': result[4]
            }

    cur.close()
    conn.close()
    return entry, morphemes, examples, allomorphs, template

def fetch_entry_summaries(search=None, entry_type=None, pos=None, status=None, startswith=None):
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT entry_id, headword, type, pos, gloss_en, translation_en
        FROM tamayame_dictionary.entry_summary
        WHERE 1=1
    """
    params = []

    if search:
        query += """
            AND (headword ILIKE %s OR gloss_en ILIKE %s OR translation_en ILIKE %s)
        """
        like = f"%{search}%"
        params.extend([like, like, like])

    if entry_type:
        query += " AND type = %s"
        params.append(entry_type)

    if pos:
        query += " AND pos = %s"
        params.append(pos)

    if status:
        query += " AND status = %s"
        params.append(status)

    if startswith:
        query += " AND headword ILIKE %s"
        params.append(f"{startswith}%")

    query += " ORDER BY headword ASC"

    cur.execute(query, params)
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def refresh_entry_summary_view():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("REFRESH MATERIALIZED VIEW tamayame_dictionary.entry_summary")
    conn.commit()
    cur.close()
    conn.close()

def insert_morpheme(entry_id, segment, gloss, position, ordering):
    conn = get_connection()
    cur = conn.cursor()

    segment = normalize_morpheme(segment)
    gloss = normalize_morpheme(gloss)

    position_to_category = {
        "prefix": "affix",
        "suffix": "affix",
        "root": "root",
        "proclitic": "clitic",
        "enclitic": "clitic",
        "stem": "stem",
        "particle": "particle",
        "unknown": "unknown"
    }
    category = position_to_category.get(position, "unknown")

    cur.execute("""
        INSERT INTO tamayame_dictionary.morphemes
        (entry_id, segment, gloss, position, ordering, category)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (entry_id, segment, gloss, position, ordering, category))

    conn.commit()
    cur.close()
    conn.close()

def insert_example(entry_id, tamayame_text, gloss_text, translation_en):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tamayame_dictionary.examples
        (entry_id, tamayame_text, gloss_text, translation_en)
        VALUES (%s, %s, %s, %s)
    """, (entry_id, tamayame_text, gloss_text, translation_en))
    conn.commit()
    cur.close()
    conn.close()

def insert_allomorph(entry_id, form, category, davis_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tamayame_dictionary.allomorphs
        (entry_id, form, category, davis_id)
        VALUES (%s, %s, %s, %s)
    """, (entry_id, form, category, davis_id))
    conn.commit()
    cur.close()
    conn.close()

def fetch_related_entries_by_segment(segment):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT e.entry_id, e.headword, e.type, e.pos, e.gloss_en, e.translation_en
        FROM tamayame_dictionary.entries e
        JOIN tamayame_dictionary.morphemes m ON e.entry_id = m.entry_id
        WHERE m.segment = %s
        ORDER BY e.headword ASC
    """, (segment,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def fetch_examples_for_morpheme(segment):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT e.headword, ex.tamayame_text, ex.gloss_text, ex.translation_en
        FROM tamayame_dictionary.entries e
        JOIN tamayame_dictionary.morphemes m ON e.entry_id = m.entry_id
        JOIN tamayame_dictionary.examples ex ON e.entry_id = ex.entry_id
        WHERE m.segment = %s
        ORDER BY e.headword ASC
    """, (segment,))
    results = cur.fetchall()
    cur.close()
    conn.close()
    return results

def fetch_all_allomorphs():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT a.form, a.ur_gloss, a.davis_id, e.entry_id, e.headword, e.type
        FROM tamayame_dictionary.allomorphs a
        LEFT JOIN tamayame_dictionary.entries e
          ON TRIM(BOTH FROM a.ur_gloss) ILIKE TRIM(BOTH FROM e.gloss_en)
        ORDER BY a.davis_id ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def fetch_all_templates():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT template_id, name, description, template_type, slots
        FROM tamayame_dictionary.templates
        ORDER BY name ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [{'template_id': r[0], 'name': r[1], 'description': r[2], 'template_type': r[3], 'slots': r[4]} for r in rows]

def fetch_template(template_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT template_id, name, description, template_type, slots
        FROM tamayame_dictionary.templates
        WHERE template_id = %s
    """, (template_id,))
    r = cur.fetchone()
    cur.close()
    conn.close()
    return {'template_id': r[0], 'name': r[1], 'description': r[2], 'template_type': r[3], 'slots': r[4]} if r else None

def insert_template(name, description, template_type, slots):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO tamayame_dictionary.templates
        (name, description, template_type, slots)
        VALUES (%s, %s, %s, %s)
    """, (name, description, template_type, slots))
    conn.commit()
    cur.close()
    conn.close()

def update_template(template_id, name, description, template_type, slots):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tamayame_dictionary.templates
        SET name = %s, description = %s, template_type = %s, slots = %s
        WHERE template_id = %s
    """, (name, description, template_type, slots, template_id))
    conn.commit()
    cur.close()
    conn.close()

def delete_template(template_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("DELETE FROM tamayame_dictionary.templates WHERE template_id = %s", (template_id,))
    conn.commit()
    cur.close()
    conn.close()

def fetch_entries_with_template(template_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT entry_id, headword, type, pos, gloss_en, translation_en
        FROM tamayame_dictionary.entries
        WHERE template_id = %s
        ORDER BY headword ASC
    """, (template_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
