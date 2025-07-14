# db.py — Updated with fetch_all_allomorphs
import psycopg2
import psycopg2.extras
import unicodedata
import logging

# Logging config
logging.basicConfig(filename='duplicate_attempts.log', level=logging.INFO)

def normalize_morpheme(s):
    if not s:
        return ''
    s = unicodedata.normalize('NFC', s)
    s = s.replace("'", "ʼ")  # Convert straight apostrophe to modifier letter apostrophe
    return s.strip()

def get_connection():
    return psycopg2.connect(
        dbname="postgres",
        user="trpete13",
        host="localhost",
        port="5432",
        cursor_factory=psycopg2.extras.DictCursor
    )

### --- ENTRIES ---

def fetch_entry(entry_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM tamayame_dictionary.entries
        WHERE entry_id = %s
    """, (entry_id,))
    row = cur.fetchone()
    if not row:
        return None, [], [], [], None
    entry = dict(row)

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
    if entry['template_id']:
        cur.execute("""
            SELECT * FROM tamayame_dictionary.templates
            WHERE template_id = %s
        """, (entry['template_id'],))
        t = cur.fetchone()
        if t:
            template = dict(t)

    cur.close()
    conn.close()
    return entry, morphemes, examples, allomorphs, template

def fetch_entry_summaries(search=None, entry_type=None, pos=None, status=None, startswith=None):
    conn = get_connection()
    cur = conn.cursor()

    query = """
        SELECT * FROM tamayame_dictionary.entry_summary
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

### --- MORPHEMES ---

def insert_morpheme(entry_id, segment, gloss, position, ordering):
    conn = get_connection()
    cur = conn.cursor()

    segment = normalize_morpheme(segment)
    gloss = normalize_morpheme(gloss)

    # Automatically assign category from position
    position_to_category = {
        'prefix': 'affix',
        'suffix': 'affix',
        'root': 'root',
        'proclitic': 'clitic',
        'enclitic': 'clitic',
        'stem': 'stem',
        'particle': 'particle'
    }
    category = position_to_category.get(position, 'unknown')

    cur.execute("""
        INSERT INTO tamayame_dictionary.morphemes
        (entry_id, segment, gloss, position, ordering, category)
        VALUES (%s, %s, %s, %s, %s, %s)
    """, (entry_id, segment, gloss, position, ordering, category))

    conn.commit()
    cur.close()
    conn.close()

def fetch_morpheme_index():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            m.segment,
            m.gloss,
            m.category,
            ARRAY_AGG(DISTINCT e.headword) AS headwords,
            CASE 
                WHEN ent.entry_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS promoted,
            ent.entry_id,
            m.position
        FROM tamayame_dictionary.morphemes m
        LEFT JOIN tamayame_dictionary.entries e ON m.segment = ANY(string_to_array(e.morpheme_break, '-'))
        LEFT JOIN tamayame_dictionary.entries ent ON ent.headword = m.segment
        GROUP BY m.segment, m.gloss, m.category, ent.entry_id, m.position
        ORDER BY m.segment
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def fetch_promotable_morphemes():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            m.segment,
            m.gloss,
            m.category,
            ARRAY_AGG(DISTINCT e.headword) AS used_in,
            CASE 
                WHEN ent.entry_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS promoted,
            ent.entry_id,
            m.position
        FROM tamayame_dictionary.morphemes m
        LEFT JOIN tamayame_dictionary.entries e ON m.segment = ANY(string_to_array(e.morpheme_break, '-'))
        LEFT JOIN tamayame_dictionary.entries ent ON ent.headword = m.segment
        WHERE m.category IN ('root', 'affix')
        GROUP BY m.segment, m.gloss, m.category, ent.entry_id, m.position
        ORDER BY m.segment
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

### --- EXAMPLES ---

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

### --- ALLOMORPHS ---

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

def fetch_all_allomorphs():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            a.form, 
            a.ur_gloss, 
            a.davis_id, 
            e.entry_id, 
            e.headword, 
            e.type,
            e.affix_position
        FROM tamayame_dictionary.allomorphs a
        LEFT JOIN tamayame_dictionary.entries e
          ON TRIM(BOTH FROM a.ur_gloss) ILIKE TRIM(BOTH FROM e.gloss_en)
        ORDER BY a.davis_id NULLS LAST
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

### --- TEMPLATES ---

def fetch_all_templates():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tamayame_dictionary.templates ORDER BY name")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def fetch_template(template_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM tamayame_dictionary.templates WHERE template_id = %s", (template_id,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return dict(row) if row else None

### --- RELATIONAL ---

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
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def fetch_entries_with_template(template_id):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT entry_id, headword, type, pos, gloss_en, translation_en, affix_position
        FROM tamayame_dictionary.entries
        WHERE template_id = %s
        ORDER BY headword ASC
    """, (template_id,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows