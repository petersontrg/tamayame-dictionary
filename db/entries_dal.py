# db/entries_dal.py
from .core import get_connection
from .intransitive import intransitive_class_letter, fetch_entry_intransitive_classes
from .lookups import fetch_suffix_subclass_allomorphs
from psycopg2.extras import RealDictCursor


def fetch_entry(entry_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 1) Main entry row, plus names for paradigm‐class & suffix‐subclass
    cur.execute("""
        SELECT 
            e.*,
            ppc.name AS primary_paradigm_class,
            sc.name  AS suffix_subclass
        FROM tamayame_dictionary.entries e
        LEFT JOIN tamayame_dictionary.primary_paradigm_classes ppc
          ON e.primary_paradigm_class_id = ppc.id
        LEFT JOIN tamayame_dictionary.suffix_subclasses sc
          ON e.suffix_subclass_id = sc.id
        WHERE e.entry_id = %s;
    """, (entry_id,))
    row = cur.fetchone()
    if not row:
        cur.close(); conn.close()
        return None, [], [], [], None

    entry = dict(row)

    # Only intransitive entries carry an A/B code; others should be None
    if entry.get('transitivity') == 'intransitive':
        entry['intransitive_class_letter'] = intransitive_class_letter(
            entry.get('intransitive_class_id')
        )
    else:
        entry['intransitive_class_letter'] = None

    # 2) PRMP allomorphs for its class (transitives only)
    if entry.get('primary_paradigm_class_id') and entry.get('transitivity') == 'transitive':
        cur.execute("""
            SELECT DISTINCT a.form, a.ur_gloss, a.davis_id, a.partial_paradigm
            FROM tamayame_dictionary.allomorphs a
            JOIN tamayame_dictionary.primary_paradigm_class_paradigms ppp
              ON a.partial_paradigm = ppp.partial_paradigm
            WHERE ppp.class_id = %s
              AND a.category     = 'PRMP'
              AND a.transitivity = 'transitive'
            ORDER BY a.partial_paradigm, a.davis_id
        """, (entry['primary_paradigm_class_id'],))
        entry['primary_paradigm_allomorphs'] = cur.fetchall()
    else:
        entry['primary_paradigm_allomorphs'] = []

    # 3) Entry‐level morphemes
    cur.execute("""
        SELECT segment, gloss, position, ordering
        FROM tamayame_dictionary.morphemes
        WHERE entry_id = %s
        ORDER BY ordering
    """, (entry_id,))
    morphemes = cur.fetchall()

    # 4) Examples for this entry
    cur.execute("""
        SELECT e.example_id, e.tamayame_text, e.gloss_text, e.translation_en,
               e.speaker_id, e.audio_file, e.notes, e.created_at, e.updated_at,
               e.voice_class_id, e.comment
        FROM tamayame_dictionary.example_entries ee
        JOIN tamayame_dictionary.examples e
          ON ee.example_id = e.example_id
        WHERE ee.entry_id = %s
        ORDER BY e.example_id
    """, (entry_id,))
    examples = cur.fetchall()
    enriched_examples = []

    for ex in examples:
        ex = dict(ex)
        ex_id = ex['example_id']
        ex['morphemes'] = []

        # 4a) linked morphemes (roots, suffixes, etc.)
        cur.execute("""
            SELECT m.segment, m.gloss, m.position, em.ordering
            FROM tamayame_dictionary.example_morphemes em
            JOIN tamayame_dictionary.morphemes m
              ON em.morpheme_id = m.morpheme_id
            WHERE em.example_id = %s
        """, (ex_id,))
        ex['morphemes'].extend(cur.fetchall())

        # 4b) PRMP (prefix) if present — legacy support
        cur.execute("""
            SELECT a.form, a.ur_gloss, a.davis_id, 'prefix' AS position, epa.ordering
            FROM tamayame_dictionary.example_prmp_allomorphs epa
            JOIN tamayame_dictionary.allomorphs a
              ON epa.allomorph_id = a.allomorph_id
            WHERE epa.example_id = %s
        """, (ex_id,))
        prmp_rows = cur.fetchall()
        for i, row2 in enumerate(prmp_rows):
            ex['morphemes'].append({
                'segment':  row2['form'],
                'gloss':    row2['ur_gloss'],
                'position': 'prefix',
                'ordering': row2['ordering'],
            })
            if i == 0:
                ex['prmp']       = row2['form']
                ex['prmp_gloss'] = row2['ur_gloss']
                ex['prmp_davis'] = row2['davis_id']

        # 4c) TA (from example_morphemes.ta_allomorph_id; new storage)
        cur.execute("""
            SELECT ta.form AS segment,
                   ta.number AS gloss,
                   'TA'      AS position,
                   em.ordering AS ordering,
                   ta.voice_class,
                   a.davis_id
            FROM tamayame_dictionary.example_morphemes em
            JOIN tamayame_dictionary.ta_allomorphs ta
              ON em.ta_allomorph_id = ta.ta_id
            LEFT JOIN tamayame_dictionary.allomorphs a
              ON a.category = 'TA' AND a.form = ta.form
            WHERE em.example_id = %s
              AND em.ta_allomorph_id IS NOT NULL
            ORDER BY em.ordering
        """, (ex_id,))
        for row3 in cur.fetchall():
            ex['morphemes'].append({
                'segment':     row3['segment'],
                'gloss':       row3['gloss'],
                'position':    row3['position'],
                'ordering':    row3['ordering'],
                'voice_class': row3.get('voice_class'),
                'davis_id':    row3.get('davis_id'),
            })

        # 4d) Realizations (ur, sr, ipa)
        cur.execute("""
            SELECT ur, sr, ipa
            FROM tamayame_dictionary.example_realizations
            WHERE example_id = %s
        """, (ex_id,))
        realization = cur.fetchone()
        if realization:
            ex['ur']  = realization.get('ur')
            ex['sr']  = realization.get('sr')
            ex['ipa'] = realization.get('ipa')

        # 4e) Which template is linked (if any)
        cur.execute("""
            SELECT t.template_id, t.name
            FROM tamayame_dictionary.example_templates et
            JOIN tamayame_dictionary.templates t
              ON et.template_id = t.template_id
            WHERE et.example_id = %s
            LIMIT 1
        """, (ex_id,))
        tr = cur.fetchone()
        ex['template'] = {'template_id': tr['template_id'], 'name': tr['name']} if tr else None

        # 4f) sort morphemes by ordering
        ex['morphemes'].sort(key=lambda m: m['ordering'])
        enriched_examples.append(ex)

    # 5) Allomorphs *defined* on this entry (for admin/use)
    cur.execute("""
        SELECT form, category, davis_id
        FROM tamayame_dictionary.allomorphs
        WHERE entry_id = %s
        ORDER BY category, form
    """, (entry_id,))
    allomorphs = cur.fetchall()

    # 6) Suffix‐subclass allomorphs
    if entry.get('suffix_subclass_id'):
        entry['suffix_subclass_allomorphs'] = fetch_suffix_subclass_allomorphs(
            entry['suffix_subclass_id']
        )
    else:
        entry['suffix_subclass_allomorphs'] = []

    # 7) Entry‐level template — only for transitive entries
    template = None
    if entry.get('transitivity') == 'transitive' and entry.get('template_id'):
        cur.execute("""
            SELECT * 
            FROM tamayame_dictionary.templates
            WHERE template_id = %s
        """, (int(entry['template_id']),))
        t = cur.fetchone()
        if t:
            template = dict(t)

    # 8) Intransitive A/B flags + per-number mappings (always compute)
    entry['intransitive_classes'] = fetch_entry_intransitive_classes(entry_id)

    cur.close(); conn.close()
    return entry, morphemes, enriched_examples, allomorphs, template


# db/entries_dal.py
from .core import get_connection
from psycopg2.extras import RealDictCursor

def fetch_root_summaries(
    search=None, pos=None, status=None, startswith=None,
    page=1, per_page=100
):
    """
    Paginated list for entries where type='root'.
    Returns: (rows, total_count)
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    wheres = ["e.type = %s"]
    params = ["root"]

    if search:
        wheres.append("(e.headword ILIKE %s OR e.translation_en ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])
    if pos:
        wheres.append("e.pos = %s"); params.append(pos)
    if status:
        wheres.append("e.status = %s"); params.append(status)
    if startswith:
        wheres.append("e.headword ILIKE %s"); params.append(f"{startswith}%")

    where_sql = "WHERE " + " AND ".join(wheres)
    offset = max(0, (int(page or 1) - 1) * int(per_page or 100))
    limit  = int(per_page or 100)

    cur.execute(f"SELECT COUNT(*) AS c FROM tamayame_dictionary.entries e {where_sql}", params)
    total = int(cur.fetchone()["c"])

    cur.execute(f"""
        SELECT
            e.entry_id, e.headword, e.type, e.affix_position, e.ipa, e.pos,
            e.translation_en, e.status, e.transitivity
        FROM tamayame_dictionary.entries e
        {where_sql}
        ORDER BY e.headword, e.entry_id
        LIMIT %s OFFSET %s
    """, params + [limit, offset])
    rows = cur.fetchall()

    cur.close(); conn.close()
    return rows, total


def fetch_word_summaries(
    search=None, pos=None, status=None, startswith=None,
    page=1, per_page=100
):
    """
    Paginated list for entries where type='word'.
    Returns: (rows, total_count)
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    wheres = ["e.type = %s"]
    params = ["word"]

    if search:
        wheres.append("(e.headword ILIKE %s OR e.translation_en ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])
    if pos:
        wheres.append("e.pos = %s"); params.append(pos)
    if status:
        wheres.append("e.status = %s"); params.append(status)
    if startswith:
        wheres.append("e.headword ILIKE %s"); params.append(f"{startswith}%")

    where_sql = "WHERE " + " AND ".join(wheres)
    offset = max(0, (int(page or 1) - 1) * int(per_page or 100))
    limit  = int(per_page or 100)

    cur.execute(f"SELECT COUNT(*) AS c FROM tamayame_dictionary.entries e {where_sql}", params)
    total = int(cur.fetchone()["c"])

    cur.execute(f"""
        SELECT
            e.entry_id, e.headword, e.type, e.affix_position, e.ipa, e.pos,
            e.translation_en, e.status, e.transitivity
        FROM tamayame_dictionary.entries e
        {where_sql}
        ORDER BY e.headword, e.entry_id
        LIMIT %s OFFSET %s
    """, params + [limit, offset])
    rows = cur.fetchall()

    cur.close(); conn.close()
    return rows, total

def fetch_entry_summaries(
    search=None, entry_type=None, pos=None, status=None, startswith=None,
    page=1, per_page=200
):
    """
    Paginated entry summaries.
    Returns: (rows, total_count)
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    wheres, params = [], []
    if search:
        wheres.append("(e.headword ILIKE %s OR e.translation_en ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])
    if entry_type:
        wheres.append("e.type = %s"); params.append(entry_type)
    if pos:
        wheres.append("e.pos = %s"); params.append(pos)
    if status:
        wheres.append("e.status = %s"); params.append(status)
    if startswith:
        wheres.append("e.headword ILIKE %s"); params.append(f"{startswith}%")

    where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
    offset = max(0, (int(page or 1) - 1) * int(per_page or 200))
    limit  = int(per_page or 200)

    # total count (same WHERE)
    count_sql = f"SELECT COUNT(*) AS c FROM tamayame_dictionary.entries e {where_sql}"
    cur.execute(count_sql, params)
    total_count = int(cur.fetchone()["c"])

    # page of rows
    list_sql = f"""
        SELECT
            e.entry_id,
            e.headword,
            e.type,
            e.affix_position,
            e.ipa,
            e.pos,
            e.translation_en,
            e.status,
            e.transitivity,
            e.intransitive_class_id,
            e.primary_paradigm_class_id,
            e.suffix_subclass_id
        FROM tamayame_dictionary.entries e
        {where_sql}
        ORDER BY e.headword, e.entry_id
        LIMIT %s OFFSET %s
    """
    cur.execute(list_sql, params + [limit, offset])
    rows = cur.fetchall()

    cur.close(); conn.close()
    return rows, total_count

def fetch_related_entries_by_segment(segment, exclude_entry_id=None, limit=50):
    """
    Find entries that use a given segment (from morphemes table) OR are that headword
    themselves. Excludes the current entry when exclude_entry_id is provided.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    params = [segment]
    where_excl = ""
    if exclude_entry_id is not None:
        where_excl = "AND e.entry_id <> %s"
        params.append(exclude_entry_id)

    sql = f"""
        (
          SELECT DISTINCT e.entry_id, e.headword, e.affix_position, e.pos, e.translation_en
          FROM tamayame_dictionary.morphemes m
          JOIN tamayame_dictionary.entries   e ON e.entry_id = m.entry_id
          WHERE m.segment = %s
          {where_excl}
        )
        UNION
        (
          SELECT e2.entry_id, e2.headword, e2.affix_position, e2.pos, e2.translation_en
          FROM tamayame_dictionary.entries e2
          WHERE e2.headword = %s
          {("AND e2.entry_id <> %s" if exclude_entry_id is not None else "")}
        )
        ORDER BY headword
        LIMIT %s
    """
    # bind params for the UNION and LIMIT
    bind = params + ([segment] + ([exclude_entry_id] if exclude_entry_id is not None else [])) + [int(limit)]
    cur.execute(sql, bind)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def fetch_entries_with_template(template_id, limit=200):
    """
    Entries linked to a template via entry_templates.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT e.entry_id, e.headword, e.transitivity, et.template_id
        FROM tamayame_dictionary.entry_templates et
        JOIN tamayame_dictionary.entries e ON e.entry_id = et.entry_id
        WHERE et.template_id = %s
        ORDER BY e.headword
        LIMIT %s
    """, (template_id, limit))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


def get_entry_by_id(entry_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT *
        FROM tamayame_dictionary.entries
        WHERE entry_id = %s
    """, (entry_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return row

# db/entries_dal.py (add near the bottom)
def fetch_template_by_id(template_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT *
        FROM tamayame_dictionary.templates
        WHERE template_id = %s
    """, (template_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return dict(row) if row else None    