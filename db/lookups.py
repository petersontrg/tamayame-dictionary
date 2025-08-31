# db/lookups.py
from psycopg2.extras import RealDictCursor
from psycopg2 import DatabaseError
from .core import get_connection

__all__ = [
    "fetch_ta_allomorphs_by_number",
    "fetch_morpheme_index",
    "fetch_suffix_subclass_allomorphs",
    "fetch_primary_paradigm_classes",
    "fetch_suffix_subclasses",
    "fetch_intransitive_classes",
    "fetch_all_ta_allomorphs",
    "fetch_ta_forms",
    "fetch_prmp_usage",
    "fetch_prmp_allomorphs_for_class",
    "fetch_prmp_allomorphs_for_intransitive_entry",
    "fetch_examples_by_segment",
    "fetch_morpheme_usage",
    "fetch_template_by_id",
    "fetch_examples_using_template",
]

# ───────────────────────── helpers ───────────────────────── #
def _normalize_ta_number(n):
    if n is None:
        return None
    s = str(n).strip().lower()
    mapping = {"1": "singular", "sg": "singular", "singular": "singular",
               "2": "dual",     "du": "dual",     "dual": "dual",
               "3": "plural",   "pl": "plural",   "plural": "plural"}
    return mapping.get(s, s)

# ─────────────────────── TA allomorphs ───────────────────── #
def fetch_ta_allomorphs_by_number(number=None, voice_class=None, search=None, limit=500, offset=0):
    num_norm = _normalize_ta_number(number)
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    wheres, params = [], []
    if num_norm:
        wheres.append("ta.number = %s"); params.append(num_norm)
    if voice_class:
        wheres.append("ta.voice_class = %s"); params.append(voice_class)
    if search:
        wheres.append("ta.form ILIKE %s"); params.append(f"%{search}%")
    where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""

    sql = f"""
        SELECT ta.ta_id, ta.form, ta.number, ta.voice_class, a.davis_id
        FROM tamayame_dictionary.ta_allomorphs ta
        LEFT JOIN tamayame_dictionary.allomorphs a
               ON a.category = 'TA' AND a.form = ta.form
        {where_sql}
        ORDER BY ta.form, ta.ta_id
        LIMIT {int(limit or 500)} OFFSET {int(offset or 0)}
    """
    cur.execute(sql, params) if params else cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

# ───────────────────── Morpheme index ────────────────────── #
def fetch_morpheme_index(search=None, position=None, startswith=None, limit=2000, offset=0):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    wheres, params = [], []
    if search:
        like = f"%{search}%"
        wheres.append("(m.segment ILIKE %s OR COALESCE(m.gloss,'') ILIKE %s)")
        params.extend([like, like])
    if position:
        wheres.append("m.position ILIKE %s"); params.append(position)
    if startswith:
        wheres.append("m.segment ILIKE %s"); params.append(f"{startswith}%")

    where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
    sql = f"""
        WITH base AS (
            SELECT
                m.segment,
                COALESCE(m.gloss,'')    AS gloss,
                COALESCE(m.position,'') AS position,
                m.entry_id
            FROM tamayame_dictionary.morphemes m
            {where_sql}
        ),
        grouped AS (
            SELECT
                segment,
                gloss,
                COUNT(DISTINCT entry_id) AS used_from_m,
                BOOL_OR(UPPER(position) = 'TA'   OR position ILIKE 'ta%')         AS has_ta,
                BOOL_OR(UPPER(position) = 'ROOT' OR position ILIKE 'root%')       AS has_root,
                BOOL_OR(position = '100' OR position ILIKE 'prefix%' OR position ILIKE 'prmp%') AS has_prmp,
                BOOL_OR(position IN ('200','300','400','500','600') OR position ILIKE 'suffix%') AS has_suffix
            FROM base
            GROUP BY segment, gloss
        )
        SELECT
            g.segment,
            NULLIF(g.gloss,'') AS gloss,
            CASE
                WHEN g.has_root   THEN 'ROOT'
                WHEN g.has_ta     THEN 'TA'
                WHEN g.has_prmp   THEN 'PRMP'
                WHEN g.has_suffix THEN 'SUFFIX'
                ELSE '—'
            END AS category,
            -- examples count
            (
              SELECT COUNT(DISTINCT ex.example_id)
              FROM tamayame_dictionary.example_morphemes em
              JOIN tamayame_dictionary.examples ex ON ex.example_id = em.example_id
              JOIN tamayame_dictionary.morphemes m ON m.morpheme_id = em.morpheme_id
              WHERE m.segment = g.segment
            ) AS used_in_count,
            -- preview: up to 5 example texts, order by example_id
            (
              SELECT ARRAY(
                SELECT q.tamayame_text
                FROM (
                  SELECT DISTINCT ON (ex.example_id)
                         ex.example_id, ex.tamayame_text
                  FROM tamayame_dictionary.example_morphemes em
                  JOIN tamayame_dictionary.examples ex
                    ON ex.example_id = em.example_id
                  JOIN tamayame_dictionary.morphemes m
                    ON m.morpheme_id = em.morpheme_id
                  WHERE m.segment = g.segment
                  ORDER BY ex.example_id
                ) AS q
                ORDER BY q.example_id
                LIMIT 5
              )
            ) AS used_in_examples,
            g.used_from_m
        FROM grouped g
        ORDER BY g.segment
        LIMIT {limit} OFFSET {offset}
    """
    cur.execute(sql, params) if params else cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

# ─────── Suffix subclass allomorphs (FK / bridge tolerant) ─────── #
def fetch_suffix_subclass_allomorphs(subclass_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    union_sql = """
        WITH via_fk AS (
            SELECT a.form, a.ur_gloss, a.davis_id
            FROM tamayame_dictionary.allomorphs a
            WHERE a.category ILIKE 'suffix'
              AND a.suffix_subclass_id = %s
        ),
        via_bridge AS (
            SELECT a.form, a.ur_gloss, a.davis_id
            FROM tamayame_dictionary.suffix_subclass_allomorphs ssa
            JOIN tamayame_dictionary.allomorphs a
              ON a.allomorph_id = ssa.allomorph_id
            WHERE ssa.subclass_id = %s
        ),
        combined AS (
            SELECT * FROM via_fk
            UNION
            SELECT * FROM via_bridge
        )
        SELECT DISTINCT ON (form) form, ur_gloss, davis_id
        FROM combined
        ORDER BY form;
    """
    try:
        cur.execute(union_sql, (subclass_id, subclass_id))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except DatabaseError:
        conn.rollback()

    try:
        cur.execute("""
            SELECT DISTINCT a.form, a.ur_gloss, a.davis_id
            FROM tamayame_dictionary.suffix_subclass_allomorphs ssa
            JOIN tamayame_dictionary.allomorphs a
              ON a.allomorph_id = ssa.allomorph_id
            WHERE ssa.subclass_id = %s
            ORDER BY a.form
        """, (subclass_id,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except DatabaseError:
        conn.rollback()

    try:
        cur.execute("""
            SELECT DISTINCT a.form, a.ur_gloss, a.davis_id
            FROM tamayame_dictionary.allomorphs a
            WHERE a.category ILIKE 'suffix'
              AND a.suffix_subclass_id = %s
            ORDER BY a.form
        """, (subclass_id,))
        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows
    except DatabaseError:
        conn.rollback()
        cur.close(); conn.close()
        return []

# ─────────────── primary paradigm classes ─────────────── #
def fetch_primary_paradigm_classes(limit=None, offset=0):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    base_sql = """
        SELECT *
        FROM tamayame_dictionary.primary_paradigm_classes
        ORDER BY name, id
    """
    if limit is not None:
        cur.execute(f"{base_sql} LIMIT {int(limit)} OFFSET {int(offset or 0)}")
    else:
        cur.execute(base_sql)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

# ───────────────────── suffix subclasses ───────────────────── #
def _table_exists(cur, schema: str, name: str) -> bool:
    cur.execute("""
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        LIMIT 1
    """, (schema, name))
    return cur.fetchone() is not None


def fetch_suffix_subclasses(include_counts=True, limit=None, offset=0):
    """
    Return suffix subclasses. If include_counts=True, adds allomorph_count when
    the necessary tables exist; otherwise falls back to a simple SELECT *.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # Only attempt the counts if the tables exist
        has_allomorphs = _table_exists(cur, "tamayame_dictionary", "allomorphs")
        has_bridge     = _table_exists(cur, "tamayame_dictionary", "suffix_subclass_allomorphs")

        if include_counts and (has_allomorphs or has_bridge):
            base_sql = """
                WITH ss AS (
                  SELECT * FROM tamayame_dictionary.suffix_subclasses
                ),
                via_fk AS (
                  SELECT a.suffix_subclass_id AS id, COUNT(*)::int AS cnt
                  FROM tamayame_dictionary.allomorphs a
                  WHERE a.category ILIKE 'suffix'
                    AND a.suffix_subclass_id IS NOT NULL
                  GROUP BY a.suffix_subclass_id
                ),
                via_bridge AS (
                  SELECT ssa.subclass_id AS id, COUNT(*)::int AS cnt
                  FROM tamayame_dictionary.suffix_subclass_allomorphs ssa
                  GROUP BY ssa.subclass_id
                ),
                merged AS (
                  SELECT id, SUM(cnt)::int AS total
                  FROM (
                    SELECT * FROM via_fk
                    {maybe_union}
                  ) u
                  GROUP BY id
                )
                SELECT ss.*, COALESCE(m.total, 0) AS allomorph_count
                FROM ss
                LEFT JOIN merged m ON m.id = ss.id
                ORDER BY ss.name, ss.id
            """
            # If the bridge table does NOT exist, drop that branch from the CTE
            maybe_union = "UNION ALL SELECT * FROM via_bridge" if has_bridge else ""
            sql = base_sql.format(maybe_union=maybe_union)

            if limit is not None:
                cur.execute(f"{sql} LIMIT %s OFFSET %s", (int(limit), int(offset or 0)))
            else:
                cur.execute(sql)

        else:
            # Simple fallback (no counts)
            sql = """
                SELECT *
                FROM tamayame_dictionary.suffix_subclasses
                ORDER BY name, id
            """
            if limit is not None:
                cur.execute(f"{sql} LIMIT %s OFFSET %s", (int(limit), int(offset or 0)))
            else:
                cur.execute(sql)

        rows = [dict(r) for r in cur.fetchall()]
        return rows

    except Exception:
        # Heal any failed txn and fall back to the simplest possible query
        try:
            conn.rollback()
            cur.execute("""
                SELECT *
                FROM tamayame_dictionary.suffix_subclasses
                ORDER BY name, id
            """)
            rows = [dict(r) for r in cur.fetchall()]
            return rows
        finally:
            pass
    finally:
        cur.close(); conn.close()
        
# ───────────── intransitive classes (tolerant) ──────────── #
def fetch_intransitive_classes(limit=None, offset=0):
    """
    Preferred: return rows from tamayame_dictionary.intransitive_classes.
    Fallback: derive a minimal list from entries where transitivity='intransitive'.

    Returns dicts with keys: class_code, id, name, description, number_usage.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Try canonical table first
    try:
        base_sql = """
            SELECT *
            FROM tamayame_dictionary.intransitive_classes
            ORDER BY class_code, id
        """
        if limit is not None:
            cur.execute(f"{base_sql} LIMIT %s OFFSET %s", (int(limit), int(offset or 0)))
        else:
            cur.execute(base_sql)

        rows = [dict(r) for r in cur.fetchall()]
        cur.close(); conn.close()
        return rows

    except Exception:
        # IMPORTANT: clear the failed tx before any fallback query
        conn.rollback()

    # Fallback: derive from entries
    # (use a NEW cursor after rollback)
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT DISTINCT
            e.intransitive_class_id        AS class_code,
            NULL::int                      AS id,
            e.intransitive_class_id::text  AS name,
            NULL::text                     AS description,
            'mixed'::text                  AS number_usage
        FROM tamayame_dictionary.entries e
        WHERE e.transitivity = 'intransitive'
          AND e.intransitive_class_id IS NOT NULL
        ORDER BY class_code
    """)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

# ───── other helpers / reports ───── #
def fetch_all_ta_allomorphs(limit=None, offset=0, voice_class=None):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    wheres, params = [], []
    if voice_class:
        wheres.append("ta.voice_class = %s"); params.append(voice_class)
    where_sql = ("WHERE " + " AND ".join(wheres)) if wheres else ""
    base_sql = f"""
        SELECT ta.ta_id, ta.form, ta.number, ta.voice_class, a.davis_id
        FROM tamayame_dictionary.ta_allomorphs ta
        LEFT JOIN tamayame_dictionary.allomorphs a
               ON a.category = 'TA' AND a.form = ta.form
        {where_sql}
        ORDER BY ta.form, ta.ta_id
    """
    if limit is not None:
        cur.execute(f"{base_sql} LIMIT %s OFFSET %s", params + [int(limit), int(offset or 0)])
    else:
        cur.execute(base_sql, params)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

def fetch_ta_forms(entry_id=None, limit=None, offset=0):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    if entry_id:
        cur.execute("""
            SELECT DISTINCT ta.form
            FROM tamayame_dictionary.example_entries ee
            JOIN tamayame_dictionary.example_morphemes em ON em.example_id = ee.example_id
            JOIN tamayame_dictionary.ta_allomorphs ta      ON ta.ta_id = em.ta_allomorph_id
            WHERE ee.entry_id = %s AND em.ta_allomorph_id IS NOT NULL
            ORDER BY ta.form
        """, (int(entry_id),))
        forms = [r["form"] for r in cur.fetchall()]
        cur.close(); conn.close()
        return forms
    base_sql = "SELECT DISTINCT ta.form FROM tamayame_dictionary.ta_allomorphs ta ORDER BY ta.form"
    if limit is not None:
        cur.execute(f"{base_sql} LIMIT %s OFFSET %s", (int(limit), int(offset or 0)))
    else:
        cur.execute(base_sql)
    forms = [r["form"] for r in cur.fetchall()]
    cur.close(); conn.close()
    return forms

def fetch_prmp_usage(limit=1000, offset=0):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT a.form, a.ur_gloss, a.partial_paradigm, COUNT(*)::int AS uses
        FROM tamayame_dictionary.allomorphs a
        LEFT JOIN tamayame_dictionary.example_prmp_allomorphs epa
               ON epa.allomorph_id = a.allomorph_id
        WHERE a.category = 'PRMP'
        GROUP BY a.form, a.ur_gloss, a.partial_paradigm
        ORDER BY a.form
        LIMIT %s OFFSET %s
    """, (int(limit), int(offset or 0)))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

def fetch_prmp_allomorphs_for_class(class_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT DISTINCT a.allomorph_id, a.form, a.ur_gloss, a.davis_id, a.partial_paradigm
        FROM tamayame_dictionary.allomorphs a
        JOIN tamayame_dictionary.primary_paradigm_class_paradigms ppp
          ON a.partial_paradigm = ppp.partial_paradigm
        WHERE ppp.class_id = %s
          AND a.category='PRMP'
          AND COALESCE(a.transitivity, 'transitive')='transitive'
        ORDER BY a.partial_paradigm, a.form
    """, (class_id,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

def fetch_prmp_allomorphs_for_intransitive_entry(entry_id):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT a.form, a.ur_gloss, a.davis_id, a.partial_paradigm
        FROM tamayame_dictionary.allomorphs a
        WHERE a.category='PRMP'
          AND (a.entry_id=%s OR COALESCE(a.transitivity,'intransitive')='intransitive')
        ORDER BY a.form
    """, (entry_id,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

def fetch_examples_by_segment(segment, limit=200, offset=0):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT ex.example_id, ex.tamayame_text, ex.gloss_text, ex.translation_en,
               ARRAY_REMOVE(ARRAY_AGG(DISTINCT en.headword), NULL) AS headwords
        FROM tamayame_dictionary.example_morphemes em
        JOIN tamayame_dictionary.morphemes m ON em.morpheme_id = m.morpheme_id
        JOIN tamayame_dictionary.examples ex ON ex.example_id = em.example_id
        LEFT JOIN tamayame_dictionary.example_entries ee ON ee.example_id = ex.example_id
        LEFT JOIN tamayame_dictionary.entries en ON en.entry_id = ee.entry_id
        WHERE m.segment = %s
        GROUP BY ex.example_id
        ORDER BY ex.example_id
        LIMIT %s OFFSET %s
    """, (segment, int(limit), int(offset or 0)))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

# ─────────── Morpheme usage (entries + examples) ─────────── #
def fetch_morpheme_usage(segment, limit_entries=200, limit_examples=200):
    seg = (segment or "").strip()
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        WITH used AS (
            SELECT DISTINCT e.entry_id
            FROM tamayame_dictionary.morphemes m
            JOIN tamayame_dictionary.entries e ON e.entry_id = m.entry_id
            WHERE m.segment = %s
            UNION
            SELECT e2.entry_id
            FROM tamayame_dictionary.entries e2
            WHERE e2.headword = %s
        )
        SELECT e.entry_id, e.headword, e.type, e.pos, e.transitivity, e.translation_en
        FROM used u
        JOIN tamayame_dictionary.entries e ON e.entry_id = u.entry_id
        ORDER BY e.headword, e.entry_id
        LIMIT %s
    """, (seg, seg, int(limit_entries)))
    entries = [dict(r) for r in cur.fetchall()]

    cur.execute("""
        SELECT DISTINCT ex.example_id, ex.tamayame_text, ex.translation_en
        FROM tamayame_dictionary.example_morphemes em
        JOIN tamayame_dictionary.morphemes m ON m.morpheme_id = em.morpheme_id
        JOIN tamayame_dictionary.examples  ex ON ex.example_id = em.example_id
        WHERE m.segment = %s
        ORDER BY ex.example_id
        LIMIT %s
    """, (seg, int(limit_examples)))
    examples = [dict(r) for r in cur.fetchall()]

    cur.close(); conn.close()
    return {"segment": seg, "entries": entries, "examples": examples}

# ─────────────── Templates ─────────────── #
def fetch_template_by_id(template_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT *
        FROM tamayame_dictionary.templates
        WHERE template_id = %s
        LIMIT 1
    """, (template_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return dict(row) if row else None

def fetch_examples_using_template(template_id: int, limit: int | None = None, offset: int = 0):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    base_sql = """
        SELECT e.example_id, e.tamayame_text, e.translation_en, er.ur, er.sr, er.ipa
        FROM tamayame_dictionary.example_templates et
        JOIN tamayame_dictionary.examples e ON e.example_id = et.example_id
        LEFT JOIN tamayame_dictionary.example_realizations er ON er.example_id = e.example_id
        WHERE et.template_id = %s
        ORDER BY e.example_id
    """
    if limit is not None:
        cur.execute(f"{base_sql} LIMIT %s OFFSET %s", (template_id, int(limit), int(offset or 0)))
    else:
        cur.execute(base_sql, (template_id,))
    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows

# --- Prefix / PRMP allomorph report ----------------------------------------
def fetch_all_allomorphs(category: str = "PRMP", limit: int | None = None, offset: int = 0):
    """
    Return all allomorphs for a given category (default: PRMP/prefixes),
    with a usage_count computed from:
      • example_prmp_allomorphs (legacy PRMP table)
      • example_morphemes.allomorph_id (current slot storage)

    Yields rows shaped for the allomorph report template:
      allomorph_id, form, ur_gloss, davis_id, category,
      entry_id, headword, type, affix_position, usage_count
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    base_sql = """
        WITH usage_legacy AS (
            SELECT allomorph_id, COUNT(*)::int AS cnt
            FROM tamayame_dictionary.example_prmp_allomorphs
            GROUP BY allomorph_id
        ),
        usage_slots AS (
            SELECT allomorph_id, COUNT(*)::int AS cnt
            FROM tamayame_dictionary.example_morphemes
            WHERE allomorph_id IS NOT NULL
            GROUP BY allomorph_id
        )
        SELECT
            a.allomorph_id,
            a.form,
            a.ur_gloss,
            a.davis_id,
            a.category,
            a.entry_id,
            e.headword,
            e.type,
            e.affix_position,
            COALESCE(l.cnt,0) + COALESCE(s.cnt,0) AS usage_count
        FROM tamayame_dictionary.allomorphs a
        LEFT JOIN tamayame_dictionary.entries e
               ON e.entry_id = a.entry_id
        LEFT JOIN usage_legacy l
               ON l.allomorph_id = a.allomorph_id
        LEFT JOIN usage_slots s
               ON s.allomorph_id = a.allomorph_id
        WHERE (%s IS NULL) OR (a.category = %s)
        ORDER BY a.form, a.allomorph_id
    """

    params = [category, category]
    if limit is not None:
        cur.execute(f"{base_sql} LIMIT %s OFFSET %s", params + [int(limit), int(offset or 0)])
    else:
        cur.execute(base_sql, params)

    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows    

def fetch_prmp_usage_detail(allomorph_id: int):
    """
    Return where a PRMP allomorph is used:
      • entries that have examples containing this allomorph
      • examples that contain this allomorph (via legacy table or example_morphemes)

    Output:
      {
        "entries":  [ {entry_id, headword, type, affix_position} ... ],
        "examples": [ {example_id, tamayame_text, translation_en} ... ],
      }
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # All example IDs that contain this allomorph (legacy or new slot storage)
    cur.execute("""
        WITH ex_ids AS (
          SELECT DISTINCT epa.example_id
          FROM tamayame_dictionary.example_prmp_allomorphs epa
          WHERE epa.allomorph_id = %s
          UNION
          SELECT DISTINCT em.example_id
          FROM tamayame_dictionary.example_morphemes em
          WHERE em.allomorph_id = %s
        )
        SELECT example_id FROM ex_ids
    """, (allomorph_id, allomorph_id))
    ex_ids = [r["example_id"] for r in cur.fetchall()]

    entries = []
    examples = []
    if ex_ids:
        # Entries linked to those examples
        cur.execute("""
            SELECT DISTINCT e.entry_id, e.headword, e.type, e.affix_position
            FROM tamayame_dictionary.example_entries ee
            JOIN tamayame_dictionary.entries e
              ON e.entry_id = ee.entry_id
            WHERE ee.example_id = ANY(%s)
            ORDER BY e.headword, e.entry_id
        """, (ex_ids,))
        entries = [dict(r) for r in cur.fetchall()]

        # The examples themselves
        cur.execute("""
            SELECT ex.example_id, ex.tamayame_text, ex.translation_en
            FROM tamayame_dictionary.examples ex
            WHERE ex.example_id = ANY(%s)
            ORDER BY ex.example_id
        """, (ex_ids,))
        examples = [dict(r) for r in cur.fetchall()]

    cur.close(); conn.close()
    return {"entries": entries, "examples": examples} 


def fetch_all_stems(limit: int = 1000, offset: int = 0):
    """
    Stem report: for each example, aggregate the ordered surface 'blocks'
    into a single hyphen-joined stem and a dot-joined gloss line.

      Returns rows like:
        {
          "example_id": int,
          "entry_id": int | None,
          "headword": str | None,
          "stem": "káʼ-a-ú-kacha-nikuya-se-de",
          "gloss_line": "IND.3-REFL-sg-see-IMPV-PL.SUBJ-COND"
        }
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    cur.execute("""
        WITH example_first_entry AS (
          SELECT ee.example_id, MIN(ee.entry_id) AS entry_id
          FROM tamayame_dictionary.example_entries ee
          GROUP BY ee.example_id
        ),
        parts AS (
          SELECT
            em.example_id,
            em.ordering,
            -- choose the visible 'form' for this block
            COALESCE(ta.form, a.form, m.segment) AS block_form,
            -- choose the gloss label for this block
            COALESCE(
              NULLIF(ta.number, ''),          -- e.g., sg/dl/pl for TA
              NULLIF(a.ur_gloss, ''),         -- PRMP / suffix gloss
              NULLIF(m.gloss, '')             -- root gloss
            ) AS block_gloss
          FROM tamayame_dictionary.example_morphemes em
          LEFT JOIN tamayame_dictionary.ta_allomorphs ta
                 ON ta.ta_id = em.ta_allomorph_id
          LEFT JOIN tamayame_dictionary.allomorphs a
                 ON a.allomorph_id = em.allomorph_id
          LEFT JOIN tamayame_dictionary.morphemes m
                 ON m.morpheme_id = em.morpheme_id
        ),
        agg AS (
          SELECT
            p.example_id,
            STRING_AGG(p.block_form, '-' ORDER BY p.ordering) AS stem,
            STRING_AGG(COALESCE(p.block_gloss, ''), '-' ORDER BY p.ordering) AS gloss_line
          FROM parts p
          WHERE p.block_form IS NOT NULL
          GROUP BY p.example_id
        )
        SELECT
          ex.example_id,
          efe.entry_id,
          en.headword,
          a.stem,
          a.gloss_line
        FROM agg a
        JOIN tamayame_dictionary.examples ex
          ON ex.example_id = a.example_id
        LEFT JOIN example_first_entry efe
          ON efe.example_id = ex.example_id
        LEFT JOIN tamayame_dictionary.entries en
          ON en.entry_id = efe.entry_id
        ORDER BY ex.example_id
        LIMIT %s OFFSET %s
    """, (int(limit), int(offset or 0)))

    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows       