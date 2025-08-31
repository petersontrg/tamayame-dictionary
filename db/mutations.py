# db/mutations.py
from .core import get_connection

# Valid sets per your DDL
VALID_POSITIONS  = {'prefix', 'root', 'suffix', 'infix', 'circumfix', 'other'}
VALID_CATEGORIES = {'root', 'affix', 'stem', 'clitic', 'particle', 'unknown', 'ta'}


def insert_example(payload: dict) -> int:
    """
    Insert into tamayame_dictionary.examples using a dynamic payload.
    Returns the new example_id.
    """
    if not payload:
        raise ValueError("insert_example: payload is empty")

    fields = list(payload.keys())
    placeholders = ["%s"] * len(fields)
    params = [payload[k] for k in fields]

    sql = f"""
        INSERT INTO tamayame_dictionary.examples ({", ".join(fields)})
        VALUES ({", ".join(placeholders)})
        RETURNING example_id
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql, params)
    new_id = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()
    return new_id


def insert_morpheme(
    *,
    entry_id: int,
    segment: str,
    gloss: str = "",
    position: str = "root",
    ordering: int = 1,
    category: str = "unknown",
) -> int | None:
    """
    Insert a morpheme row that satisfies your table DDL:

      columns: (entry_id, segment, gloss, position, ordering, category)

    - `position` is validated against: {'prefix','root','suffix','infix','circumfix','other'}
    - `category` is validated against: {'root','affix','stem','clitic','particle','unknown','ta'}
    - De-duplicates via your unique constraint (entry_id, segment, gloss, position).
      Returns morpheme_id, or None if it already existed.
    """
    if not segment:
        raise ValueError("insert_morpheme: segment is required")

    pos = (position or "root").strip().lower()
    if pos not in VALID_POSITIONS:
        pos = "root"

    cat = (category or "unknown").strip().lower()
    if cat not in VALID_CATEGORIES:
        cat = "unknown"

    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tamayame_dictionary.morphemes
            (entry_id, segment, gloss, position, ordering, category, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
        ON CONFLICT ON CONSTRAINT uniq_entry_segment_gloss_position DO NOTHING
        RETURNING morpheme_id
        """,
        (entry_id, segment, gloss, pos, ordering, cat),
    )
    row = cur.fetchone()
    conn.commit()
    cur.close(); conn.close()
    return row[0] if row else None


def insert_allomorph(
    entry_id: int,
    form: str,
    category: str,
    davis_id: str | None = None,
    partial_paradigm: str | None = None,
    transitivity: str | None = None,
) -> int:
    """
    Insert a row into tamayame_dictionary.allomorphs and return allomorph_id.
    """
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tamayame_dictionary.allomorphs
            (entry_id, form, category, davis_id, partial_paradigm, transitivity)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING allomorph_id
        """,
        (entry_id, form, category, davis_id, partial_paradigm, transitivity),
    )
    aid = cur.fetchone()[0]
    conn.commit()
    cur.close(); conn.close()
    return aid


def refresh_entry_summary_view() -> None:
    """
    Refresh the materialized view 'tamayame_dictionary.entry_summary' if present.
    Tries CONCURRENTLY first (requires a unique index), then falls back.
    Silently no-ops if the view doesn't exist.
    """
    conn = get_connection()
    cur = conn.cursor()
    try:
        try:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY tamayame_dictionary.entry_summary;")
            conn.commit()
            return
        except Exception:
            conn.rollback()
            # Fallback to non-concurrent
            cur.execute("REFRESH MATERIALIZED VIEW tamayame_dictionary.entry_summary;")
            conn.commit()
    except Exception:
        # swallow (view may not exist)
        conn.rollback()
    finally:
        cur.close(); conn.close()