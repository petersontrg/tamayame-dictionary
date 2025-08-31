# db/intransitive.py
from .core import get_connection
import psycopg2

def intransitive_class_letter(raw):
    """
    Normalize entries.intransitive_class_id to 'A' or 'B'. Others -> None.
    """
    if raw is None:
        return None
    s = str(raw).strip().upper()
    return s if s in ("A", "B") else None

def _norm_number(val):
    if not val:
        return ""
    s = str(val).strip().lower()
    if s.startswith("sg") or s.startswith("sing"):
        return "sg"
    if s.startswith("dl") or s.startswith("du") or s.startswith("dual"):
        return "dl"
    if s.startswith("pl"):
        return "pl"
    return s

def fetch_entry_intransitive_classes(entry_id: int):
    """
    Return per-number intransitive class mappings for an entry.

    Assumes:
      - entry_intransitive_classes has: number, intransitive_class_id, ta_allomorph_id
      - intransitive_classes PK may be one of: id, class_id, intransitive_class_id
    Returns a dict keyed by number (e.g., 'sg','dl','pl') with:
      { 'class_code', 'number_usage', 'ta_id', 'ta_form', 'ta_number' }
    """
    schema = "tamayame_dictionary"
    conn = get_connection()
    cur = conn.cursor()

    # Discover PK column name in intransitive_classes
    cur.execute("""
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema=%s AND table_name=%s
         ORDER BY ordinal_position
    """, (schema, "intransitive_classes"))
    ic_cols = {r[0] for r in cur.fetchall()}

    ic_pk_candidates = ["id", "class_id", "intransitive_class_id"]
    ic_pk = next((c for c in ic_pk_candidates if c in ic_cols), None)
    if ic_pk is None:
        cur.close(); conn.close()
        raise RuntimeError(
            f"Couldn't find PK in {schema}.intransitive_classes; "
            f"looked for {ic_pk_candidates}, found {sorted(ic_cols)}"
        )

    # Build SQL using fixed ta_allomorph_id on EIC + detected PK on IC
    sql = f"""
        SELECT eic.number,
               ic.class_code,
               ic.number_usage,
               eic.ta_allomorph_id AS ta_fk,
               ta.form             AS ta_form,
               ta.number           AS ta_number
          FROM {schema}.entry_intransitive_classes eic
          JOIN {schema}.intransitive_classes ic
            ON eic.intransitive_class_id = ic.{ic_pk}
     LEFT JOIN {schema}.ta_allomorphs ta
            ON eic.ta_allomorph_id = ta.ta_id
         WHERE eic.entry_id = %s
         ORDER BY eic.number
    """

    cur.execute(sql, (entry_id,))
    rows = cur.fetchall()

    out = {}
    for number, class_code, number_usage, ta_fk, ta_form, ta_number in rows:
        out[number] = {
            "class_code":   class_code,
            "number_usage": number_usage,
            "ta_id":        ta_fk,
            "ta_form":      ta_form,
            "ta_number":    _norm_number(ta_number),
        }

    cur.close(); conn.close()
    return out

__all__ = ["intransitive_class_letter", "fetch_entry_intransitive_classes"]