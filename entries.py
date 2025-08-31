from psycopg2 import IntegrityError
from psycopg2.errorcodes import UNIQUE_VIOLATION
import logging
from db import get_connection, normalize_morpheme

logging.basicConfig(filename='duplicate_attempts.log', level=logging.INFO)

def insert_entry(headword, entry_type, morpheme_break, pos,
                 gloss_en, translation_en, definition_tamayame,
                 notes, source, status, bound_status,
                 affix_position, voice_class, ipa,
                 primary_paradigm_class_id, suffix_subclass_id, transitivity):

    # Normalize inputs (and coerce empty strings to None)
    def nz(v): 
        if v is None: return None
        s = normalize_morpheme(v)
        return s if s != '' else None

    payload = {
        "headword":                 nz(headword),
        "type":                     entry_type,                 # keep raw (enum/text value)
        "morpheme_break":           nz(morpheme_break),
        "pos":                      nz(pos),
        "gloss_en":                 nz(gloss_en),
        "translation_en":           nz(translation_en),
        "definition_tamayame":      nz(definition_tamayame),
        "notes":                    nz(notes),
        "source":                   nz(source),
        "status":                   status or "draft",
        "bound_status":             bound_status or "unknown",
        "affix_position":           nz(affix_position),
        "voice_class":              nz(voice_class),
        "ipa":                      nz(ipa),
        "primary_paradigm_class_id": primary_paradigm_class_id,   # cast to int in app.py
        "suffix_subclass_id":        suffix_subclass_id,          # cast to int in app.py
        "transitivity":              nz(transitivity),
    }

    conn = get_connection()
    cur = conn.cursor()

    try:
        # Only insert columns that actually exist in the table
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'tamayame_dictionary'
              AND table_name   = 'entries'
        """)
        existing_cols = {r[0] for r in cur.fetchall()}

        data = {k: v for k, v in payload.items() if k in existing_cols}

        cols = ", ".join(data.keys())
        ph   = ", ".join(["%s"] * len(data))
        vals = list(data.values())

        sql = f"""
            INSERT INTO tamayame_dictionary.entries ({cols})
            VALUES ({ph})
            RETURNING entry_id
        """
        cur.execute(sql, vals)
        entry_id = cur.fetchone()[0]
        conn.commit()
        return entry_id

    except IntegrityError as e:
        conn.rollback()
        if getattr(e, 'pgcode', None) == UNIQUE_VIOLATION and \
           getattr(e.diag, 'constraint_name', '') == 'unique_headword_type':
            logging.info(f"Duplicate headword+type: {payload['headword']}/{entry_type}")
            cur.execute("""
                SELECT entry_id
                FROM tamayame_dictionary.entries
                WHERE headword = %s AND type = %s
                LIMIT 1
            """, (payload['headword'], entry_type))
            row = cur.fetchone()
            return row[0] if row else None
        raise
    finally:
        cur.close()
        conn.close()