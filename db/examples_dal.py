# db/examples_dal.py
from typing import List, Dict, Any, Optional
from psycopg2.extras import RealDictCursor
from .core import get_connection

# ────────────────────────── small helpers ──────────────────────────
def _push_dedup(bucket: list, item: dict, key_fields=("form", "ur_gloss", "davis_id")):
    """
    Append `item` to `bucket` unless another item with the same key_fields exists.
    """
    k = tuple(item.get(f) for f in key_fields)
    for x in bucket:
        if tuple(x.get(f) for f in key_fields) == k:
            return
    bucket.append(item)


def _ur_add(ur_blocks: list, seen: set, segment, gloss, position, ordering):
    """
    Add a UR block (segment + gloss) to the list (de-duplicated by segment+gloss+position).
    """
    if not segment:
        return
    k = (segment, (gloss or "").strip(), position or "")
    if k in seen:
        return
    seen.add(k)
    ur_blocks.append({
        "segment": segment,
        "gloss": (gloss or "").strip(),
        "position": position or "",
        "ordering": ordering or 0,
    })


# ────────────────────────── thin helpers ──────────────────────────
def fetch_example_by_id(example_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT e.example_id, e.entry_id, e.tamayame_text, e.gloss_text,
               e.translation_en, e.comment
          FROM tamayame_dictionary.examples e
         WHERE e.example_id = %s
    """, (example_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return dict(row) if row else None


def get_entries_for_example(example_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT ee.entry_id, en.headword
          FROM tamayame_dictionary.example_entries ee
          JOIN tamayame_dictionary.entries en
            ON en.entry_id = ee.entry_id
         WHERE ee.example_id = %s
         ORDER BY ee.entry_id
    """, (example_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]


def get_media_for_example(example_id: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT type, filename, notes
          FROM tamayame_dictionary.media
         WHERE example_id = %s
         ORDER BY filename
    """, (example_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]


def fetch_example_prmp_allomorphs(example_id: int):
    """
    PRMP allomorphs linked via legacy example_prmp_allomorphs (slot 100).
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT a.form, a.ur_gloss, a.davis_id, epa.ordering
          FROM tamayame_dictionary.example_prmp_allomorphs epa
          JOIN tamayame_dictionary.allomorphs a
            ON a.allomorph_id = epa.allomorph_id
         WHERE epa.example_id = %s
         ORDER BY epa.ordering, a.form
    """, (example_id,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]


def fetch_example_ta_allomorph(example_id: int):
    """
    TA allomorph linked to this example:
      • preferred: example_morphemes.ta_allomorph_id
      • fallback: example_ta_allomorphs (legacy)
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Preferred: example_morphemes
    cur.execute("""
        SELECT ta.ta_id, ta.form, ta.number, ta.voice_class
          FROM tamayame_dictionary.example_morphemes em
          JOIN tamayame_dictionary.ta_allomorphs ta
            ON ta.ta_id = em.ta_allomorph_id
         WHERE em.example_id = %s
           AND em.ta_allomorph_id IS NOT NULL
         ORDER BY em.ordering
         LIMIT 1
    """, (example_id,))
    row = cur.fetchone()
    if row:
        cur.close(); conn.close()
        return dict(row)

    # Legacy
    cur.execute("""
        SELECT ta.ta_id, ta.form, ta.number, ta.voice_class
          FROM tamayame_dictionary.example_ta_allomorphs eta
          JOIN tamayame_dictionary.ta_allomorphs ta
            ON ta.ta_id = eta.ta_id
         WHERE eta.example_id = %s
         ORDER BY eta.ordering
         LIMIT 1
    """, (example_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    return dict(row) if row else None


def fetch_examples_for_morpheme(segment_normalized: str):
    """
    Examples that include the morpheme with `segment_normalized`.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT DISTINCT e.example_id, e.tamayame_text, e.translation_en
          FROM tamayame_dictionary.example_morphemes em
          JOIN tamayame_dictionary.morphemes m
            ON m.morpheme_id = em.morpheme_id
          JOIN tamayame_dictionary.examples e
            ON e.example_id = em.example_id
         WHERE m.segment = %s
         ORDER BY e.example_id
    """, (segment_normalized,))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]


def fetch_examples_by_template(template_id: int, limit=None, offset=0):
    """
    Examples linked to a template via example_templates.
    Returns: example_id, tamayame_text, translation_en, ur, sr, ipa (if present).
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    base_sql = """
        SELECT ex.example_id,
               ex.tamayame_text,
               ex.gloss_text,
               ex.translation_en,
               er.ur, er.sr, er.ipa
        FROM tamayame_dictionary.example_templates et
        JOIN tamayame_dictionary.examples ex
          ON ex.example_id = et.example_id
        LEFT JOIN tamayame_dictionary.example_realizations er
          ON er.example_id = ex.example_id
        WHERE et.template_id = %s
        ORDER BY ex.example_id
    """

    if limit is not None:
        cur.execute(f"{base_sql} LIMIT %s OFFSET %s",
                    (template_id, int(limit), int(offset or 0)))
    else:
        cur.execute(base_sql, (template_id,))

    rows = [dict(r) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows


# ────────────────────────── main payload builder ──────────────────────────
def fetch_example_full(example_id: int):
    """
    Returns a dict with:
      • example fields (tamayame_text, translation_en, comment, etc.)
      • morphemes: ordered UR blocks for “Stem morphology” (segment + gloss)
      • slotted_allomorphs: {'100': [...], 'TA': [...], 'ROOT': [...], '400': [...], ...}
      • prmp_allomorphs / ta_allomorphs convenience lists
      • template: {template_id, name} if linked

    Robust to legacy storage:
      - PRMP in example_prmp_allomorphs (slot 100)
      - TA in example_ta_allomorphs (legacy) or example_morphemes.ta_allomorph_id (current)
    """
    conn = get_connection()
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # 1) Example row
    cur.execute("""
        SELECT e.example_id, e.entry_id, e.tamayame_text, e.gloss_text,
               e.translation_en, e.comment
          FROM tamayame_dictionary.examples e
         WHERE e.example_id = %s
         LIMIT 1
    """, (example_id,))
    ex = cur.fetchone()
    if not ex:
        cur.close(); conn.close()
        return None
    example = dict(ex)

    # 2) Template (if any)
    cur.execute("""
        SELECT t.template_id, t.name
          FROM tamayame_dictionary.example_templates et
          JOIN tamayame_dictionary.templates t
            ON et.template_id = t.template_id
         WHERE et.example_id = %s
         LIMIT 1
    """, (example_id,))
    trow = cur.fetchone()
    example["template"] = dict(trow) if trow else None

    # 3) PRMP (legacy table)
    cur.execute("""
        SELECT a.form, a.ur_gloss, a.davis_id, epa.ordering
          FROM tamayame_dictionary.example_prmp_allomorphs epa
          JOIN tamayame_dictionary.allomorphs a
            ON epa.allomorph_id = a.allomorph_id
         WHERE epa.example_id = %s
         ORDER BY epa.ordering, a.form
    """, (example_id,))
    prmp_legacy = [dict(r) for r in cur.fetchall()]

    # 4) TA (legacy table)
    cur.execute("""
        SELECT ta.ta_id, ta.form, ta.number, ta.voice_class, eta.ordering
          FROM tamayame_dictionary.example_ta_allomorphs eta
          JOIN tamayame_dictionary.ta_allomorphs ta
            ON ta.ta_id = eta.ta_id
         WHERE eta.example_id = %s
         ORDER BY eta.ordering
    """, (example_id,))
    ta_legacy = [dict(r) for r in cur.fetchall()]

    # 5) Unified slot rows from example_morphemes
    cur.execute("""
        SELECT em.slot,
               em.ordering,
               m.segment        AS m_segment,
               m.gloss          AS m_gloss,
               a.form           AS a_form,
               a.ur_gloss       AS a_gloss,
               a.davis_id       AS a_davis,
               ta.form          AS ta_form,
               ta.number        AS ta_number,
               ta.voice_class   AS ta_voice
          FROM tamayame_dictionary.example_morphemes em
     LEFT JOIN tamayame_dictionary.morphemes      m  ON em.morpheme_id     = m.morpheme_id
     LEFT JOIN tamayame_dictionary.allomorphs     a  ON em.allomorph_id    = a.allomorph_id
     LEFT JOIN tamayame_dictionary.ta_allomorphs  ta ON em.ta_allomorph_id = ta.ta_id
         WHERE em.example_id = %s
         ORDER BY em.ordering
    """, (example_id,))
    rows = [dict(r) for r in cur.fetchall()]

    cur.close(); conn.close()

    # ─────────── determine observed slot order (for A/B placement) ───────────
    observed_slots = [(r.get("slot") or "").upper() for r in rows if r.get("slot")]
    def _first_index(sl):
        try:
            return observed_slots.index(sl)
        except ValueError:
            return None

    idx_root = _first_index("ROOT")
    idx_ta   = _first_index("TA")
    b_style = (idx_root is not None and idx_ta is not None and idx_root < idx_ta)
    # If ROOT appears before TA → “B-style” (TA after ROOT); else A-style

    # ─────────── build slotted cards (dedup) ───────────
    slotted = {}

    # From example_morphemes
    for r in rows:
        slot = (r["slot"] or "").upper()
        ord_ = r.get("ordering") or 0
        if slot == "TA" and r.get("ta_form"):
            _push_dedup(slotted.setdefault("TA", []), {
                "form":        r["ta_form"],
                "ur_gloss":    r["ta_number"],
                "voice_class": r.get("ta_voice"),
                "davis_id":    None,
                "ordering":    ord_,
            })
        elif slot == "ROOT" and r.get("m_segment"):
            _push_dedup(slotted.setdefault("ROOT", []), {
                "form":     r["m_segment"],
                "ur_gloss": r.get("m_gloss"),
                "davis_id": None,
                "ordering": ord_,
            })
        elif slot in ("100","200","300","400","500","600") and r.get("a_form"):
            _push_dedup(slotted.setdefault(slot, []), {
                "form":     r["a_form"],
                "ur_gloss": r.get("a_gloss"),
                "davis_id": r.get("a_davis"),
                "ordering": ord_,
            })

    # Bring in PRMP from legacy table only if 100 is missing entirely
    if "100" not in slotted and prmp_legacy:
        for p in prmp_legacy:
            _push_dedup(slotted.setdefault("100", []), {
                "form":     p["form"],
                "ur_gloss": p["ur_gloss"],
                "davis_id": p["davis_id"],
                "ordering": p.get("ordering") or 0,
            })

    # Bring in TA from legacy table only if TA is missing entirely
    if "TA" not in slotted and ta_legacy:
        for i, t in enumerate(ta_legacy, start=1):
            _push_dedup(slotted.setdefault("TA", []), {
                "form":        t["form"],
                "ur_gloss":    t["number"],
                "voice_class": t.get("voice_class"),
                "davis_id":    None,
                "ordering":    t.get("ordering") or i,
            })

    # Sort each slot by ordering
    for bucket in slotted.values():
        bucket.sort(key=lambda x: x.get("ordering", 0))

    # ─────────── assemble UR “Stem morphology” (ordered) ───────────
    ur_blocks = []
    seen_ur = set()

    # 1) Start with exactly what is stored (respects DB ordering)
    for r in rows:
        slot = (r.get("slot") or "").upper()
        ord_ = r.get("ordering") or 0
        if r.get("ta_form"):
            _ur_add(ur_blocks, seen_ur, r["ta_form"], r.get("ta_number"), "TA", ord_)
        elif r.get("a_form"):
            _ur_add(ur_blocks, seen_ur, r["a_form"], r.get("a_gloss"), slot, ord_)
        elif r.get("m_segment"):
            _ur_add(ur_blocks, seen_ur, r["m_segment"], r.get("m_gloss"), slot, ord_)

    # 2) If 100 never appeared in rows, but legacy PRMP exists — inject it
    has_100 = any(b.get("position") == "100" for b in ur_blocks)
    if not has_100 and prmp_legacy:
        root_orders = [b["ordering"] for b in ur_blocks if b["position"] == "ROOT"]
        ta_orders   = [b["ordering"] for b in ur_blocks if b["position"] == "TA"]
        if b_style and root_orders:
            inject_order = (max(root_orders) + 0.5)
        else:
            anchor = min(ta_orders + root_orders, default=0)
            inject_order = (anchor - 0.5)
        for p in prmp_legacy:
            _ur_add(ur_blocks, seen_ur, p["form"], p.get("ur_gloss"), "100", inject_order)

    # 3) If TA never appeared in rows, but legacy TA exists — inject depending on style
    has_TA = any(b.get("position") == "TA" for b in ur_blocks)
    if not has_TA and ta_legacy:
        root_orders = [b["ordering"] for b in ur_blocks if b["position"] == "ROOT"]
        if b_style and root_orders:
            inject_order = (max(root_orders) + 0.75)
        else:
            if root_orders:
                inject_order = (min(root_orders) - 0.75)
            else:
                inject_order = -1.0
        t = ta_legacy[0]
        _ur_add(ur_blocks, seen_ur, t["form"], t.get("number"), "TA", inject_order)

    # Final: stable sort by ordering
    ur_blocks.sort(key=lambda x: x.get("ordering", 0))

    # ─────────── ship it ───────────
    example["morphemes"] = ur_blocks
    example["slotted_allomorphs"] = slotted
    example["prmp_allomorphs"]    = slotted.get("100", [])
    example["ta_allomorphs"]      = slotted.get("TA", [])

    return example


__all__ = [
    "fetch_example_by_id",
    "get_entries_for_example",
    "get_media_for_example",
    "fetch_example_prmp_allomorphs",
    "fetch_example_ta_allomorph",
    "fetch_examples_for_morpheme",
    "fetch_examples_by_template",
    "fetch_example_full",
]

def fetch_stem_report_rows():
    """
    Stem Report to match example-detail:
      - stem: join segs with '-' (no spaces)
      - gloss_line: join ONLY non-empty tokens with '-' (no spaces)
      - token: ROOT -> morphemes.gloss; else -> allomorphs.ur_gloss (incl. TA)
      - translation: examples.translation_en
    """
    sql = """
    SELECT
      ex.example_id,
      en.headword,
      en.primary_paradigm_class_id,
      em.ordering,
      COALESCE(a.form, ta.form, m.segment) AS seg,
      CASE
        WHEN em.slot = 'ROOT' THEN NULLIF(m.gloss, '')
        ELSE NULLIF(a.ur_gloss, '')
      END AS gloss,
      ex.translation_en AS translation
    FROM tamayame_dictionary.examples ex
    JOIN tamayame_dictionary.example_morphemes em
      ON em.example_id = ex.example_id
    LEFT JOIN tamayame_dictionary.allomorphs a
      ON a.allomorph_id = em.allomorph_id
    LEFT JOIN tamayame_dictionary.ta_allomorphs ta
      ON ta.ta_id = em.ta_allomorph_id
    LEFT JOIN tamayame_dictionary.morphemes m
      ON m.morpheme_id = em.morpheme_id
    LEFT JOIN tamayame_dictionary.entries en
      ON en.entry_id = ex.entry_id
    ORDER BY ex.example_id, em.ordering;
    """

    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close(); conn.close()

    # Group & aggregate exactly like example-detail (skip empties)
    by_ex = {}
    for r in rows:
        exid = r["example_id"]
        rec = by_ex.setdefault(exid, {
            "example_id": exid,
            "headword": r["headword"],
            "translation": r["translation"],  # keep once per example
            "primary_paradigm_class": {1: "A", 2: "B", 3: "C", 4: "D"}.get(r["primary_paradigm_class_id"]),
            "segs": [],
            "glosses": []
        })
        rec["segs"].append(r["seg"] or "")
        token = (r["gloss"] or "").strip()
        if token:
            rec["glosses"].append(token)

    # Build final rows
    out = []
    for rec in by_ex.values():
        out.append({
            "example_id": rec["example_id"],
            "headword": rec["headword"],
            "translation": rec["translation"],   # <-- fixed (was r["translation"])
            "stem": "-".join(rec["segs"]),
            "gloss_line": "-".join(rec["glosses"]),
            "primary_paradigm_class": rec["primary_paradigm_class"],
        })
    out.sort(key=lambda x: x["example_id"])
    return out

def fetch_examples_by_segment(segment: str, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Return examples that include a morpheme whose exact segment matches `segment`.
    Adjust WHERE clause to ILIKE / pattern if you want fuzzy matching.
    """
    sql = """
    SELECT
      ex.example_id,
      ex.entry_id,
      en.headword,
      ex.tamayame_text,
      ex.translation_en
    FROM tamayame_dictionary.example_morphemes em
    JOIN tamayame_dictionary.morphemes m
      ON m.morpheme_id = em.morpheme_id
    JOIN tamayame_dictionary.examples ex
      ON ex.example_id = em.example_id
    LEFT JOIN tamayame_dictionary.entries en
      ON en.entry_id = ex.entry_id
    WHERE m.segment = %s
    ORDER BY ex.example_id
    LIMIT %s;
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(sql, (segment, limit))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [dict(r) for r in rows]    