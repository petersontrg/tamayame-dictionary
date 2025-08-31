# app.py
from flask import (
    jsonify, Flask, render_template, request,
    redirect, url_for, flash, abort,
)
import os
import re
from werkzeug.utils import secure_filename

import math
from math import ceil
from db.core import normalize_morpheme
from psycopg2.extras import RealDictCursor
from db.intransitive import fetch_entry_intransitive_classes
from template_defs import TEMPLATES

from db import (
    # core
    get_connection,

    # entries
    fetch_entry,
    fetch_related_entries_by_segment,
    fetch_entries_with_template,
    get_entry_by_id,
    fetch_prmp_allomorphs_for_class,
    fetch_morpheme_index,
    fetch_all_allomorphs,
    fetch_all_stems,
    fetch_prmp_usage,

    # examples (implemented/exported)
    fetch_example_full,
    fetch_example_by_id,
    get_entries_for_example,
    get_media_for_example,

    # lookups
    fetch_primary_paradigm_classes,
    fetch_suffix_subclasses,
    fetch_intransitive_classes,
    fetch_all_ta_allomorphs,
    fetch_ta_forms,
    fetch_prmp_allomorphs_for_intransitive_entry,
    fetch_morpheme_usage,
    fetch_template_by_id,
    fetch_examples_using_template,
)

# summaries live in entries_dal
from db.entries_dal import (
    fetch_entry_summaries,
    fetch_root_summaries,
    fetch_word_summaries,
)

# mutations only from db.mutations (avoid duplicate names from db)
from db.mutations import (
    insert_example,
    insert_morpheme,
    insert_allomorph,
    refresh_entry_summary_view,
)

from db.examples_dal import fetch_stem_report_rows

# ── Flask setup ──────────────────────────────────────────────────
app = Flask(__name__)
app.secret_key = 'a-unique-and-secret-key'

# ── Uploads ──────────────────────────────────────────────────────
UPLOAD_FOLDER = 'static/uploads'
ALLOWED_EXTENSIONS = {
    'mp3','wav','jpg','jpeg','png','mp4',
    'webm','pdf','docx','txt','rtf','mov'
}
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return ('.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS)

def format_headword(headword, affix_position):
    if affix_position == 'prefix':
        return f"{headword}-"
    elif affix_position == 'suffix':
        return f"-{headword}"
    return headword

app.jinja_env.globals.update(format_headword=format_headword)

# ─────────────────────────────────────────────────────────────────────────────
# Entry: Add
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/add', methods=['GET', 'POST'], endpoint='add_entry')
def add_entry():
    """
    Add a new dictionary entry (root or affix), plus intransitive‐class→TA mappings.
    """
    message = None
    templates = TEMPLATES

    if request.method == 'POST':
        # ─── 1) Read the form fields ──────────────────────────────
        headword              = request.form['headword']
        entry_type            = request.form['type']
        bound_status          = request.form.get('bound_status', 'unknown')
        morpheme_break        = request.form.get('morpheme_break', '')
        pos                   = request.form.get('pos', '')
        transitivity          = request.form.get('transitivity') or None
        gloss_en              = request.form.get('gloss_en', '')
        translation_en        = request.form.get('translation_en', '')
        definition_tamayame   = request.form.get('definition_tamayame', '')
        notes                 = request.form.get('notes', '')
        source                = request.form.get('source', '')
        status                = request.form.get('status', 'draft')
        affix_position        = request.form.get('affix_position') or None
        voice_class           = request.form.get('voice_class') or None
        ipa                   = request.form.get('ipa', '')

        ppc_raw = request.form.get('primary_paradigm_class_id')
        ssc_raw = request.form.get('suffix_subclass_id')
        primary_paradigm_class_id = int(ppc_raw) if ppc_raw and ppc_raw.strip() else None
        suffix_subclass_id        = int(ssc_raw) if ssc_raw and ssc_raw.strip() else None

        # Intransitive X–X ↔ TA selections
        intrans_sg = request.form.get("intrans_sg_class_id")
        ta_sg      = request.form.get("intrans_sg_ta_id")
        intrans_dl = request.form.get("intrans_dl_class_id")
        ta_dl      = request.form.get("intrans_dl_ta_id")
        intrans_pl = request.form.get("intrans_pl_class_id")
        ta_pl      = request.form.get("intrans_pl_ta_id")

        # A/B template pick for intransitives
        intransitive_type = (request.form.get('intransitive_type') or '').strip().upper() or None

        # Collections
        segments        = request.form.getlist('segment[]')
        glosses         = request.form.getlist('gloss[]')
        positions       = request.form.getlist('position[]')
        allomorph_forms = request.form.getlist('allomorph_form[]')
        davis_ids       = request.form.getlist('davis_id[]')
        allomorph_cats  = request.form.getlist('allomorph_category[]')

        # Helpers for number normalization
        def _detect_allowed_numbers():
            try:
                conn = get_connection(); cur = conn.cursor()
                cur.execute("""
                    SELECT pg_get_constraintdef(c.oid)
                    FROM   pg_constraint c
                    JOIN   pg_class t ON t.oid = c.conrelid
                    JOIN   pg_namespace n ON n.oid = t.relnamespace
                    WHERE  c.conname = 'entry_intransitive_classes_number_check'
                """)
                row = cur.fetchone()
                cur.close(); conn.close()
                if not row or not row[0]:
                    return {'sg','du','pl'}
                text = row[0]
                vals = set(re.findall(r"'([A-Za-z]+)'::", text))
                return set(v.lower() for v in vals) if vals else {'sg','du','pl'}
            except Exception:
                return {'sg','du','pl'}

        def _norm_number_to_allowed(raw, allowed):
            r = (raw or '').strip().lower()
            if r in allowed:
                return r
            if r in ('sg','s','sing','singular'):
                for c in ('singular','sg'):
                    if c in allowed: return c
            if r in ('dl','du','dual'):
                for c in ('dual','du','dl'):
                    if c in allowed: return c
            if r in ('pl','p','plural'):
                for c in ('plural','pl'):
                    if c in allowed: return c
            return next(iter(allowed))

        # ─── 2) Insert entry ──────────────────────────────────────
        from entries import insert_entry
        try:
            entry_id = insert_entry(
                headword, entry_type, morpheme_break, pos,
                gloss_en, translation_en, definition_tamayame,
                notes, source, status, bound_status, affix_position,
                voice_class, ipa, primary_paradigm_class_id,
                suffix_subclass_id, transitivity
            )
        except Exception as e:
            print("insert_entry error:", e)
            entry_id = None

        # ─── After insert_entry(...) succeeds ─────────────────────
        if entry_id:
            # Only store A/B letter for INTRANSITIVE ROOT/STEM; clear otherwise
            is_intrans_root = (
                (entry_type in ('root', 'stem')) and
                ((transitivity or '').strip().lower() == 'intransitive')
            )
            try:
                conn = get_connection(); cur = conn.cursor()
                if is_intrans_root and intransitive_type:
                    cur.execute("""
                        UPDATE tamayame_dictionary.entries
                           SET intransitive_class_id = %s
                         WHERE entry_id = %s
                    """, (intransitive_type, entry_id))
                else:
                    cur.execute("""
                        UPDATE tamayame_dictionary.entries
                           SET intransitive_class_id = NULL
                         WHERE entry_id = %s
                    """, (entry_id,))
                conn.commit()
            except Exception as e:
                if conn: conn.rollback()
                print("⚠️ intransitive_class_id update skipped:", e)
            finally:
                try:
                    if cur: cur.close()
                    if conn: conn.close()
                except Exception:
                    pass

            # 3) Save morphemes (normalize to DDL's lowercase positions)
            VALID_POSITIONS = {'prefix','root','suffix','infix','circumfix','other'}

            def _norm_position(raw, default_pos):
                p = (raw or '').strip().lower()
                return p if p in VALID_POSITIONS else default_pos

            # Default: roots/stems → 'root'; affixes → affix_position ('prefix'/'suffix'), else 'other'
            default_pos = 'root' if entry_type in ('root', 'stem') \
                          else ((affix_position or '').strip().lower() or 'other')
            if default_pos not in VALID_POSITIONS:
                default_pos = 'other'

            for i, seg in enumerate(segments):
                seg_val   = (seg or '').strip()
                raw_gloss = (glosses[i] if i < len(glosses) else '' or '').strip()
                if not seg_val:
                    continue

                raw_pos = positions[i] if i < len(positions) else None
                pos_val = _norm_position(raw_pos, default_pos)

                # If it's a root morpheme and the row gloss is blank, use entry's gloss_en
                gloss_val = raw_gloss
                if not gloss_val and pos_val == 'root':
                    gloss_val = (gloss_en or '').strip()
                if not gloss_val:
                    continue

                # Sensible category default for DDL:
                category = 'root' if entry_type in ('root','stem') else 'affix'

                insert_morpheme(
                    entry_id=entry_id,
                    segment=seg_val,
                    gloss=gloss_val,
                    position=pos_val,      # 'root' / 'prefix' / 'suffix' / 'infix' / 'circumfix' / 'other'
                    ordering=i + 1,
                    category=category,
                )

            # 4) Save any new allomorphs
            for i, form in enumerate(allomorph_forms):
                if form.strip():
                    insert_allomorph(
                        entry_id,
                        form.strip(),
                        (allomorph_cats[i] if i < len(allomorph_cats) else None) or 'general',
                        (davis_ids[i] if i < len(davis_ids) else None) or None
                    )

            # 5) Store intransitive X–X ↔ TA choices (allow partials)
            conn = None; cur = None
            try:
                conn = get_connection(); cur = conn.cursor()
                allowed_numbers = _detect_allowed_numbers()
                rows_to_insert = [
                    ("sg", intrans_sg, ta_sg),
                    ("dl", intrans_dl, ta_dl),
                    ("pl", intrans_pl, ta_pl),
                ]
                for raw_number, cls_id_raw, ta_id_raw in rows_to_insert:
                    # insert if either a class or a TA was provided
                    if (cls_id_raw and cls_id_raw.strip()) or (ta_id_raw and ta_id_raw.strip()):
                        try:
                            cls_id = int(cls_id_raw) if (cls_id_raw and cls_id_raw.strip()) else None
                        except ValueError:
                            cls_id = None
                        try:
                            ta_id = int(ta_id_raw) if (ta_id_raw and ta_id_raw.strip()) else None
                        except ValueError:
                            ta_id = None

                        number = _norm_number_to_allowed(raw_number, allowed_numbers)
                        cur.execute("""
                          INSERT INTO tamayame_dictionary.entry_intransitive_classes
                            (entry_id, number, intransitive_class_id, ta_allomorph_id)
                          VALUES (%s, %s, %s, %s)
                          ON CONFLICT DO NOTHING
                        """, (entry_id, number, cls_id, ta_id))
                conn.commit()
            except Exception as e:
                if conn: conn.rollback()
                print("⚠️ Skipped inserting intransitive mappings:", e)
                flash("Saved the entry, but skipped some intransitive links (you can add them later).", "warning")
            finally:
                try:
                    if cur: cur.close()
                    if conn: conn.close()
                except Exception:
                    pass
                try:
                    refresh_entry_summary_view()
                except Exception as e:
                    print("⚠️ refresh_entry_summary_view failed:", e)

            return redirect(url_for('add_example', entry_id=entry_id))

        # ─── Insert failed → re-render form ──────────────────────
        return render_template(
            'add_entry.html',
            message="❌ Entry insert failed; check the form.",
            templates=templates,
            primary_paradigm_classes=fetch_primary_paradigm_classes(),
            suffix_subclasses=fetch_suffix_subclasses(include_counts=False),
            intransitive_classes=fetch_intransitive_classes_list(),
            ta_allomorphs=fetch_ta_allomorphs_labeled(),
        )

    # ─── GET: blank form ─────────────────────────────────────────
    return render_template(
        'add_entry.html',
        entry_id=None,
        message=None,
        templates=templates,
        primary_paradigm_classes=fetch_primary_paradigm_classes(),
        suffix_subclasses=fetch_suffix_subclasses(include_counts=False),
        intransitive_classes=fetch_intransitive_classes_list(),
        ta_allomorphs=fetch_ta_allomorphs_labeled(),
    )

# ─────────────────────────────────────────────────────────────────────────────
# Home / Lists
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/")
def home():
    search     = request.args.get("q") or None
    entry_type = request.args.get("type") or None
    pos        = request.args.get("pos") or None
    status     = request.args.get("status") or None
    startswith = request.args.get("startswith") or None

    page       = request.args.get("page", default=1, type=int)
    per_page   = request.args.get("per_page", default=200, type=int)
    entries, total = fetch_entry_summaries(
        search, entry_type, pos, status, startswith,
        page=page, per_page=per_page
    )
    total_pages = max(1, ceil(total / per_page))

    root_page     = request.args.get("root_page", default=1, type=int)
    root_per_page = request.args.get("root_per_page", default=100, type=int)
    roots, roots_total = fetch_root_summaries(
        search, pos, status, startswith,
        page=root_page, per_page=root_per_page
    )
    roots_pages = max(1, ceil(roots_total / root_per_page))

    word_page     = request.args.get("word_page", default=1, type=int)
    word_per_page = request.args.get("word_per_page", default=100, type=int)
    words, words_total = fetch_word_summaries(
        search, pos, status, startswith,
        page=word_page, per_page=word_per_page
    )
    words_pages = max(1, ceil(words_total / word_per_page))

    return render_template(
        "home.html",
        search=search, entry_type=entry_type, pos=pos, status=status, startswith=startswith,
        entries=entries, page=page, per_page=per_page, total=total, total_pages=total_pages,
        roots=roots, roots_total=roots_total, roots_pages=roots_pages,
        root_page=root_page, root_per_page=root_per_page,
        words=words, words_total=words_total, words_pages=words_pages,
        word_page=word_page, word_per_page=word_per_page
    )

@app.route('/morphemes')
def morpheme_index():
    return render_template("morpheme_index.html", morphemes=fetch_morpheme_index())

# ─────────────────────────────────────────────────────────────────────────────
# Entry detail
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/entry/<int:entry_id>')
def entry_detail(entry_id):
    entry, morphemes, examples, allomorphs, template = fetch_entry(entry_id)
    if not entry:
        return "Entry not found", 404

    # Start with the raw entry dict we got back
    entry_view = dict(entry)

    # Normalize "intransitivity" key for templates that expect it
    if "intransitivity" not in entry_view:
        entry_view["intransitivity"] = entry_view.get("transitivity")

    # Ensure the template sees an A/B letter when available
    entry_view["intransitive_class_letter"] = (
        entry.get("intransitive_class_letter")
        or entry.get("intransitive_class_id")
        or entry.get("intransitive_type")
    )

    # If we don't already have a single code, derive it from the first mapped class
    if "intransitive_class_code" not in entry_view and entry.get("intransitive_classes"):
        first = next(iter(entry["intransitive_classes"].values()), {})
        entry_view["intransitive_class_code"] = first.get("class_code")
        entry_view["intransitive_number_usage"] = first.get("number_usage")

    # Related entries by headword segment
    segment_key = entry.get("headword")
    related = []
    if segment_key:
        segment_norm = normalize_morpheme(segment_key)
        related = fetch_related_entries_by_segment(
            segment_norm, exclude_entry_id=entry_id, limit=50
        )

    # TA forms (if any)
    ta_forms = fetch_ta_forms(entry_id)

    # Other entries with same template (exclude this one)
    template_entries = []
    if template and template.get('template_id'):
        template_entries = [
            e for e in fetch_entries_with_template(template['template_id'])
            if e['entry_id'] != entry_id
        ]

    # Media + Suffix Subclass allomorphs (for transitive entries)
    conn = get_connection(); cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        # Media
        cur.execute("""
            SELECT type, filename, notes
            FROM tamayame_dictionary.media
            WHERE entry_id = %s
        """, (entry_id,))
        media = cur.fetchall()

        # Suffix Subclass details (transitives only)
        subclass_allos = []
        if (entry_view.get("transitivity") or "").lower() == "transitive":
            subclass_id = entry_view.get("suffix_subclass_id")
            if subclass_id:
                # Backfill human label if missing
                if not entry_view.get("suffix_subclass"):
                    cur.execute("""
                        SELECT name
                        FROM tamayame_dictionary.suffix_subclasses
                        WHERE id = %s
                        LIMIT 1
                    """, (subclass_id,))
                    row = cur.fetchone()
                    if row and row.get("name"):
                        entry_view["suffix_subclass"] = row["name"]

                # Fetch subclass-linked allomorphs
                cur.execute("""
                    SELECT a.allomorph_id, a.form, a.ur_gloss, a.davis_id
                    FROM tamayame_dictionary.subclass_allomorphs s
                    JOIN tamayame_dictionary.allomorphs a
                      ON a.allomorph_id = s.allomorph_id
                    WHERE s.subclass_id = %s
                    ORDER BY COALESCE(a.davis_id,'ZZZ'), a.form
                """, (subclass_id,))
                rows = cur.fetchall() or []
                subclass_allos = [
                    {
                        "allomorph_id": r["allomorph_id"],
                        "form": r["form"],
                        "ur_gloss": r.get("ur_gloss"),
                        "davis_id": r.get("davis_id"),
                    }
                    for r in rows
                ]

        # Make available to the template (your HTML already checks this key)
        entry_view["suffix_subclass_allomorphs"] = subclass_allos

    finally:
        cur.close(); conn.close()

    return render_template(
        "entry_detail.html",
        entry=entry_view,
        morphemes=morphemes,
        examples=examples,
        related=related,
        cross_examples=[],
        allomorphs=allomorphs,
        template=template,
        template_entries=template_entries,
        ta_forms=ta_forms,
        media=media,
    )

# ─────────────────────────────────────────────────────────────────────────────
# Reports & helpers
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/morpheme-report")
def morpheme_report():
    segment = (request.args.get("segment") or "").strip()
    if not segment:
        flash("No morpheme segment provided.", "warning")
        return redirect(url_for("morpheme_index"))
    data = fetch_morpheme_usage(segment)
    return render_template("morpheme_report.html",
                           segment=data["segment"],
                           entries=data["entries"],
                           examples=data["examples"])

# ─────────────────────────────────────────────────────────────────────────────
# Example: Add (builder)
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/add-example/<int:entry_id>', methods=['GET', 'POST'])
def add_example(entry_id):
    if request.method == 'POST':
        tamayame = request.form['tamayame_text'].strip()
        gloss    = request.form['gloss_text'].strip()
        trans    = request.form['translation_en'].strip()
        comment  = request.form.get('comment', '').strip()

        # Selected slot sequence (e.g., ['TA','100','ROOT','400'])
        slots = request.form.getlist('slots[]')

        # ── Common data used when re-rendering on error ───────────
        conn = get_connection(); cur = conn.cursor()

        cur.execute("""
            SELECT voice_class, primary_paradigm_class_id, transitivity
            FROM tamayame_dictionary.entries
            WHERE entry_id = %s
            LIMIT 1
        """, (entry_id,))
        row = cur.fetchone() or (None, 1, "Transitive")
        root_voice_class          = row[0]
        primary_paradigm_class_id = row[1] or 1
        transitivity              = row[2] or "Transitive"

        # Roots for THIS entry
        # Roots for THIS entry (ensure at least one 'root' morpheme exists)
        cur.execute("""
            SELECT morpheme_id, segment, gloss
            FROM tamayame_dictionary.morphemes
            WHERE entry_id = %s
              AND LOWER(COALESCE(position,'')) = 'root'
            ORDER BY ordering, segment
        """, (entry_id,))
        rows = cur.fetchall()

        if not rows:
            # seed a default ROOT morpheme using the headword if none exist
            cur.execute("SELECT headword FROM tamayame_dictionary.entries WHERE entry_id = %s", (entry_id,))
            r = cur.fetchone()
            head = (r[0] if r else '') or ''
            insert_morpheme(entry_id, head.strip() or head, '', 'root', 1)

            # re-query after seeding
            cur.execute("""
                SELECT morpheme_id, segment, gloss
                FROM tamayame_dictionary.morphemes
                WHERE entry_id = %s
                  AND LOWER(COALESCE(position,'')) = 'root'
                ORDER BY ordering, segment
            """, (entry_id,))
            rows = cur.fetchall()

        morphemes = [{'id': r[0], 'label': f"{r[1]} ({r[2]})" if r[2] else r[1]} for r in rows]

        # All allomorphs (fallback)
        cur.execute("""
            SELECT allomorph_id, form, ur_gloss, davis_id, category
            FROM tamayame_dictionary.allomorphs
            ORDER BY form
        """)
        allomorphs = [
            {'id': r[0], 'label': f"{r[1]} ({r[2] or ''})", 'form': r[1], 'ur_gloss': r[2], 'davis_id': r[3], 'category': r[4]}
            for r in cur.fetchall()
        ]

        # TA allomorphs
        cur.execute("""
            SELECT ta_id, form, number, voice_class
            FROM tamayame_dictionary.ta_allomorphs
            ORDER BY form
        """)
        ta_allomorphs = [
            {'id': r[0], 'label': f"{r[1]} ({r[2]})", 'voice_class': r[3], 'number': r[2]}
            for r in cur.fetchall()
        ]

        # PRMP by class (A–D => 1..4)
        prmp_by_class = {}
        for class_id in range(1, 5):
            rows = fetch_prmp_allomorphs_for_class(class_id)
            prmp_by_class[class_id] = [
                {'id': r['allomorph_id'], 'label': f"{r['form']} ({r.get('ur_gloss') or ''})"}
                for r in rows
            ]

        # Slot-specific lists (300 / 400 / 500 / B)
        slot_allomorphs = {}

        # 300 = Voice (REFL / PASS)
        cur.execute("""
            SELECT allomorph_id, form, ur_gloss, davis_id
            FROM tamayame_dictionary.allomorphs
            WHERE davis_id IN ('301','302A','302B')
               OR LOWER(COALESCE(category,'')) IN ('reflexive','passive')
               OR (UPPER(COALESCE(ur_gloss,'')) IN ('REFL','PASS')
                   AND LEFT(COALESCE(davis_id,''),3)='30')
            ORDER BY CASE davis_id
                       WHEN '301'  THEN 1
                       WHEN '302A' THEN 2
                       WHEN '302B' THEN 3
                       ELSE 99
                     END,
                     form
        """)
        slot_allomorphs['300'] = [
            {'id': r[0], 'label': f"{r[1]} ({r[2] or ''}) — {r[3] or ''}".strip(), 'form': r[1], 'ur_gloss': r[2], 'davis_id': r[3]}
            for r in cur.fetchall()
        ]

        # 400/500
        for s in ('400', '500'):
            cur.execute("""
                SELECT allomorph_id, form, ur_gloss
                FROM tamayame_dictionary.allomorphs
                WHERE category = %s
                ORDER BY form
            """, (s,))
            slot_allomorphs[s] = [{'id': r[0], 'label': f"{r[1]} ({r[2] or ''})"} for r in cur.fetchall()]

        # B (Benefactive) options
        cur.execute("""
          SELECT allomorph_id, form, COALESCE(ur_gloss,''), COALESCE(davis_id,'')
          FROM tamayame_dictionary.allomorphs
          WHERE LOWER(category) IN ('b','benefactive')
          ORDER BY form
        """)
        slot_allomorphs['B'] = [
            {
                'id': r[0],
                'label': f"{r[1]} ({r[2]})" + (f" — {r[3]}" if r[3] else ''),
                'form': r[1], 'ur_gloss': r[2], 'davis_id': r[3],
            }
            for r in cur.fetchall()
        ]

        # B → 500 map
        cur.execute("""
          SELECT b_allomorph_id, suffix500_allomorph_id
          FROM tamayame_dictionary.benefactive_500_map
        """)
        B_500_MAP = {}
        for b_id, s500_id in cur.fetchall():
            B_500_MAP.setdefault(b_id, []).append(s500_id)

        # Intransitive classes map + TA prefs for this entry (used by JS)
        INTRANS_MAP = fetch_entry_intransitive_classes(entry_id) or {}

        cur.execute("""
            SELECT LOWER(number) AS number, ta_allomorph_id
            FROM tamayame_dictionary.entry_intransitive_classes
            WHERE entry_id = %s
        """, (entry_id,))
        INTRANS_TA_PREFS = {row[0]: row[1] for row in cur.fetchall() if row[1]}

        # Guard: TA required
        if 'TA' not in slots:
            cur.close(); conn.close()
            return render_template(
                'add_example.html',
                entry_id=entry_id,
                morphemes=morphemes,
                allomorphs=allomorphs,
                ta_allomorphs=ta_allomorphs,
                root_voice_class=root_voice_class,
                primary_paradigm_class_id=primary_paradigm_class_id,
                prmp_by_class=prmp_by_class,
                slot_allomorphs=slot_allomorphs,
                TRANSITIVITY=transitivity,
                INTRANS_MAP=INTRANS_MAP,
                INTRANS_TA_PREFS=INTRANS_TA_PREFS,
                B_500_MAP=B_500_MAP,
                message="Please include a TA slot before saving this example."
            )

        # 3) Find matching template (if any)
        match = next((t for t in TEMPLATES if t['slot_order'] == slots), None)
        template_id = match['template_id'] if match else None

        # 4) Insert example row  ✅ payload dict (new signature)
        example_id = insert_example({
            "entry_id": entry_id,
            "tamayame_text": tamayame,
            "gloss_text": gloss,
            "translation_en": trans,
            "comment": comment,
        })

        # 5) Link to entry (+template optional)
        cur.execute("""
            INSERT INTO tamayame_dictionary.example_entries (example_id, entry_id)
            VALUES (%s, %s)
            ON CONFLICT DO NOTHING
        """, (example_id, entry_id))

        if template_id:
            cur.execute("""
                INSERT INTO tamayame_dictionary.example_templates (example_id, template_id)
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
            """, (example_id, template_id))

        # 6) Insert morpheme rows
        morpheme_slots   = request.form.getlist('slots[]')
        morpheme_ids     = request.form.getlist('morpheme_ids[]')       # ROOT
        allomorph_ids    = request.form.getlist('allomorph_ids[]')      # 100..600 (incl B/300)
        ta_allomorph_ids = request.form.getlist('ta_allomorph_ids[]')   # TA

        order_counter = 1
        for i, slot in enumerate(morpheme_slots):
            morpheme_id     = int(morpheme_ids[i]) if i < len(morpheme_ids) and morpheme_ids[i] not in ['', '0'] else None
            allomorph_id    = int(allomorph_ids[i]) if i < len(allomorph_ids) and allomorph_ids[i] not in ['', '0'] else None
            ta_allomorph_id = int(ta_allomorph_ids[i]) if i < len(ta_allomorph_ids) and ta_allomorph_ids[i] not in ['', '0'] else None
            if morpheme_id is None and allomorph_id is None and ta_allomorph_id is None:
                continue
            cur.execute("""
                INSERT INTO tamayame_dictionary.example_morphemes
                    (example_id, slot, morpheme_id, allomorph_id, ta_allomorph_id, ordering)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (example_id, slot, morpheme_id, allomorph_id, ta_allomorph_id, order_counter))
            order_counter += 1

        conn.commit()
        cur.close(); conn.close()
        return redirect(url_for('example_detail', example_id=example_id))

    # ── GET: load data for builder ───────────────────────────────
    conn = get_connection(); cur  = conn.cursor()

    cur.execute("""
        SELECT voice_class, primary_paradigm_class_id, transitivity
        FROM tamayame_dictionary.entries
        WHERE entry_id = %s
        LIMIT 1
    """, (entry_id,))
    row = cur.fetchone() or (None, 1, "Transitive")
    root_voice_class          = row[0]
    primary_paradigm_class_id = row[1] or 1
    transitivity              = row[2] or "Transitive"

    # ROOT morphemes (auto-seed from headword if none)
    cur.execute("""
        SELECT morpheme_id, segment, gloss
        FROM tamayame_dictionary.morphemes
        WHERE entry_id = %s
          AND LOWER(COALESCE(position,'')) = 'root'
        ORDER BY ordering, segment
    """, (entry_id,))
    rows = cur.fetchall()

    if not rows:
        cur.execute("SELECT headword FROM tamayame_dictionary.entries WHERE entry_id = %s", (entry_id,))
        r = cur.fetchone()
        head = (r[0] if r else '') or ''
        insert_morpheme(entry_id, head.strip() or head, '', 'root', 1)

        cur.execute("""
            SELECT morpheme_id, segment, gloss
            FROM tamayame_dictionary.morphemes
            WHERE entry_id = %s
              AND LOWER(COALESCE(position,'')) = 'root'
            ORDER BY ordering, segment
        """, (entry_id,))
        rows = cur.fetchall()

    morphemes = [{'id': r[0], 'label': f"{r[1]} ({r[2]})" if r[2] else r[1]} for r in rows]

    cur.execute("""
        SELECT allomorph_id, form, ur_gloss, davis_id, category
        FROM tamayame_dictionary.allomorphs
        ORDER BY form
    """)
    allomorphs = [
        {'id': r[0], 'label': f"{r[1]} ({r[2] or ''})", 'form': r[1], 'ur_gloss': r[2], 'davis_id': r[3], 'category': r[4]}
        for r in cur.fetchall()
    ]

    cur.execute("""
        SELECT ta_id, form, number, voice_class
        FROM tamayame_dictionary.ta_allomorphs
        ORDER BY form
    """)
    ta_allomorphs = [
        {'id': r[0], 'label': f"{r[1]} ({r[2]})", 'voice_class': r[3], 'number': r[2]}
        for r in cur.fetchall()
    ]

    prmp_by_class = {}
    for class_id in range(1, 5):
        rows = fetch_prmp_allomorphs_for_class(class_id)
        prmp_by_class[class_id] = [
            {'id': r['allomorph_id'], 'label': f"{r['form']} ({r.get('ur_gloss') or ''})"}
            for r in rows
        ]

    slot_allomorphs = {}

    cur.execute("""
        SELECT allomorph_id, form, ur_gloss, davis_id
        FROM tamayame_dictionary.allomorphs
        WHERE davis_id IN ('301','302A','302B')
           OR LOWER(COALESCE(category,'')) IN ('reflexive','passive')
           OR (UPPER(COALESCE(ur_gloss,'')) IN ('REFL','PASS')
               AND LEFT(COALESCE(davis_id,''),3)='30')
        ORDER BY CASE davis_id
                   WHEN '301'  THEN 1
                   WHEN '302A' THEN 2
                   WHEN '302B' THEN 3
                   ELSE 99
                 END,
                 form
    """)
    slot_allomorphs['300'] = [
        {'id': r[0], 'label': f"{r[1]} ({r[2] or ''}) — {r[3] or ''}".strip(), 'form': r[1], 'ur_gloss': r[2], 'davis_id': r[3]}
        for r in cur.fetchall()
    ]

    for s in ('400', '500'):
        cur.execute("""
            SELECT allomorph_id, form, ur_gloss
            FROM tamayame_dictionary.allomorphs
            WHERE category = %s
            ORDER BY form
        """, (s,))
        slot_allomorphs[s] = [{'id': r[0], 'label': f"{r[1]} ({r[2] or ''})"} for r in cur.fetchall()]

    # B (Benefactive) options (GET)
    cur.execute("""
      SELECT allomorph_id, form, COALESCE(ur_gloss,''), COALESCE(davis_id,'')
      FROM tamayame_dictionary.allomorphs
      WHERE LOWER(category) IN ('b','benefactive')
      ORDER BY form
    """)
    slot_allomorphs['B'] = [
        {
            'id': r[0],
            'label': f"{r[1]} ({r[2]})" + (f" — {r[3]}" if r[3] else ''),
            'form': r[1], 'ur_gloss': r[2], 'davis_id': r[3],
        }
        for r in cur.fetchall()
    ]

    # B → 500 (GET)
    cur.execute("""
      SELECT b_allomorph_id, suffix500_allomorph_id
      FROM tamayame_dictionary.benefactive_500_map
    """)
    B_500_MAP = {}
    for b_id, s500_id in cur.fetchall():
        B_500_MAP.setdefault(b_id, []).append(s500_id)

    INTRANS_MAP = fetch_entry_intransitive_classes(entry_id) or {}

    cur.execute("""
        SELECT LOWER(number) AS number, ta_allomorph_id
        FROM tamayame_dictionary.entry_intransitive_classes
        WHERE entry_id = %s
    """, (entry_id,))
    INTRANS_TA_PREFS = {row[0]: row[1] for row in cur.fetchall() if row[1]}

    cur.close(); conn.close()

    return render_template(
        'add_example.html',
        entry_id=entry_id,
        morphemes=morphemes,
        allomorphs=allomorphs,
        ta_allomorphs=ta_allomorphs,
        root_voice_class=root_voice_class,
        primary_paradigm_class_id=primary_paradigm_class_id,
        prmp_by_class=prmp_by_class,
        slot_allomorphs=slot_allomorphs,
        TRANSITIVITY=transitivity,
        INTRANS_MAP=INTRANS_MAP,
        INTRANS_TA_PREFS=INTRANS_TA_PREFS,
        B_500_MAP=B_500_MAP,
    )

# ─────────────────────────────────────────────────────────────────────────────
# Allomorph report (list)
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/allomorph-report')
def allomorph_report():
    return render_template("allomorph_report.html", allomorphs=fetch_all_allomorphs())

@app.route('/help')
def help_page():
    return render_template("guide.html")

@app.route('/drafts')
def draft_entries():
    page     = request.args.get("page", default=1, type=int)
    per_page = request.args.get("per_page", default=200, type=int)
    entries, total = fetch_entry_summaries(
        search=None, entry_type=None, pos=None, status='draft',
        startswith=None, page=page, per_page=per_page
    )
    total_pages = max(1, ceil(total / per_page))
    return render_template("drafts.html",
                           entries=entries, page=page, per_page=per_page,
                           total=total, total_pages=total_pages)

@app.route('/update-status/<int:entry_id>', methods=['POST'])
def update_status(entry_id):
    new_status = request.form.get('status')
    if new_status not in ('draft', 'verified'):
        return "Invalid status", 400

    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        UPDATE tamayame_dictionary.entries
        SET status = %s
        WHERE entry_id = %s
    """, (new_status, entry_id))
    conn.commit()
    cur.close(); conn.close()

    refresh_entry_summary_view()
    return redirect(url_for('home'))

@app.route('/select-example')
def select_entry_for_example():
    startswith = request.args.get('startswith')
    entries = fetch_entry_summaries(startswith=startswith)
    return render_template("select_example_entry.html", entries=entries, startswith=startswith)

# ─────────────────────────────────────────────────────────────────────────────
# TA detail
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/ta/<int:ta_id>')
def ta_detail(ta_id):
    conn = get_connection(); cur  = conn.cursor()
    cur.execute("""
        SELECT ta_id, form, number, voice_class, notes
          FROM tamayame_dictionary.ta_allomorphs
         WHERE ta_id = %s
    """, (ta_id,))
    ta = cur.fetchone()
    cur.close(); conn.close()
    if not ta:
        return "TA allomorph not found", 404

    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT e.entry_id, e.headword, ex.example_id, ex.tamayame_text
        FROM tamayame_dictionary.example_morphemes em
        JOIN tamayame_dictionary.example_entries ee
          ON ee.example_id = em.example_id
        JOIN tamayame_dictionary.entries e
          ON e.entry_id = ee.entry_id
        JOIN tamayame_dictionary.examples ex
          ON ex.example_id = em.example_id
        WHERE em.ta_allomorph_id = %s
        ORDER BY e.headword, ex.example_id
    """, (ta_id,))
    used = cur.fetchall()
    cur.close(); conn.close()

    return render_template('ta_detail.html', ta=ta, used=used)


@app.route("/ta-options/<number>")
def ta_options(number):
    num = number.lower()
    ali = {"singular":"sg","dual":"dl","plural":"pl"}
    num = ali.get(num, num)  # accept singular/dual/plural or sg/dl/pl
    ta = fetch_ta_allomorphs_labeled()
    filtered = [t for t in ta if (t.get("number") == num)]
    return jsonify({"number": num, "options": filtered})    

# --- DB helpers (top of app.py or db module) -------------------
def fetch_intransitive_classes_list():
    """All intransitive class codes for the X–X selectors."""
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        SELECT class_id, class_code
          FROM tamayame_dictionary.intransitive_classes
         ORDER BY class_code
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return [{"id": r[0], "label": r[1]} for r in rows]


def fetch_ta_allomorphs_labeled():
    """
    TA options with number baked into the label and a 'number' key
    so the UI can filter easily.
    """
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        SELECT ta_id, form, number
          FROM tamayame_dictionary.ta_allomorphs
         ORDER BY number, form
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()

    def _norm(n):
        n = (n or "").lower()
        return {"singular":"sg","dual":"dl","plural":"pl"}.get(n, n)

    return [
        {"id": r[0], "label": f"{r[1]} ({_norm(r[2])})", "number": _norm(r[2])}
        for r in rows
    ]  

# ─────────────────────────────────────────────────────────────────────────────
# PRMP options (slot 100) — voice / benefactive / intrans / fallback
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/get-prmp-options/<int:entry_id>/<int:slot>", endpoint="get_prmp_options_for_slot")
def get_prmp_options_for_slot(entry_id, slot):
    try:
        if slot != 100:
            return jsonify({"slot": slot, "options": [], "source": "unsupported-slot"}), 400

        ta_number    = (request.args.get("ta_number") or "sg").lower()
        long_alias   = {"sg":"singular", "dl":"dual", "pl":"plural"}.get(ta_number, ta_number)
        voice        = (request.args.get("voice") or "NONE").upper()
        transitivity = (request.args.get("transitivity") or "Transitive").title()
        has_b        = (request.args.get("has_b") or "0") in ("1", "true", "yes")

        def _rows_to_options(rows):
            # each row: (allomorph_id, davis_id, form, ur_gloss)
            return [
                {
                    "allomorph_id": r[0],
                    "davis_id": r[1],
                    "form": r[2],
                    "ur_gloss": r[3],
                }
                for r in rows
            ]

        # 1) Voice override (REFL/PASS) — curated membership (intrans only)
        if voice in ("REFL", "PASS"):
            conn = get_connection(); cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT a.allomorph_id, a.davis_id, a.form, a.ur_gloss
                      FROM tamayame_dictionary.prmp_voice_membership p
                      JOIN tamayame_dictionary.allomorphs a
                        ON a.allomorph_id = p.allomorph_id
                     WHERE p.voice = %s
                       AND a.category = 'PRMP'
                       AND a.ur_gloss IS NOT NULL
                       AND a.ur_gloss !~ '(^|[^0-9])[123]/[123]'  -- single-person = intrans
                     ORDER BY a.davis_id, length(a.form), a.allomorph_id
                """, (voice,))
                vrows = cur.fetchall()
            finally:
                cur.close(); conn.close()

            if vrows:
                return jsonify({
                    "slot": 100,
                    "source": f"voice-{voice.lower()}-membership",
                    "options": _rows_to_options(vrows)
                })

        # 2) Benefactive present? → force transitive inventory
        if has_b:
            conn = get_connection(); cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT COALESCE(primary_paradigm_class_id, 1)
                      FROM tamayame_dictionary.entries
                     WHERE entry_id = %s
                     LIMIT 1
                """, (entry_id,))
                r = cur.fetchone()
                t_class = int(r[0]) if r and r[0] else 1
            finally:
                cur.close(); conn.close()

            conn = get_connection(); cur = conn.cursor()
            try:
                cur.execute("""
                    SELECT DISTINCT ON (a.davis_id)
                           a.allomorph_id, a.davis_id, a.form, a.ur_gloss
                      FROM tamayame_dictionary.allomorphs a
                      JOIN tamayame_dictionary.primary_paradigm_class_paradigms p
                        ON p.class_id = %s
                       AND a.partial_paradigm = p.partial_paradigm
                     WHERE a.category = 'PRMP'
                       AND (
                             LOWER(COALESCE(a.transitivity,'')) = 'transitive'
                             OR (
                                  COALESCE(a.transitivity,'') = ''
                                  AND a.ur_gloss IS NOT NULL
                                  AND a.ur_gloss ~ '(^|[^0-9])[123]/[123]'
                                )
                           )
                     ORDER BY a.davis_id, length(a.form), a.allomorph_id
                """, (t_class,))
                rows = cur.fetchall()
            finally:
                cur.close(); conn.close()

            return jsonify({
                "slot": 100,
                "source": f"benefactive→transitive-class-{t_class}",
                "options": _rows_to_options(rows)
            })

        # 3) Intransitive path (normalized codes)
        if transitivity == "Intransitive":
            # Accept common aliases for the number scope
            _aliases = {
                "sg": ["sg", "singular"],
                "dl": ["dl", "du", "dual"],
                "du": ["dl", "du", "dual"],
                "pl": ["pl", "plural"],
            }
            scopes = _aliases.get(ta_number, [ta_number])

            conn = get_connection(); cur = conn.cursor()
            try:
                # Which intransitive class is mapped for this TA number?
                cur.execute("""
                    SELECT ic.class_code
                      FROM tamayame_dictionary.entry_intransitive_classes eic
                      JOIN tamayame_dictionary.intransitive_classes ic
                        ON ic.class_id = eic.intransitive_class_id
                     WHERE eic.entry_id = %s
                       AND LOWER(eic.number) = %s
                     LIMIT 1
                """, (entry_id, ta_number))
                r = cur.fetchone()
                class_code = r[0] if r else None
                if not class_code:
                    return jsonify({"slot": 100, "source": "intrans-missing-code", "options": []})

                # Expand normalized codes → PRMP allomorphs (intransitive inventory only)
                cur.execute("""
                    WITH codes AS (
                      SELECT v.base3, v.full4
                        FROM tamayame_dictionary.v_intrans_class_codes_norm v
                       WHERE v.class_code = %s
                         AND v.scope_norm = ANY(%s)      -- accept any alias (sg/du/pl/etc.)
                       GROUP BY v.base3, v.full4
                    ),
                    joined AS (
                      SELECT a.allomorph_id, a.davis_id, a.form, a.ur_gloss, a.transitivity
                        FROM tamayame_dictionary.allomorphs a
                        JOIN codes c
                          ON (
                               (c.full4 IS NOT NULL AND a.davis_id = c.full4)
                               OR
                               (c.full4 IS NULL  AND SUBSTRING(a.davis_id FROM '^[0-9]{3}') = c.base3)
                             )
                       WHERE a.category = 'PRMP'
                         AND (
                               LOWER(COALESCE(a.transitivity,'')) = 'intransitive'
                               OR (
                                    COALESCE(a.transitivity,'') = ''
                                    AND a.ur_gloss IS NOT NULL
                                    AND a.ur_gloss !~ '(^|[^0-9])[123]/[123]'  -- single-person
                                  )
                             )
                    )
                    SELECT DISTINCT ON (davis_id)
                           allomorph_id, davis_id, form, ur_gloss
                      FROM joined
                     ORDER BY davis_id, length(form), allomorph_id
                """, (class_code, scopes))
                irows = cur.fetchall()
            finally:
                cur.close(); conn.close()

            return jsonify({
                "slot": 100,
                "source": f"intrans-class-{class_code}",
                "options": _rows_to_options(irows)
            })

        # 4) Transitive fallback
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("""
                SELECT DISTINCT ON (a.davis_id)
                       a.allomorph_id, a.davis_id, a.form, a.ur_gloss
                  FROM tamayame_dictionary.allomorphs a
                 WHERE a.category = 'PRMP'
                   AND (
                         LOWER(COALESCE(a.transitivity,'')) = 'transitive'
                         OR (
                              COALESCE(a.transitivity,'') = ''
                              AND a.ur_gloss IS NOT NULL
                              AND a.ur_gloss ~ '(^|[^0-9])[123]/[123]'
                            )
                       )
                 ORDER BY a.davis_id, length(a.form), a.allomorph_id
            """)
            rows = cur.fetchall()
        finally:
            cur.close(); conn.close()

        return jsonify({
            "slot": 100,
            "source": "transitive-fallback",
            "options": _rows_to_options(rows)
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ─────────────────────────────────────────────────────────────────────────────
# PRMP pages, suffix options, stems, templates
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/prmp/<int:allomorph_id>')
def prmp_detail(allomorph_id):
    conn = get_connection(); cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT allomorph_id, form, ur_gloss, davis_id, category,
               subject_person, object_person, transitivity, entry_id
          FROM tamayame_dictionary.allomorphs
         WHERE allomorph_id = %s
         LIMIT 1
    """, (allomorph_id,))
    row = cur.fetchone()
    cur.close(); conn.close()
    if not row:
        return "PRMP allomorph not found", 404

    allomorph = dict(row)
    usage = fetch_prmp_usage(allomorph_id)  # ← use the imported name

    return render_template(
        'prmp_detail.html',
        allomorph=allomorph,
        entries=usage["entries"],
        examples=usage["examples"]
    )

@app.route("/get-suffix-options/<int:entry_id>/<series_csv>")
def get_suffix_options(entry_id, series_csv):
    fam = (series_csv or "").strip().split(",")[0]  # take first family only
    voice = (request.args.get("voice") or "NONE").upper()
    include_all_500 = (request.args.get("include_all_500") or "0") in ("1","true","yes")

    # 200 = FUT (leave as-is)
    if fam.startswith("2"):
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT allomorph_id, form, ur_gloss, davis_id
              FROM tamayame_dictionary.allomorphs
             WHERE LOWER(COALESCE(ur_gloss,'')) = 'fut'
                OR LOWER(COALESCE(category,'')) = 'future'
                OR (davis_id IS NOT NULL AND LEFT(davis_id,3) = '201')
             ORDER BY davis_id NULLS LAST, form
        """)
        rows = cur.fetchall(); cur.close(); conn.close()
        options = []
        for aid, form, gloss, did in rows:
            label = form + (f" ({gloss})" if gloss else " (FUT)")
            if did: label += f" — {did}"
            options.append({
                "allomorph_id": aid,
                "label": label,
                "form": form,
                "ur_gloss": gloss,
                "davis_id": did,
                "slot_code": "200",
                "source": "future"
            })
        return jsonify({"series": ["200"], "options": options})

    # Normalize to a family code
    family = "400" if fam.startswith("4") else "500" if fam.startswith("5") else "600" if fam.startswith("6") else None
    if family is None:
        return jsonify({"series": [], "options": []})

    # PASSive special-case: allow any 500
    if family == "500" and (voice == "PASS" or include_all_500):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("""
                SELECT allomorph_id, form, ur_gloss, davis_id
                  FROM tamayame_dictionary.allomorphs
                 WHERE (davis_id IS NOT NULL AND LEFT(davis_id,1) = '5')
                    OR LOWER(COALESCE(category,'')) IN (
                        '500','subject-number','agreement','subject-agreement','object-agreement'
                    )
                 ORDER BY COALESCE(davis_id,'ZZZ'), form
            """)
            rows = cur.fetchall()
        finally:
            cur.close(); conn.close()

        options = []
        for aid, form, gloss, did in rows:
            label = form + (f" ({gloss})" if gloss else "")
            if did: label += f" — {did}"
            options.append({
                "allomorph_id": aid,
                "label": label,
                "form": form,
                "ur_gloss": gloss,
                "davis_id": did,
                "slot_code": "500",
                "source": "passive-any-500"
            })
        return jsonify({"series": ["500"], "options": options})

    # ---------- Subclass-aware 400/500 ----------
    subclass_id = None
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT suffix_subclass_id
              FROM tamayame_dictionary.entries
             WHERE entry_id = %s
             LIMIT 1
        """, (entry_id,))
        r = cur.fetchone()
        if r and r[0]:
            subclass_id = int(r[0])
    finally:
        cur.close(); conn.close()

    if subclass_id and family in ("400", "500"):
        conn = get_connection(); cur = conn.cursor()
        try:
            cur.execute("""
                SELECT a.allomorph_id, a.form, COALESCE(a.ur_gloss,''), COALESCE(a.davis_id,'')
                  FROM tamayame_dictionary.subclass_allomorphs s
                  JOIN tamayame_dictionary.allomorphs a
                    ON a.allomorph_id = s.allomorph_id
                 WHERE s.subclass_id = %s
                 ORDER BY COALESCE(a.davis_id,'ZZZ'), a.form
            """, (subclass_id,))
            rows = cur.fetchall()
        finally:
            cur.close(); conn.close()

        fam_digit = family[0]  # '4' or '5'
        options = []
        for aid, form, gloss, did in rows:
            if (did or "").startswith(fam_digit):  # keep only requested family
                label = form + (f" ({gloss})" if gloss else "")
                if did: label += f" — {did}"
                options.append({
                    "allomorph_id": aid,
                    "label": label,
                    "form": form,
                    "ur_gloss": gloss,
                    "davis_id": did,
                    "slot_code": family,
                    "source": f"subclass-{subclass_id}"
                })
        if options:
            return jsonify({"series": [family], "options": options})

    # ---------- Fallbacks (category-based) ----------
    SERIES_CATEGORY_MAP = {
        "400": ["imperfective", "remote-state", "purposive", "passive", "reflexive"],
        "500": ["subject-number", "agreement", "subject-agreement", "object-agreement"],
        "600": ["conditional"],
    }
    cats = SERIES_CATEGORY_MAP[family]
    conn = get_connection(); cur = conn.cursor()
    cur.execute("""
        SELECT allomorph_id, form, COALESCE(ur_gloss,''), COALESCE(davis_id,'')
          FROM tamayame_dictionary.allomorphs
         WHERE LOWER(category) = ANY(%s)
         ORDER BY COALESCE(davis_id,'ZZZ'), form
    """, (list(map(str.lower, cats)),))
    rows = cur.fetchall(); cur.close(); conn.close()

    options = []
    for aid, form, gloss, did in rows:
        label = form + (f" ({gloss})" if gloss else "")
        if did: label += f" — {did}"
        options.append({
            "allomorph_id": aid,
            "label": label,
            "form": form,
            "ur_gloss": gloss,
            "davis_id": did,
            "slot_code": family,
            "source": "category"
        })
    return jsonify({"series": [family], "options": options})

@app.route("/stem-report")
def stem_report():
    rows = fetch_stem_report_rows()
    return render_template("stem_report.html", stems=rows)

@app.route("/template/<int:template_id>")
def template_detail(template_id):
    tpl = fetch_template_by_id(template_id)
    if not tpl:
        abort(404)

    slot_raw = tpl.get("slot_order") or tpl.get("slot_sequence") or tpl.get("slots")
    if isinstance(slot_raw, list):
        slot_list = slot_raw
    elif isinstance(slot_raw, str):
        parts = re.split(r"\s*[—–-]\s*|,\s*", slot_raw.strip())
        slot_list = [p for p in parts if p]
    else:
        slot_list = []
    tpl["slot_order"] = slot_list

    entries = fetch_entries_with_template(template_id)
    examples = fetch_examples_using_template(template_id)

    return render_template("template_detail.html",
                           template=tpl,
                           entries=entries,
                           examples=examples)

@app.route('/template-list')
def template_list():
    return render_template("template_list.html", templates=TEMPLATES)

# ─────────────────────────────────────────────────────────────────────────────
# Link example (legacy helper)
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/link-example/<int:example_id>', methods=['GET', 'POST'])
def link_example(example_id):
    conn = get_connection(); cur = conn.cursor()

    example = fetch_example_by_id(example_id)
    entry_id = example['entry_id']

    cur.execute("""
        SELECT primary_paradigm_class_id, name AS prmp_class_label, voice_class, suffix_subclass_id
          FROM tamayame_dictionary.entries e
     LEFT JOIN tamayame_dictionary.primary_paradigm_classes ppc
            ON e.primary_paradigm_class_id = ppc.id
         WHERE entry_id = %s
    """, (entry_id,))
    row = cur.fetchone()
    if row:
        class_id, prmp_class_label, voice_class, suffix_subclass_id = row
    else:
        class_id = prmp_class_label = voice_class = suffix_subclass_id = None

    ta_options = []
    if voice_class:
        cur.execute("""
            WITH rows AS (
              SELECT voice_class_id AS row, voice_class, form_sg AS sg, form_du AS dl, form_pl AS pl
              FROM tamayame_dictionary.ta_paradigm_rows
              WHERE voice_class = %s
            ),
            seq AS (
              SELECT row, voice_class, 'sg' AS number, sg AS form FROM rows
              UNION ALL
              SELECT row, voice_class, 'dl', dl FROM rows
              UNION ALL
              SELECT row, voice_class, 'pl', pl FROM rows
            ),
            joined AS (
              SELECT seq.*, t.ta_id, t.form AS ta_form,
                     ROW_NUMBER() OVER (
                       PARTITION BY seq.row, seq.number
                       ORDER BY t.ta_id
                     ) AS rk
              FROM seq
              JOIN tamayame_dictionary.ta_allomorphs t
                ON t.voice_class = seq.voice_class
               AND t.number = seq.number
               AND t.form = seq.form
            )
            SELECT ta_id, ta_form AS form, number, voice_class, row
            FROM joined
            WHERE rk = 1
            ORDER BY 
              CASE number WHEN 'sg' THEN 1 WHEN 'dl' THEN 2 ELSE 3 END, row;
        """, (voice_class,))
        ta_options = [dict(zip(('ta_id','form','number','voice_class','row'), r)) for r in cur.fetchall()]

    raw_own = fetch_prmp_allomorphs_for_class(class_id) if class_id else []
    prmp_own = [{'allomorph_id': r[0], 'form': r[1], 'ur_gloss': r[2]} for r in raw_own]
    raw_A = fetch_prmp_allomorphs_for_class(1)
    prmp_A = [{'allomorph_id': r[0], 'form': r[1], 'ur_gloss': r[2]} for r in raw_A]

    slot_options = {'400': [], '500': []}

    def _slot_options_for(family: str):
        if suffix_subclass_id:
            cur.execute("""
                SELECT a.allomorph_id, a.form, a.ur_gloss, a.davis_id
                FROM tamayame_dictionary.suffix_subclass_allomorphs ssa
                JOIN tamayame_dictionary.allomorphs a
                  ON a.allomorph_id = ssa.allomorph_id
                WHERE ssa.subclass_id = %s
                  AND (a.category = %s OR LEFT(COALESCE(a.davis_id,''),1) = %s)
                ORDER BY COALESCE(a.davis_id, 'ZZZ'), a.form
            """, (suffix_subclass_id, family, family[0]))
        else:
            cur.execute("""
                SELECT a.allomorph_id, a.form, a.ur_gloss, a.davis_id
                FROM tamayame_dictionary.allomorphs a
                WHERE a.category = %s OR LEFT(COALESCE(a.davis_id,''),1) = %s
                ORDER BY COALESCE(a.davis_id, 'ZZZ'), a.form
            """, (family, family[0]))
        return [{'id': r[0], 'label': f"{r[1]} ({r[2] or ''}) — {r[3] or ''}".strip()} for r in cur.fetchall()]

    slot_options['400'] = _slot_options_for('400')
    slot_options['500'] = _slot_options_for('500')

    if request.method == 'POST':
        prmp_id = request.form.get('prmp_allomorph')
        ta_id   = request.form.get('ta_allomorph')
        all_400 = request.form.get('slot_400')
        all_500 = request.form.get('slot_500')

        cur = conn.cursor()
        order = 1

        if ta_id:
            cur.execute("""
                INSERT INTO tamayame_dictionary.example_morphemes
                    (example_id, slot, ta_allomorph_id, ordering)
                VALUES (%s, 'TA', %s, %s)
            """, (example_id, ta_id, order))
            order += 1

        if all_400:
            cur.execute("""
                INSERT INTO tamayame_dictionary.example_morphemes
                    (example_id, slot, allomorph_id, ordering)
                VALUES (%s, '400', %s, %s)
            """, (example_id, all_400, order))
            order += 1

        if all_500:
            cur.execute("""
                INSERT INTO tamayame_dictionary.example_morphemes
                    (example_id, slot, allomorph_id, ordering)
                VALUES (%s, '500', %s, %s)
            """, (example_id, all_500, order))
            order += 1

        cur.execute("DELETE FROM tamayame_dictionary.example_morphemes WHERE example_id = %s AND slot = 'ROOT'", (example_id,))
        cur.execute("""
            SELECT morpheme_id, ordering
              FROM tamayame_dictionary.morphemes
             WHERE entry_id = %s
             ORDER BY ordering
        """, (entry_id,))
        for mid, ord_ in cur.fetchall():
            cur.execute("""
                INSERT INTO tamayame_dictionary.example_morphemes
                   (example_id, morpheme_id, ordering, slot)
                VALUES (%s, %s, %s, 'ROOT')
            """, (example_id, mid, ord_))

        conn.commit()
        cur.close(); conn.close()
        return redirect(url_for('example_detail', example_id=example_id))

    cur.close(); conn.close()

    return render_template(
        'link_example.html',
        example=example,
        slots=['100', 'TA', 'ROOT', '400', '500'],
        prmp_own=prmp_own,
        prmp_A=prmp_A,
        ta_options=ta_options,
        morpheme_options=fetch_morphemes_for_entry(entry_id=None) if 'fetch_morphemes_for_entry' in globals() else [],
        prmp_class_label=prmp_class_label,
        slot_options=slot_options
    )

@app.route('/link-examples/<int:entry_id>', methods=['GET'])
def link_examples(entry_id):
    conn = get_connection(); cur = conn.cursor()
    cur.execute("SELECT headword FROM tamayame_dictionary.entries WHERE entry_id = %s", (entry_id,))
    row = cur.fetchone()
    if not row:
        return "Entry not found", 404
    headword = row[0]

    cur.execute("""
        SELECT e.example_id, e.tamayame_text, e.translation_en
          FROM tamayame_dictionary.examples e
         WHERE e.tamayame_text ILIKE %s
           AND e.example_id NOT IN (
               SELECT example_id FROM tamayame_dictionary.example_entries
                WHERE entry_id = %s
           )
    """, (f'%{headword}%', entry_id))
    examples = cur.fetchall()
    cur.close(); conn.close()

    if not examples:
        flash("No new examples found containing this headword.")
        return redirect(url_for('entry_detail', entry_id=entry_id))

    return render_template("confirm_example_links.html",
                           entry_id=entry_id,
                           headword=headword,
                           examples=examples)

@app.route("/get-slot-options/<int:entry_id>")
def get_slot_options(entry_id):
    slot = int(request.args.get("slot"))

    if slot == 300:
        conn = get_connection(); cur = conn.cursor()
        cur.execute("""
            SELECT allomorph_id, form, ur_gloss, davis_id
              FROM tamayame_dictionary.allomorphs
             WHERE davis_id IN ('301', '302A', '302B')
                OR LOWER(COALESCE(category,'')) IN ('reflexive','passive')
                OR (UPPER(COALESCE(ur_gloss,'')) IN ('REFL','PASS') AND LEFT(COALESCE(davis_id,''),3) = '30')
             ORDER BY 
               CASE davis_id WHEN '301' THEN 1 WHEN '302A' THEN 2 WHEN '302B' THEN 3 ELSE 99 END,
               form
        """)
        rows = cur.fetchall(); cur.close(); conn.close()

        options = [{
            "allomorph_id": r[0],
            "label": f"{r[1]} ({r[2] or ''}) — {r[3] or ''}".strip(),
            "form": r[1],
            "ur_gloss": r[2],
            "davis_id": r[3],
            "slot_code": "300",
            "source": "300-series"
        } for r in rows]

        return jsonify({"series": ["300"], "options": options})

    return jsonify({"series": [], "options": []})

# ─────────────────────────────────────────────────────────────────────────────
# Validation helpers
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/validate-stem/<int:example_id>')
def validate_stem(example_id):
    # 1) Which template is linked to this example? (and which entry, for backlink)
    conn = get_connection(); cur = conn.cursor()
    try:
        cur.execute("""
            SELECT et.template_id,
                   COALESCE(ee.entry_id, NULL) AS entry_id
            FROM tamayame_dictionary.example_templates et
            LEFT JOIN tamayame_dictionary.example_entries ee
              ON ee.example_id = et.example_id
           WHERE et.example_id = %s
           LIMIT 1
        """, (example_id,))
        row = cur.fetchone()
        if not row:
            cur.close(); conn.close()
            return f"No template linked to example {example_id}"
        template_id, entry_id = row

        # 2) What slots are actually linked on this example?
        cur.execute("""
            SELECT DISTINCT UPPER(slot)
            FROM tamayame_dictionary.example_morphemes
            WHERE example_id = %s
              AND slot IS NOT NULL
            ORDER BY 1
        """, (example_id,))
        actual_slots = [r[0] for r in cur.fetchall()]
    finally:
        cur.close(); conn.close()

    # 3) Match against Python template defs
    matched = next((t for t in TEMPLATES if t["template_id"] == template_id), None)
    if not matched:
        return f"Template ID {template_id} not found in Python templates."

    expected_slots = matched['slot_order']
    template_name  = matched['name']

    # 4) Compare
    results = []
    for slot in expected_slots:
        if slot in actual_slots:
            results.append(f"Slot matched: {slot}")
        else:
            results.append(f"Missing slot: {slot}")
    for extra in actual_slots:
        if extra not in expected_slots:
            results.append(f"Unexpected slot: {extra}")

    return render_template(
        "validate_stem.html",
        example_id=example_id,
        template_name=template_name,
        template_slots=expected_slots,
        detected_slots=actual_slots,
        results=results,
        entry_id=entry_id
    )

@app.route('/validate-template-slots', methods=['POST'])
def validate_template_slots():
    data = request.get_json()
    slots = data.get('slots', [])
    if not slots:
        return jsonify({'match': False, 'error': 'No slots received.'})

    for t in TEMPLATES:
        if t['slot_order'] == slots:
            return jsonify({
                'match': True,
                'template_id': t['template_id'],
                'name': t['name'],
                'slots': t['slot_order']
            })
    return jsonify({'match': False})

# ─────────────────────────────────────────────────────────────────────────────
# Example detail / edit
# ─────────────────────────────────────────────────────────────────────────────
@app.route("/example/<int:example_id>")
def example_detail(example_id):
    ex = fetch_example_full(example_id)
    if not ex:
        abort(404)

    entries = get_entries_for_example(example_id) or []
    media   = get_media_for_example(example_id) or []
    if entries and "entry_id" not in ex:
        ex["entry_id"] = entries[0]["entry_id"]

    slotted = dict(ex.get("slotted_allomorphs") or {})
    slot_order = None
    if ex.get("template") and ex["template"].get("template_id"):
        tpl = next((t for t in TEMPLATES if t["template_id"] == ex["template"]["template_id"]), None)
        if tpl:
            slot_order = tpl["slot_order"]
    if not slot_order:
        slot_order = ["100","200","300","TA","ROOT","400","500","600"]

    stem_urs = []
    for code in slot_order:
        items = slotted.get(code) or []
        for it in items:
            stem_urs.append({
                "segment": it.get("form"),
                "gloss":   it.get("ur_gloss"),
                "ordering": it.get("ordering", 0),
            })

    seen = set(); stem_tiles = []
    for m in stem_urs:
        key = (m["segment"], m.get("gloss"))
        if key in seen:
            continue
        seen.add(key); stem_tiles.append(m)
    ex["morphemes"] = stem_tiles

    for k, items in list(slotted.items()):
        uniq = []
        seen = set()
        for it in items:
            key = (it.get("form"), it.get("ur_gloss"), it.get("davis_id"))
            if key in seen:
                continue
            seen.add(key); uniq.append(it)
        slotted[k] = uniq

    ex["slotted_allomorphs"] = {k: v for k, v in slotted.items() if v}

    if request.args.get("debug") == "1":
        return jsonify({
            "example_id": example_id,
            "template": ex.get("template"),
            "slot_order": slot_order,
            "slots": list(ex["slotted_allomorphs"].keys()),
            "slot_counts": {k: len(v) for k, v in ex["slotted_allomorphs"].items()},
            "ur_preview": ex["morphemes"][:6],
        })

    return render_template(
        "example_detail.html",
        example=ex,
        entries=entries,
        media=media,
        slotted_allomorphs=ex["slotted_allomorphs"],
    )

@app.route('/class-a')
def class_a_morphemes():
    prmp_a = fetch_prmp_allomorphs_for_class(1)
    return render_template('class_a.html', prmp_a=prmp_a)

@app.route('/edit-example/<int:example_id>', methods=['GET','POST'])
def edit_example(example_id):
    conn = get_connection(); cur  = conn.cursor()

    cur.execute("""
        SELECT template_id
          FROM tamayame_dictionary.example_templates
         WHERE example_id = %s
    """, (example_id,))
    row = cur.fetchone()
    current_template_id = row[0] if row else None

    cur.execute("""
        SELECT entry_id, tamayame_text, gloss_text, translation_en, comment
          FROM tamayame_dictionary.examples
         WHERE example_id = %s
    """, (example_id,))
    ex = cur.fetchone()
    if not ex:
        cur.close(); conn.close(); abort(404)
    entry_id, tamayame, gloss_text, translation_en, comment = ex

    if request.method == 'POST':
        new_tamayame = request.form['tamayame_text']
        new_gloss    = request.form['gloss_text']
        new_trans    = request.form['translation_en']
        new_comment  = request.form.get('comment','')

        cur.execute("""
            UPDATE tamayame_dictionary.examples
               SET tamayame_text   = %s,
                   gloss_text      = %s,
                   translation_en  = %s,
                   comment         = %s
             WHERE example_id     = %s
        """, (new_tamayame, new_gloss, new_trans, new_comment, example_id))

        new_prmps = request.form.getlist('prmp_allomorphs')
        cur.execute("""
          DELETE FROM tamayame_dictionary.example_prmp_allomorphs
           WHERE example_id = %s
        """, (example_id,))
        for idx, prmp_id in enumerate(new_prmps, start=1):
            cur.execute("""
              INSERT INTO tamayame_dictionary.example_prmp_allomorphs
                (example_id, allomorph_id, ordering)
              VALUES (%s, %s, %s)
              ON CONFLICT DO NOTHING
            """, (example_id, prmp_id, idx))

        new_ta = request.form.get('ta_allomorph') or None
        cur.execute("""
          DELETE FROM tamayame_dictionary.example_morphemes
           WHERE example_id = %s AND slot = 'TA'
        """, (example_id,))
        if new_ta:
            cur.execute("""
              INSERT INTO tamayame_dictionary.example_morphemes
                (example_id, slot, ta_allomorph_id, ordering)
              VALUES (%s, 'TA', %s, 1)
            """, (example_id, new_ta))

        new_tpl = request.form.get('template_id') or None
        cur.execute("""
          DELETE FROM tamayame_dictionary.example_templates
           WHERE example_id = %s
        """, (example_id,))
        if new_tpl:
            cur.execute("""
              INSERT INTO tamayame_dictionary.example_templates
                (example_id, template_id)
              VALUES (%s, %s)
            """, (example_id, new_tpl))

        conn.commit()
        cur.close(); conn.close()
        return redirect(url_for('example_detail', example_id=example_id))

    tamayame, gloss, trans, comment = ex[1:]

    cur.execute("""
      SELECT allomorph_id
        FROM tamayame_dictionary.example_prmp_allomorphs
       WHERE example_id=%s
       ORDER BY ordering
    """, (example_id,))
    current_prmps = [r[0] for r in cur.fetchall()]

    cur.execute("""
        SELECT ta_allomorph_id
          FROM tamayame_dictionary.example_morphemes
         WHERE example_id = %s AND slot = 'TA'
         LIMIT 1
    """, (example_id,))
    row = cur.fetchone()
    current_ta = row[0] if row else None

    cur.execute("SELECT primary_paradigm_class_id FROM tamayame_dictionary.entries WHERE entry_id=%s",
                (entry_id,))
    row = cur.fetchone()
    class_id = row[0] if (row and row[0] is not None) else 1

    prmp_own = fetch_prmp_allomorphs_for_class(class_id)
    prmp_A   = fetch_prmp_allomorphs_for_class(1)
    prmp_options = prmp_own + prmp_A

    ta_options = [
        {'ta_id': r[0], 'form': r[1], 'number': r[2]}
        for r in fetch_all_ta_allomorphs()
    ]

    templates = TEMPLATES

    cur.close(); conn.close()

    return render_template('edit_example.html',
        example_id=example_id,
        tamayame=tamayame,
        gloss=gloss,
        translation=trans,
        comment=comment,
        current_prmps=current_prmps,
        prmp_options=prmp_options,
        current_ta=current_ta,
        ta_options=ta_options,
        templates=templates,
        current_template_id=current_template_id
    )

# ─────────────────────────────────────────────────────────────────────────────
# Admin utilities
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/admin/refresh-summaries')
def admin_refresh_summaries():
    try:
        refresh_entry_summary_view()
        flash("Summaries refreshed.")
    except Exception as e:
        print("⚠️ refresh failed:", e)
        flash(f"Refresh failed: {e}", "error")
    return redirect(url_for('home'))

# ─────────────────────────────────────────────────────────────────────────────
# Media upload
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/upload-media/entry/<int:entry_id>', methods=['GET', 'POST'])
def upload_media_for_entry(entry_id):
    if request.method == 'POST':
        file = request.files['file']
        media_type = request.form.get('type')
        notes = request.form.get('notes', '')
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            conn = get_connection(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO tamayame_dictionary.media (entry_id, type, filename, notes)
                VALUES (%s, %s, %s, %s)
            """, (entry_id, media_type, filename, notes))
            conn.commit(); cur.close(); conn.close()
            flash("Media uploaded successfully.")
            return redirect(url_for('entry_detail', entry_id=entry_id))
    return render_template("upload_media.html", entry_id=entry_id)

@app.route('/upload-media/example/<int:example_id>', methods=['GET', 'POST'])
def upload_media_for_example(example_id):
    if request.method == 'POST':
        file = request.files['file']
        media_type = request.form.get('type')
        notes = request.form.get('notes', '')
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(file_path)
            conn = get_connection(); cur = conn.cursor()
            cur.execute("""
                INSERT INTO tamayame_dictionary.media (example_id, type, filename, notes)
                VALUES (%s, %s, %s, %s)
            """, (example_id, media_type, filename, notes))
            conn.commit(); cur.close(); conn.close()
            entries = get_entries_for_example(example_id)
            if entries:
                return redirect(url_for('entry_detail', entry_id=entries[0]['entry_id']))
            else:
                return redirect(url_for('home'))
    return render_template("upload_media.html", example_id=example_id)

@app.route('/edit-realization/<int:example_id>', methods=['GET', 'POST'], endpoint='edit_realization')
def edit_realization(example_id):
    # keep imports local to avoid circulars
    from db import fetch_example_by_id, insert_example_realization, get_entries_for_example

    if request.method == 'POST':
        ur  = request.form.get('ur') or None
        sr  = request.form.get('sr') or None
        ipa = request.form.get('ipa') or None

        insert_example_realization(example_id, ur, sr, ipa)

        # redirect back to the entry if possible
        entries = get_entries_for_example(example_id)
        if entries:
            return redirect(url_for('entry_detail', entry_id=entries[0]['entry_id']))
        return redirect(url_for('home'))

    example = fetch_example_by_id(example_id)
    return render_template("edit_realization.html", example=example)

# ─────────────────────────────────────────────────────────────────────────────
# Misc helpers
# ─────────────────────────────────────────────────────────────────────────────
@app.route('/help/intransitive-classes')
def help_intransitive_classes():
    return render_template("help_intransitive_classes.html")

@app.route('/check-secret')
def check_secret():
    if not app.debug:
        abort(404)
    print("🔒 SECRET KEY:", app.secret_key)
    return f"Secret key is: {app.secret_key or 'None'}"

@app.route('/help/primary-paradigms')
def help_primary_paradigms():
    return render_template("help_primary_paradigms.html")    

# ─────────────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    app.run(debug=True)