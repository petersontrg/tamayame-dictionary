# app.py 
from flask import Flask, render_template, request, redirect, url_for, Response
from io import StringIO
import csv
from db import (
    get_connection,
    normalize_morpheme,
    fetch_entry,
    fetch_entry_summaries,
    fetch_related_entries_by_segment,
    fetch_examples_for_morpheme,
    insert_example,
    insert_morpheme,
    insert_allomorph,
    fetch_all_templates,
    fetch_template,
    fetch_entries_with_template, 
    refresh_entry_summary_view
)

def format_headword(headword, affix_position):
    if affix_position == 'prefix':
        return f"{headword}-"   # Prefix: root-like word followed by a hyphen
    elif affix_position == 'suffix':
        return f"-{headword}"   # Suffix: hyphen precedes word
    return headword

app = Flask(__name__)
app.jinja_env.globals.update(format_headword=format_headword)

@app.route('/add', methods=['GET', 'POST'], endpoint='add_entry')
def add_entry():
    message = None
    templates = fetch_all_templates()
    if request.method == 'POST':
        headword = request.form['headword']
        entry_type = request.form['type']
        bound_status = request.form.get('bound_status', 'unknown')
        morpheme_break = request.form.get('morpheme_break', '')
        pos = request.form.get('pos', '')
        gloss_en = request.form.get('gloss_en', '')
        translation_en = request.form.get('translation_en', '')
        definition_tamayame = request.form.get('definition_tamayame', '')
        notes = request.form.get('notes', '')
        source = request.form.get('source', '')
        status = request.form.get('status', 'draft')
        affix_position = request.form.get('affix_position', 'unknown')
        template_id = request.form.get('template_id') or None

        segments = request.form.getlist('segment[]')
        glosses = request.form.getlist('gloss[]')
        positions = request.form.getlist('position[]')

        allomorph_forms = request.form.getlist('allomorph_form[]')
        davis_ids = request.form.getlist('davis_id[]')
        allomorph_cats = request.form.getlist('allomorph_category[]')

        from entries import insert_entry
        entry_id = insert_entry(
            headword, entry_type, morpheme_break, pos,
            gloss_en, translation_en, definition_tamayame, notes, source,
            status, bound_status, affix_position, template_id
        )

        if entry_id:
            for i in range(len(segments)):
                segment = segments[i].strip()
                gloss = glosses[i].strip()
                position = positions[i]
                if segment and gloss:
                    insert_morpheme(entry_id, segment, gloss, position, i + 1)

            for i in range(len(allomorph_forms)):
                form = allomorph_forms[i].strip()
                davis = davis_ids[i].strip() or None
                cat = allomorph_cats[i].strip() or 'general'
                if form:
                    insert_allomorph(entry_id, form, cat, davis)

            refresh_entry_summary_view()
            return redirect(url_for('home'))
        else:
            message = f"The entry '{headword}' as type '{entry_type}' already exists."

    return render_template('add_entry.html', message=message, templates=templates)

@app.route('/')
def home():
    search = request.args.get('q')
    entry_type = request.args.get('type')
    pos = request.args.get('pos')
    status = request.args.get('status')
    startswith = request.args.get('startswith')

    entries = fetch_entry_summaries(search, entry_type, pos, status, startswith)
    return render_template("home.html", entries=entries,
                           search=search, entry_type=entry_type,
                           pos=pos, status=status, startswith=startswith)

@app.route('/morphemes')
def morpheme_index():
    from db import fetch_morpheme_index
    morphemes = fetch_morpheme_index()
    return render_template("morpheme_index.html", morphemes=morphemes)

@app.route('/entry/<int:entry_id>')
def entry_detail(entry_id):
    entry, morphemes, examples, allomorphs, template = fetch_entry(entry_id)
    if not entry:
        return "Entry not found", 404
    segment = normalize_morpheme(entry['headword'])
    related = fetch_related_entries_by_segment(segment)
    cross_examples = fetch_examples_for_morpheme(segment)

    template_entries = []
    if template and template['template_id']:
        template_entries = fetch_entries_with_template(template['template_id'])

    return render_template("entry_detail.html",
        entry=entry,
        morphemes=morphemes,
        examples=examples,
        related=related,
        cross_examples=cross_examples,
        allomorphs=allomorphs,
        template=template,
        template_entries=template_entries)

@app.route('/morpheme-report')
def morpheme_report():
    from db import fetch_promotable_morphemes
    morphemes = fetch_promotable_morphemes()
    return render_template("morpheme_report.html", morphemes=morphemes)

@app.route('/add-example/<int:entry_id>', methods=['GET', 'POST'])
def add_example(entry_id):
    if request.method == 'POST':
        tamayame_texts = request.form.getlist('tamayame_text[]')
        gloss_texts = request.form.getlist('gloss_text[]')
        translations = request.form.getlist('translation_en[]')

        for i in range(len(tamayame_texts)):
            tam = tamayame_texts[i].strip()
            gloss = gloss_texts[i].strip()
            trans = translations[i].strip()
            if tam and gloss and trans:
                insert_example(entry_id, tam, gloss, trans)

        return redirect(url_for('entry_detail', entry_id=entry_id))

    return render_template("add_example.html", entry_id=entry_id)

@app.route('/allomorph-report')
def allomorph_report():
    from db import fetch_all_allomorphs
    allomorphs = fetch_all_allomorphs()
    return render_template("allomorph_report.html", allomorphs=allomorphs)

@app.route('/help')
def help_page():
    return render_template("guide.html")

@app.route('/drafts')
def draft_entries():
    entries = fetch_entry_summaries(status='draft')
    return render_template("home.html", entries=entries, status='draft')

@app.route('/update-status/<int:entry_id>', methods=['POST'])
def update_status(entry_id):
    new_status = request.form.get('status')
    if new_status not in ('draft', 'verified'):
        return "Invalid status", 400

    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE tamayame_dictionary.entries
        SET status = %s
        WHERE entry_id = %s
    """, (new_status, entry_id))
    conn.commit()
    cur.close()
    conn.close()

    refresh_entry_summary_view()

    return redirect(url_for('home'))

@app.route('/select-example')
def select_entry_for_example():
    entries = fetch_entry_summaries()
    return render_template("select_example_entry.html", entries=entries)    

if __name__ == '__main__':
    app.run(debug=True)