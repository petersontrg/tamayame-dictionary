from db import get_connection, normalize_morpheme, insert_entry

def fetch_unpromoted_morphemes():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT m.segment, m.gloss, m.category
        FROM tamayame_dictionary.morphemes m
        LEFT JOIN tamayame_dictionary.entries e
          ON m.segment = e.headword AND m.category = e.type
        WHERE e.headword IS NULL
          AND m.category IN ('root', 'affix')
        ORDER BY m.segment ASC;
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def auto_promote():
    morphemes = fetch_unpromoted_morphemes()
    print(f"Found {len(morphemes)} unpromoted morphemes.")

    count = 0
    for segment, gloss, category in morphemes:
        segment = normalize_morpheme(segment)
        gloss = normalize_morpheme(gloss)

        entry_id = insert_entry(
            headword=segment,
            entry_type=category,
            morpheme_break=segment,
            pos='',
            definition_en=gloss,
            definition_tamayame='',
            notes='Auto-promoted from morphemes table',
            source='',
            status='draft'
        )

        if entry_id:
            print(f"✅ Promoted: {segment} → entry_id {entry_id}")
            count += 1
        else:
            print(f"⚠️  Skipped (duplicate): {segment}")

    print(f"\n✅ Auto-promotion complete. {count} morphemes added.")

if __name__ == "__main__":
    auto_promote()
