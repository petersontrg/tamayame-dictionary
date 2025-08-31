# db/__init__.py

# Core
from .core import get_connection, normalize_morpheme
from .mutations import insert_example

# Intransitive helpers
from .intransitive import (
    intransitive_class_letter,
    fetch_entry_intransitive_classes,
)

# Lookups (implemented in db/lookups.py)
from .lookups import (
    fetch_ta_allomorphs_by_number,
    fetch_morpheme_index,
    fetch_suffix_subclass_allomorphs,
    fetch_primary_paradigm_classes,
    fetch_suffix_subclasses,
    fetch_intransitive_classes,
    fetch_all_ta_allomorphs,
    fetch_ta_forms,
    fetch_prmp_usage,
    fetch_prmp_allomorphs_for_class,
    fetch_prmp_allomorphs_for_intransitive_entry,
    fetch_morpheme_usage,
    fetch_template_by_id,             # template metadata
    fetch_examples_using_template,    # examples tied to a template
    fetch_all_allomorphs,
    fetch_prmp_usage_detail,
    fetch_all_stems, 
)

# Entries (implemented in db/entries_dal.py)
from .entries_dal import (
    fetch_entry,
    fetch_entry_summaries,
    fetch_related_entries_by_segment,
    fetch_entries_with_template,
    get_entry_by_id,
)

# Examples (implemented in db/examples_dal.py)
from .examples_dal import (
    fetch_example_full,
    fetch_example_by_id,
    get_entries_for_example,
    get_media_for_example,
    fetch_examples_by_segment,   # where a segment appears in examples
    fetch_examples_by_template,  # optional helper; keep if implemented
)

# Mutations
from .mutations import (
    insert_example,
    insert_morpheme,
    insert_allomorph,
    refresh_entry_summary_view,
)

__all__ = [
    # core
    "get_connection", "normalize_morpheme",

    # intransitive helpers
    "intransitive_class_letter", "fetch_entry_intransitive_classes",

    # lookups
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
    "fetch_morpheme_usage",
    "fetch_template_by_id",
    "fetch_examples_using_template",
    "fetch_all_allomorphs",
    "fetch_prmp_usage_detail",
    "fetch_all_stems",

    # entries
    "fetch_entry", "fetch_entry_summaries",
    "fetch_related_entries_by_segment",
    "fetch_entries_with_template",
    "get_entry_by_id",

    # examples
    "fetch_example_full", "fetch_example_by_id",
    "get_entries_for_example", "get_media_for_example",
    "fetch_examples_by_segment", "fetch_examples_by_template",

    # mutations
    "insert_example", "insert_morpheme", "insert_allomorph",
    "refresh_entry_summary_view",
]