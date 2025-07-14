import sqlalchemy
from sqlalchemy import create_engine, text

# Connect to PostgreSQL
engine = create_engine("postgresql+psycopg2://trpete13@localhost:5432/postgres")

# Define noun templates
templates = [
    {
        "name": "Free Noun",
        "type": "noun",
        "description": "A noun that can occur independently without possession or nominalization.",
        "slots": ["root"]
    },
    {
        "name": "Possessed Noun",
        "type": "noun",
        "description": "A noun that requires a possessive prefix.",
        "slots": ["possessor", "root"]
    },
    {
        "name": "Nominalized Noun",
        "type": "noun",
        "description": "A noun that must be derived using a nominalizer suffix.",
        "slots": ["root", "nominalizer"]
    }
]

# Insert templates
with engine.connect() as conn:
    for tpl in templates:
        result = conn.execute(
            text("""
                INSERT INTO tamayame_dictionary.templates (name, type, description, slots)
                VALUES (:name, :type, :description, :slots)
            """),
            {
                "name": tpl["name"],
                "type": tpl["type"],
                "description": tpl["description"],
                "slots": tpl["slots"]
            }
        )
    conn.commit()

print("✅ Inserted noun templates into the database.")
