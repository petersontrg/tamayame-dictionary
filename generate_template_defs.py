import csv
import json

# Path to your exported templates CSV
csv_path = "templates.csv"

# Output file
output_path = "template_defs.py"

templates = []

with open(csv_path, newline='', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    for row in reader:
        template_id = int(row['template_id'])
        name = row['name']
        
        # ✅ Correct parsing of slot_order as list
        slot_order_raw = row['slot_order'].strip('{}')
        slot_order = [s.strip() for s in slot_order_raw.split(',') if s.strip()]
        
        template_type = row.get('type', 'verb')
        transitivity = row.get('transitivity', '')

        templates.append({
            "template_id": template_id,
            "name": name,
            "slot_order": slot_order,
            "type": template_type,
            "transitivity": transitivity
        })

# Write as Python code
with open(output_path, "w", encoding="utf-8") as out:
    out.write("# Auto-generated from templates.csv\n\n")
    out.write("TEMPLATES = ")
    out.write(json.dumps(templates, indent=4, ensure_ascii=False))
    out.write("\n")