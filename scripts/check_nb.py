import json
import os
notebook_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'demo_notebook.ipynb')
with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

print(f"Total cells: {len(nb['cells'])}")
for i, cell in enumerate(nb['cells']):
    cell_type = cell['cell_type']
    src = ''.join(cell.get('source', []))[:80].replace('\n', ' ')
    print(f"Cell {i} ({cell_type}): {src}...")

