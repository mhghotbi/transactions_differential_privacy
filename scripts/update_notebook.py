"""Update notebook with comprehensive DP evaluation cells."""
import json
from scripts.dp_evaluation_cells import (
    CELL_HEADER, CELL_SETUP, CELL_LOAD_DATA, CELL_PART_A_HEADER,
    CELL_A1_STATISTICAL, CELL_A2_DISTRIBUTIONAL, CELL_A3_STRUCTURAL,
    CELL_A4_PERCELL, CELL_A5_STRATIFIED, CELL_PART_B_HEADER,
    CELL_B1_QUERY, CELL_B2_RANKING, CELL_B3_TEMPORAL, CELL_B4_GEOGRAPHIC,
    CELL_FINAL_SUMMARY, CELL_EXPORT
)

# Load notebook
import os
notebook_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'demo_notebook.ipynb')
with open(notebook_path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

# Keep cells 0-17 (setup + pipeline)
original_cells = nb['cells'][:18]

# New evaluation cells
new_cells = [
    # Header
    {"cell_type": "markdown", "metadata": {}, "source": CELL_HEADER.strip().split('\n')},
    # Setup
    {"cell_type": "code", "metadata": {}, "source": CELL_SETUP.strip().split('\n'), "outputs": [], "execution_count": None},
    # Load data
    {"cell_type": "code", "metadata": {}, "source": CELL_LOAD_DATA.strip().split('\n'), "outputs": [], "execution_count": None},
    # Part A header
    {"cell_type": "markdown", "metadata": {}, "source": CELL_PART_A_HEADER.strip().split('\n')},
    # A1 Statistical
    {"cell_type": "code", "metadata": {}, "source": CELL_A1_STATISTICAL.strip().split('\n'), "outputs": [], "execution_count": None},
    # A2 Distributional
    {"cell_type": "code", "metadata": {}, "source": CELL_A2_DISTRIBUTIONAL.strip().split('\n'), "outputs": [], "execution_count": None},
    # A3 Structural
    {"cell_type": "code", "metadata": {}, "source": CELL_A3_STRUCTURAL.strip().split('\n'), "outputs": [], "execution_count": None},
    # A4 Per-cell
    {"cell_type": "code", "metadata": {}, "source": CELL_A4_PERCELL.strip().split('\n'), "outputs": [], "execution_count": None},
    # A5 Stratified
    {"cell_type": "code", "metadata": {}, "source": CELL_A5_STRATIFIED.strip().split('\n'), "outputs": [], "execution_count": None},
    # Part B header
    {"cell_type": "markdown", "metadata": {}, "source": CELL_PART_B_HEADER.strip().split('\n')},
    # B1 Query
    {"cell_type": "code", "metadata": {}, "source": CELL_B1_QUERY.strip().split('\n'), "outputs": [], "execution_count": None},
    # B2 Ranking
    {"cell_type": "code", "metadata": {}, "source": CELL_B2_RANKING.strip().split('\n'), "outputs": [], "execution_count": None},
    # B3 Temporal
    {"cell_type": "code", "metadata": {}, "source": CELL_B3_TEMPORAL.strip().split('\n'), "outputs": [], "execution_count": None},
    # B4 Geographic
    {"cell_type": "code", "metadata": {}, "source": CELL_B4_GEOGRAPHIC.strip().split('\n'), "outputs": [], "execution_count": None},
    # Summary
    {"cell_type": "code", "metadata": {}, "source": CELL_FINAL_SUMMARY.strip().split('\n'), "outputs": [], "execution_count": None},
    # Export
    {"cell_type": "code", "metadata": {}, "source": CELL_EXPORT.strip().split('\n'), "outputs": [], "execution_count": None},
]

# Fix source format (needs to be list of strings with newlines)
for cell in new_cells:
    if isinstance(cell['source'], list):
        cell['source'] = [line + '\n' for line in cell['source']]
        # Remove trailing newline from last line
        if cell['source']:
            cell['source'][-1] = cell['source'][-1].rstrip('\n')

# Combine
nb['cells'] = original_cells + new_cells

# Save
with open(notebook_path, 'w', encoding='utf-8') as f:
    json.dump(nb, f, indent=1, ensure_ascii=False)

print(f"Updated notebook with {len(new_cells)} new evaluation cells")
print(f"Total cells: {len(nb['cells'])}")

