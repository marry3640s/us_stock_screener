import warnings; warnings.filterwarnings('ignore')
from sec_parsers import Filing
from pathlib import Path

html = Path('/Users/guowenyong/sec-data/DUOL/DUOL_10-K_2026-02-27.htm').read_text(encoding='utf-8', errors='replace')
f = Filing(html)
f.parse()
f.get_title_tree()