import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
steps = [
    ROOT / "pipeline" / "01_inventario.py",
    ROOT / "pipeline" / "02_padronizar_mapear.py",
    ROOT / "pipeline" / "03_unificar_deduplicar.py",
    ROOT / "pipeline" / "04_inteligencia.py",
]

for s in steps:
    subprocess.run(["python", str(s)], check=False)
