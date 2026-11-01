#!/usr/bin/env python3
"""Entry point of the OPDI pipeline (same role as run_pipeline.py of the labs).

    source ~/spark-env/bin/activate
    python projet_opdi/run_pipeline.py --env dev --layer all
    python projet_opdi/run_pipeline.py --help
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))

from opdi_pipeline.cli import main  # noqa: E402  (after the path set-up)

if __name__ == "__main__":
    sys.exit(main())
