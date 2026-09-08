"""Audit Table Shim: Re-export modul audit_table dari pipeline.audit.audit_table."""
import sys
from pathlib import Path

BASE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE))

from audit.audit_table import *
from audit.audit_table import main

if __name__ == "__main__":
    main()
