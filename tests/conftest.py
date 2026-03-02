"""
Shared fixtures for Gerald-SuperBrain test suite.
"""
import sys
import os
import pytest

# Add project root and sniper skill to path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SNIPER_ROOT = os.path.join(PROJECT_ROOT, "skills", "gerald-sniper")

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, SNIPER_ROOT)

# Ensure .env is loaded for tests
from dotenv import load_dotenv
load_dotenv(os.path.join(PROJECT_ROOT, ".env"), override=True)
