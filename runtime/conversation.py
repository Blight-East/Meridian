import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from memory.structured.db import save_message, load_conversation as load_recent
