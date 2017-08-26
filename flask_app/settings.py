VECTORS_DIR = ""

try:
    from local_settings import *
except ImportError:
    print("NO local settings found. Will use global settings.")
