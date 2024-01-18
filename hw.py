import sys
import os
args = sys.argv
host = os.environ.get('Host')      #Host = "appdbdev"
arg = args[1]
prgm_file = args[0]
print(f"Hello {arg} from {prgm_file}")
print(f'connecting to the {host}')