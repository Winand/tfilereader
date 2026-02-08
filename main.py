import sys
from pathlib import Path

from tfile_reader import TFileReader

if __name__ == "__main__":
    reader = TFileReader(Path(sys.argv[1]))
    print(f"BCFile Version: {reader.version}")
    print(f"TFile Version: {reader.meta.version}")
    for container, log_type, contents in reader.parse():
        print("Container:", container)
        print("LogType:", log_type)
        print("LogContents:", contents.strip()[:500])
        print()
    print("Log metadata")  # available after parsing
    print(f"Log Version: {reader.log_version}")
    print(f"Application ACL: {reader.app_acl}")
    print(f"Application owner: {reader.app_owner}")
