import platform
import sys

print(f"_python_{platform.python_version()}\t1")
for line in sys.stdin:
    for word in line.split():
        print(f"{word.lower()}\t1")
