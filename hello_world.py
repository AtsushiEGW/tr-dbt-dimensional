#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import platform
import sys
import os
import shutil
import getpass
from datetime import datetime

def main():
    print("=== System Information (hello_world.py) ===")
    print(f"Execution Time        : {datetime.now()}")
    print()

    # ----- OS情報 -----
    print("[ OS Information ]")
    print(f"OS Name               : {platform.system()}")
    print(f"OS Release            : {platform.release()}")
    print(f"OS Version            : {platform.version()}")
    print(f"Platform Detail       : {platform.platform()}")
    print(f"Machine Architecture   : {platform.machine()}")
    print(f"Processor             : {platform.processor()}")
    print()

    # ----- CPU情報 -----
    print("[ CPU Information ]")
    print(f"CPU Cores (logical)   : {os.cpu_count()}")
    print()

    # ----- Python情報 -----
    print("[ Python Information ]")
    print(f"Python Version        : {sys.version}")
    print(f"Python Executable Path: {sys.executable}")
    print()

    # ----- ユーザー & 環境情報 -----
    print("[ User / Environment ]")
    print(f"Current User          : {getpass.getuser()}")
    print(f"Current Working Dir   : {os.getcwd()}")
    print(f"Home Directory        : {os.path.expanduser('~')}")
    print()

    # ----- Disk情報 -----
    print("[ Disk Information ]")
    try:
        total, used, free = shutil.disk_usage("/")
        print(f"Disk Total (GB)       : {total / (1024 ** 3):.2f}")
        print(f"Disk Used  (GB)       : {used / (1024 ** 3):.2f}")
        print(f"Disk Free  (GB)       : {free / (1024 ** 3):.2f}")
    except Exception as e:
        print(f"Disk info error: {e}")
    print()

    # ----- 環境変数の一部 -----
    print("[ Sample Environment Variables ]")
    for key in ["PATH", "SHELL", "LANG", "HOME"]:
        print(f"{key:10}: {os.environ.get(key, '-')}")
    print()

    print("=== End ===")

if __name__ == "__main__":
    main()