# coding=utf-8
import os
import subprocess
import sys
import time

def run():
    return subprocess.Popen('python3 /home/gzcyy/hive_load/load_4g_source.py',shell=True)
    
if __name__ == "__main__":
    sub_p = run()
    while True:
        if sub_p.wait() and sub_p.poll() != 0:
            sub_p = run()
        time.sleep(10)
