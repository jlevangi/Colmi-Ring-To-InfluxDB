import os
import shutil
import sqlite3
import sys
import tempfile

def fetch_database():
    '''Fetch the database from the local path'''
    LOCAL_PATH = os.getenv("LOCAL_PATH")
    if not os.path.exists(LOCAL_PATH):
        print("Error: Export file does not exist")
        sys.exit(1)
        
    tempdir = tempfile.mkdtemp()
    shutil.copy(LOCAL_PATH, f'{tempdir}/gadgetbridge.sqlite')
    return tempdir

def open_database(tempdir):
    '''Open a handle on the database'''
    conn = sqlite3.connect(f"{tempdir}/gadgetbridge.sqlite")
    cur = conn.cursor()
    return conn, cur
