#!/usr/bin/env python3
"""
NEO Web Data Migration Script
Migrates data from desktop neo_master.db to web neo_web.db
"""

import sqlite3
import os
import shutil

def migrate_data():
    desktop_db = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "neo_master.db")
    web_db = os.path.join(os.path.dirname(os.path.abspath(__file__)), "neo_web.db")

    if not os.path.exists(desktop_db):
        print("Desktop database not found. Skipping migration.")
        return

    print("Copying desktop database to web database...")
    shutil.copy2(desktop_db, web_db)
    print("Migration completed. Web database ready.")

if __name__ == "__main__":
    migrate_data()