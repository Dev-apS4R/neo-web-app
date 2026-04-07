#!/usr/bin/env python3
"""
Script to delete all users from the NEO web database
"""

from neo_vault import NeoVault

vault = NeoVault()
vault.conn.execute("DELETE FROM users")
vault.conn.commit()
print("All users deleted from the web database.")
