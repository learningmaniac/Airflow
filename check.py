import sqlite3
conn = sqlite3.connect('/tmp/crypto.db')
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
print('Tables:', cursor.fetchall())
cursor.execute("SELECT COUNT(*) FROM crypto_snapshots")
print('Row count:', cursor.fetchone())
cursor.execute("SELECT id, extracted_at FROM crypto_snapshots LIMIT 3")
for row in cursor.fetchall():
    print(row)
conn.close()