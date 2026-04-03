#!/usr/bin/env python3
"""Check failed explain queue entries."""
import mysql.connector

conn = mysql.connector.connect(
    host="REDACTED_HOST",
    port=3306, user="root", password="REDACTED_PASSWORD",
    connection_timeout=30, use_pure=True,
)
cur = conn.cursor(dictionary=True)
cur.execute("USE dt_monitoring")

print("=== FAILED explain queue entries ===")
cur.execute(
    "SELECT id, LEFT(query_text, 150) as sql_text, status, error_message "
    "FROM dt_explain_queue WHERE status = 'FAILED' ORDER BY id DESC"
)
for r in cur.fetchall():
    print(f"id={r['id']} status={r['status']}")
    print(f"  sql: {r['sql_text']}")
    print(f"  err: {r['error_message']}")
    print()

print("=== COMPLETED explain queue entries ===")
cur.execute(
    "SELECT id, LEFT(query_text, 150) as sql_text, status "
    "FROM dt_explain_queue WHERE status = 'COMPLETED' ORDER BY id DESC"
)
for r in cur.fetchall():
    print(f"id={r['id']} sql: {r['sql_text']}")

print("\n=== SKIPPED sample (first 5) ===")
cur.execute(
    "SELECT id, LEFT(query_text, 150) as sql_text, error_message "
    "FROM dt_explain_queue WHERE status = 'SKIPPED' ORDER BY id DESC LIMIT 5"
)
for r in cur.fetchall():
    print(f"id={r['id']} reason={r['error_message']}")
    print(f"  sql: {r['sql_text']}")

cur.close()
conn.close()
