#!/usr/bin/env python3
"""Check explain plan pipeline on MariaDB."""
import os
import mysql.connector

conn = mysql.connector.connect(
    host=os.environ["MARIADB_HOST"],
    port=int(os.environ.get("MARIADB_PORT", 3306)),
    user=os.environ.get("MARIADB_USER", "root"),
    password=os.environ["MARIADB_PASSWORD"],
    connection_timeout=30, use_pure=True,
)
cur = conn.cursor(dictionary=True)

# Check dt_monitoring database
cur.execute("USE dt_monitoring")

print("=== dt_explain_queue status ===")
cur.execute("SELECT status, COUNT(*) as cnt FROM dt_explain_queue GROUP BY status")
for r in cur.fetchall():
    print(f"  {r['status']}: {r['cnt']}")

print("\n=== Recent errors in dt_explain_queue ===")
cur.execute(
    "SELECT id, query_digest, status, error_message, processed_at "
    "FROM dt_explain_queue WHERE status != 'completed' "
    "ORDER BY processed_at DESC LIMIT 10"
)
rows = cur.fetchall()
if rows:
    for r in rows:
        err = str(r.get("error_message", ""))[:120]
        print(f"  id={r['id']} status={r['status']} error={err}")
else:
    print("  No errors found")

print("\n=== dt_mariadb_explain_plans ===")
cur.execute("SELECT COUNT(*) as cnt FROM dt_mariadb_explain_plans")
cnt = cur.fetchone()
print(f"  Total explain plans stored: {cnt['cnt']}")

if cnt["cnt"] > 0:
    cur.execute(
        "SELECT database_name, table_access_type, key_used, rows_estimated, "
        "LEFT(query_text, 80) as query_snippet "
        "FROM dt_mariadb_explain_plans LIMIT 5"
    )
    for r in cur.fetchall():
        print(f"  db={r['database_name']} type={r['table_access_type']} "
              f"key={r['key_used']} rows={r['rows_estimated']} "
              f"sql={r['query_snippet']}")

print("\n=== Explain plan JSON sample ===")
if cnt["cnt"] > 0:
    cur.execute("SELECT explain_plan_json FROM dt_mariadb_explain_plans LIMIT 1")
    row = cur.fetchone()
    if row:
        print(f"  {str(row['explain_plan_json'])[:500]}")

# Check the stored procedure
print("\n=== dynatrace.dynatrace_execution_plan procedure ===")
try:
    cur.execute("SHOW PROCEDURE STATUS WHERE Db='dynatrace' AND Name='dynatrace_execution_plan'")
    procs = cur.fetchall()
    if procs:
        for p in procs:
            print(f"  Found: {p['Name']} definer={p['Definer']}")
    else:
        print("  NOT FOUND!")
except Exception as e:
    print(f"  Error: {e}")

# Check what the extension user can do
print("\n=== Extension config endpoint check ===")
cur.execute("SELECT CURRENT_USER()")
print(f"  Connected as: {cur.fetchone()}")

# Check if performance_schema has explain-related data
print("\n=== performance_schema: EXPLAIN statements ===")
cur.execute(
    "SELECT DIGEST_TEXT, COUNT_STAR, AVG_TIMER_WAIT/1000000000 as avg_ms "
    "FROM performance_schema.events_statements_summary_by_digest "
    "WHERE DIGEST_TEXT LIKE 'EXPLAIN%' ORDER BY COUNT_STAR DESC LIMIT 10"
)
rows = cur.fetchall()
if rows:
    for r in rows:
        text = (r["DIGEST_TEXT"] or "")[:100]
        print(f"  count={r['COUNT_STAR']} avg={r['avg_ms']:.1f}ms {text}")
else:
    print("  No EXPLAIN digests found")

cur.close()
conn.close()
print("\nDone.")
