#!/usr/bin/env python3
"""Check MariaDB for explain plans and Dynatrace DB observability readiness."""

import mysql.connector

HOST = "REDACTED_HOST"
PORT = 3306
USER = "root"
PASSWORD = "REDACTED_PASSWORD"
DATABASE = "dynatrace_demo"

conn = mysql.connector.connect(
    host=HOST, port=PORT, user=USER, password=PASSWORD,
    connection_timeout=30, use_pure=True,
)
cur = conn.cursor(dictionary=True)

print("=" * 60)
print("  MariaDB Explain Plan & Observability Check")
print("=" * 60)

# 1. performance_schema
cur.execute("SHOW VARIABLES LIKE 'performance_schema'")
row = cur.fetchone()
ps_on = row and row.get("Value") == "ON"
print(f"\n[1] performance_schema = {row.get('Value') if row else 'NOT FOUND'}  {'OK' if ps_on else 'PROBLEM'}")

# 2. Check databases
cur.execute("SHOW DATABASES")
dbs = [d["Database"] for d in cur.fetchall()]
print(f"\n[2] Databases: {dbs}")
has_db = DATABASE in dbs
print(f"    dynatrace_demo exists: {has_db}")

# 3. Slow query log settings
for var in ["slow_query_log", "long_query_time", "log_slow_verbosity", "log_output"]:
    cur.execute(f"SHOW VARIABLES LIKE '{var}'")
    r = cur.fetchone()
    print(f"    {var} = {r.get('Value') if r else 'NOT SET'}")

# 4. Statement digest consumers
if ps_on:
    print("\n[3] performance_schema consumers:")
    cur.execute(
        "SELECT NAME, ENABLED FROM performance_schema.setup_consumers "
        "WHERE NAME LIKE 'events_statements%' OR NAME LIKE 'statements_digest'"
    )
    for r in cur.fetchall():
        print(f"    {r['NAME']} = {r['ENABLED']}")

# 5. Check for statement digests
if ps_on:
    print("\n[4] Statement digests in performance_schema:")
    cur.execute(
        "SELECT COUNT(*) AS total FROM performance_schema.events_statements_summary_by_digest"
    )
    total = cur.fetchone()
    print(f"    Total digests: {total['total']}")

    if has_db:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM performance_schema.events_statements_summary_by_digest "
            "WHERE SCHEMA_NAME = %s", (DATABASE,)
        )
        cnt = cur.fetchone()
        print(f"    Digests for '{DATABASE}': {cnt['cnt']}")

        # Show some sample digests
        cur.execute(
            "SELECT SCHEMA_NAME, DIGEST_TEXT, COUNT_STAR, AVG_TIMER_WAIT/1000000000 AS avg_ms "
            "FROM performance_schema.events_statements_summary_by_digest "
            "WHERE SCHEMA_NAME = %s ORDER BY COUNT_STAR DESC LIMIT 10", (DATABASE,)
        )
        rows = cur.fetchall()
        if rows:
            print(f"\n    Top 10 digests for '{DATABASE}':")
            for r in rows:
                text = (r["DIGEST_TEXT"] or "")[:80]
                print(f"      count={r['COUNT_STAR']:>6}  avg={r['avg_ms']:.1f}ms  {text}")
        else:
            print(f"    No digests found for '{DATABASE}' — run the generator first.")

# 6. Check for EXPLAIN plans in slow query log table
print("\n[5] Slow query log (mysql.slow_log table):")
try:
    cur.execute("SELECT COUNT(*) AS cnt FROM mysql.slow_log")
    cnt = cur.fetchone()
    print(f"    Total slow log entries: {cnt['cnt']}")
    if cnt["cnt"] > 0:
        cur.execute(
            "SELECT start_time, db, query_time, sql_text "
            "FROM mysql.slow_log ORDER BY start_time DESC LIMIT 5"
        )
        for r in cur.fetchall():
            sql = (r["sql_text"].decode() if isinstance(r["sql_text"], bytes) else str(r["sql_text"]))[:80]
            print(f"    [{r['start_time']}] db={r['db']}  time={r['query_time']}  {sql}")
except Exception as e:
    print(f"    Could not read slow_log table: {e}")

# 7. Check current statement history for EXPLAIN
if ps_on:
    print("\n[6] Recent statements with EXPLAIN in history_long:")
    try:
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM performance_schema.events_statements_history_long "
            "WHERE SQL_TEXT LIKE 'EXPLAIN%'"
        )
        cnt = cur.fetchone()
        print(f"    EXPLAIN statements in history_long: {cnt['cnt']}")
    except Exception as e:
        print(f"    Could not check history_long: {e}")

# 8. Try running an EXPLAIN ourselves to verify it works
if has_db:
    print("\n[7] Test EXPLAIN on dynatrace_demo:")
    try:
        cur.execute(f"USE `{DATABASE}`")
        cur.execute("SHOW TABLES")
        tables = [list(t.values())[0] for t in cur.fetchall()]
        print(f"    Tables: {tables}")
        if tables:
            test_table = tables[0]
            cur.execute(f"EXPLAIN SELECT * FROM `{test_table}` LIMIT 1")
            plan = cur.fetchall()
            for r in plan:
                print(f"    EXPLAIN: table={r.get('table')} type={r.get('type')} "
                      f"key={r.get('key')} rows={r.get('rows')} Extra={r.get('Extra')}")
            print("    EXPLAIN works!")
    except Exception as e:
        print(f"    EXPLAIN test failed: {e}")

# 9. Check Dynatrace user grants
print("\n[8] Checking for 'dynatrace' user:")
try:
    cur.execute("SELECT User, Host FROM mysql.user WHERE User = 'dynatrace'")
    users = cur.fetchall()
    if users:
        for u in users:
            print(f"    Found: {u['User']}@{u['Host']}")
            cur.execute(f"SHOW GRANTS FOR '{u['User']}'@'{u['Host']}'")
            grants = cur.fetchall()
            for g in grants:
                print(f"      {list(g.values())[0]}")
    else:
        print("    No 'dynatrace' user found — Dynatrace extension needs this user!")
        print("    Run:")
        print("      CREATE USER 'dynatrace'@'%' IDENTIFIED BY '<password>';")
        print("      GRANT SELECT ON performance_schema.* TO 'dynatrace'@'%';")
        print("      GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'dynatrace'@'%';")
        print(f"      GRANT SELECT ON {DATABASE}.* TO 'dynatrace'@'%';")
except Exception as e:
    print(f"    Could not check users: {e}")

cur.close()
conn.close()

print("\n" + "=" * 60)
print("  Done. Review output above for issues.")
print("=" * 60)
