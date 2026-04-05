#!/usr/bin/env python3
"""
MariaDB Test Data Generator for Dynatrace Database Observability Demo
======================================================================
Simulates real-world database scenarios that surface in Dynatrace:
  - Slow queries / full table scans (missing indexes)
  - Query execution plan regressions
  - Lock contention / deadlocks
  - N+1 query patterns
  - High-frequency small queries
  - Batch inserts vs row-by-row inserts
  - Temp table usage and filesorts
  - Connection pool exhaustion patterns
  - Mix of OLTP + reporting (heavy aggregations)
  - Deadlock generation with retry detection
  - Non-sargable queries (function on indexed column)
  - Implicit type conversion defeating indexes
  - Cartesian joins / missing JOIN conditions
  - Unbounded SELECTs (missing LIMIT)
  - Metadata lock / DDL blocking DML
  - Query pattern drift (bad deployment simulation)
  - Complex explain plans (subqueries, 5+ joins, derived tables, UNIONs, HAVING)
  - Heavy lock contention (SHARE locks, gap locks, lock wait timeouts, rapid cycling)

Usage:
    pip install mysql-connector-python faker
    python mariadb_dynatrace_generator.py --host 127.0.0.1 --user root --password secret

    # Or run a specific scenario only:
    python mariadb_dynatrace_generator.py --scenario slow_queries
"""

import argparse
import random
import time
import threading
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import mysql.connector
    from mysql.connector import Error
except ImportError:
    print("Install required: pip install mysql-connector-python")
    exit(1)

try:
    from faker import Faker
except ImportError:
    print("Install required: pip install faker")
    exit(1)

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dt-generator")

fake = Faker()

# ─── DB helpers ───────────────────────────────────────────────────────────────

def get_connection(cfg):
    conn = mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        connection_timeout=30,
        autocommit=False,
        use_pure=True,   # Text protocol — ensures statement text in performance_schema
    )
    # Per-session: lower slow-query threshold so demo queries are captured
    cur = conn.cursor()
    try:
        cur.execute("SET SESSION long_query_time = 0.3")
    except Error:
        pass
    cur.close()
    return conn

def execute(conn, sql, params=None, fetch=False, many=False):
    cur = conn.cursor(dictionary=True)
    try:
        if many:
            cur.executemany(sql, params or [])
        else:
            log.debug(f"SQL: {sql.strip()[:120]}")
            cur.execute(sql, params or ())
        if fetch:
            return cur.fetchall()
        conn.commit()
    except Error as e:
        conn.rollback()
        log.warning(f"SQL error: {e}")
        return []
    finally:
        cur.close()
    return []

def explain_query(conn, sql, params=None):
    """Run EXPLAIN on a query and log the execution plan.
    This makes explain plans visible to Dynatrace DB monitoring."""
    cur = conn.cursor(dictionary=True)
    try:
        explain_sql = f"EXPLAIN {sql}"
        cur.execute(explain_sql, params or ())
        plan = cur.fetchall()
        for row in plan:
            log.info(f"  EXPLAIN: table={row.get('table')} "
                     f"type={row.get('type')} "
                     f"key={row.get('key')} "
                     f"rows={row.get('rows')} "
                     f"Extra={row.get('Extra')}")
        return plan
    except Error as e:
        log.debug(f"EXPLAIN error: {e}")
        return []
    finally:
        cur.close()

# =============================================================================
# DYNATRACE MONITORING SETUP
# =============================================================================

def setup_dynatrace_monitoring(cfg):
    """
    Configure MariaDB for Dynatrace Database Observability.
    Ensures SQL statements appear in performance_schema and execution plans
    are logged via slow query log with MariaDB's explain verbosity.
    """
    log.info("=" * 60)
    log.info("  Configuring MariaDB for Dynatrace Observability")
    log.info("=" * 60)

    conn = mysql.connector.connect(
        host=cfg.host, port=cfg.port,
        user=cfg.user, password=cfg.password,
        connection_timeout=30, use_pure=True,
    )
    cur = conn.cursor(dictionary=True)
    issues = []

    # ── 1. Check performance_schema (server startup variable) ────────────
    cur.execute("SHOW VARIABLES LIKE 'performance_schema'")
    row = cur.fetchone()
    ps_on = row and row.get('Value', 'OFF') == 'ON'
    if not ps_on:
        issues.append("performance_schema is OFF")
        log.error("✗ performance_schema is OFF!")
        log.error("  Dynatrace CANNOT capture SQL statements or execution plans.")
        log.error("  Fix: add to [mysqld] in my.cnf / server config and restart:")
        log.error("    performance_schema = ON")
    else:
        log.info("  ✓ performance_schema = ON")

    # ── 2. Enable statement consumers + instruments (dynamic) ───────────
    if ps_on:
        consumers = [
            'events_statements_current',
            'events_statements_history',
            'events_statements_history_long',
            'statements_digest',
        ]
        for name in consumers:
            try:
                cur.execute(
                    "UPDATE performance_schema.setup_consumers "
                    "SET ENABLED='YES' WHERE NAME=%s", (name,)
                )
                conn.commit()
            except Error as e:
                log.warning(f"  Could not enable consumer '{name}': {e}")
        log.info("  ✓ Statement digest consumers enabled")

        try:
            cur.execute(
                "UPDATE performance_schema.setup_instruments "
                "SET ENABLED='YES', TIMED='YES' WHERE NAME LIKE 'statement/%%'"
            )
            conn.commit()
            log.info("  ✓ Statement instruments enabled with timing")
        except Error as e:
            log.warning(f"  Could not enable statement instruments: {e}")

    # ── 3. Slow query log with execution plans (MariaDB-specific) ───────
    try:
        cur.execute("SET GLOBAL slow_query_log = 1")
        conn.commit()
        cur.execute("SET GLOBAL long_query_time = 0.3")
        conn.commit()
        log.info("  ✓ Slow query log ON (threshold: 0.3s)")
    except Error as e:
        log.warning(f"  Could not enable slow query log: {e}")

    # MariaDB-specific: log_slow_verbosity includes EXPLAIN output in slow log
    try:
        cur.execute("SET GLOBAL log_slow_verbosity = 'query_plan,explain'")
        conn.commit()
        log.info("  ✓ log_slow_verbosity = 'query_plan,explain' (execution plans in slow log)")
    except Error as e:
        log.warning(f"  Could not set log_slow_verbosity: {e}")

    # ── 4. Log output to TABLE + FILE for Dynatrace access ────────────
    try:
        cur.execute("SET GLOBAL log_output = 'TABLE,FILE'")
        conn.commit()
        log.info("  ✓ log_output = TABLE,FILE")
    except Error as e:
        log.warning(f"  Could not set log_output: {e}")

    # ── 5. Enable userstat for per-user / per-table metrics ─────────────
    try:
        cur.execute("SET GLOBAL userstat = 1")
        conn.commit()
        log.info("  ✓ userstat enabled")
    except Error as e:
        log.debug(f"  userstat not available: {e}")

    # ── 6. Reset digest table for clean capture ─────────────────────────
    if ps_on:
        try:
            cur.execute(
                "TRUNCATE performance_schema.events_statements_summary_by_digest"
            )
            conn.commit()
            log.info("  ✓ Statement digest table reset for fresh capture")
        except Error as e:
            log.debug(f"  Could not reset digest table: {e}")

    # ── 7. Verify statement capture works ───────────────────────────────
    if ps_on:
        try:
            cur.execute("USE `%s`" % cfg.database.replace('`', '``'))
            cur.execute("SELECT 1 AS dynatrace_test")
            cur.fetchall()
            cur.execute(
                "SELECT COUNT(*) AS cnt "
                "FROM performance_schema.events_statements_summary_by_digest "
                "WHERE SCHEMA_NAME = %s", (cfg.database,)
            )
            check = cur.fetchone()
            if check and check.get('cnt', 0) > 0:
                log.info("  ✓ Statement capture verified — digests being collected")
            else:
                log.warning("  ⚠ No digests found yet (will populate when scenarios run)")
        except Error as e:
            log.warning(f"  Could not verify capture: {e}")

    cur.close()
    conn.close()

    # ── Print Dynatrace monitoring user SQL ─────────────────────────────
    log.info("")
    log.info("  Dynatrace monitoring user — run these grants if not done:")
    log.info("    CREATE USER IF NOT EXISTS 'dynatrace'@'%%' IDENTIFIED BY '<password>';")
    log.info("    GRANT SELECT ON performance_schema.* TO 'dynatrace'@'%%';")
    log.info("    GRANT PROCESS, REPLICATION CLIENT ON *.* TO 'dynatrace'@'%%';")
    log.info(f"    GRANT SELECT ON `{cfg.database}`.* TO 'dynatrace'@'%%';  -- for EXPLAIN")
    log.info("    FLUSH PRIVILEGES;")

    if issues:
        log.warning("─" * 60)
        log.warning("  ⚠ Issues that BLOCK Dynatrace visibility:")
        for issue in issues:
            log.warning(f"    • {issue}")
        log.warning("─" * 60)
    else:
        log.info("  ✓ MariaDB is ready for Dynatrace Database Observability")
    log.info("=" * 60)


# =============================================================================
# SCHEMA SETUP
# =============================================================================

SCHEMA_STATEMENTS = [
    """CREATE TABLE IF NOT EXISTS customers (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        email       VARCHAR(255) NOT NULL,
        first_name  VARCHAR(100),
        last_name   VARCHAR(100),
        country     VARCHAR(50),
        tier        ENUM('bronze','silver','gold','platinum') DEFAULT 'bronze',
        created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
        updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS products (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        sku         VARCHAR(50) NOT NULL,
        name        VARCHAR(255),
        category    VARCHAR(100),
        price       DECIMAL(10,2),
        stock_qty   INT DEFAULT 0,
        created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS orders (
        id            INT AUTO_INCREMENT PRIMARY KEY,
        customer_id   INT NOT NULL,
        status        ENUM('pending','processing','shipped','delivered','cancelled') DEFAULT 'pending',
        total_amount  DECIMAL(12,2),
        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
        shipped_at    DATETIME
    )""",
    """CREATE TABLE IF NOT EXISTS order_items (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        order_id    INT NOT NULL,
        product_id  INT NOT NULL,
        quantity    INT,
        unit_price  DECIMAL(10,2)
    )""",
    """CREATE TABLE IF NOT EXISTS events (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        event_type  VARCHAR(50),
        entity_type VARCHAR(50),
        entity_id   INT,
        payload     TEXT,
        created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS sessions (
        id          VARCHAR(64) PRIMARY KEY,
        customer_id INT,
        ip_address  VARCHAR(45),
        user_agent  TEXT,
        last_active DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
    )""",
    """CREATE TABLE IF NOT EXISTS sales_facts (
        id          BIGINT AUTO_INCREMENT PRIMARY KEY,
        order_id    INT,
        product_id  INT,
        customer_id INT,
        category    VARCHAR(100),
        country     VARCHAR(50),
        tier        VARCHAR(20),
        revenue     DECIMAL(12,2),
        quantity    INT,
        sale_date   DATE,
        sale_month  TINYINT,
        sale_year   SMALLINT
    )""",
]

def setup_schema(cfg):
    log.info("Setting up schema...")
    conn = get_connection(cfg)
    cur = conn.cursor()
    for stmt in SCHEMA_STATEMENTS:
        try:
            cur.execute(stmt)
            conn.commit()
        except Error as e:
            log.warning(f"Schema warning: {e}")
    cur.close()
    conn.close()
    log.info("Schema ready.")

# =============================================================================
# SEED DATA
# =============================================================================

def seed_customers(conn, n=2000):
    log.info(f"Seeding {n} customers...")
    tiers = ['bronze'] * 50 + ['silver'] * 30 + ['gold'] * 15 + ['platinum'] * 5
    rows = []
    for _ in range(n):
        rows.append((
            fake.email(), fake.first_name(), fake.last_name(),
            fake.country_code(), random.choice(tiers),
            fake.date_time_between(start_date="-2y", end_date="now"),
        ))
    execute(conn,
        "INSERT INTO customers (email,first_name,last_name,country,tier,created_at) VALUES (%s,%s,%s,%s,%s,%s)",
        rows, many=True)
    log.info("Customers seeded.")

def seed_products(conn, n=500):
    log.info(f"Seeding {n} products...")
    cats = ['Electronics','Clothing','Books','Home','Sports','Food','Toys','Beauty']
    rows = []
    for i in range(n):
        rows.append((
            f"SKU-{i:05d}", fake.catch_phrase()[:80],
            random.choice(cats),
            round(random.uniform(1.99, 999.99), 2),
            random.randint(0, 5000),
        ))
    execute(conn,
        "INSERT INTO products (sku,name,category,price,stock_qty) VALUES (%s,%s,%s,%s,%s)",
        rows, many=True)
    log.info("Products seeded.")

def seed_orders(conn, n=5000):
    log.info(f"Seeding {n} orders + items...")
    statuses = ['pending','processing','shipped','delivered','cancelled']
    cur = conn.cursor()
    cur.execute("SELECT id FROM customers LIMIT 500")
    cust_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id, price FROM products LIMIT 200")
    prod_rows = cur.fetchall()
    cur.close()

    if not cust_ids or not prod_rows:
        log.warning("No customers/products found, skipping orders.")
        return

    BATCH_SIZE = 100
    order_batch = []
    for i in range(n):
        cid = random.choice(cust_ids)
        status = random.choice(statuses)
        created = fake.date_time_between(start_date="-1y", end_date="now")
        order_batch.append((cid, status, created))

        if len(order_batch) >= BATCH_SIZE or i == n - 1:
            cur2 = conn.cursor()
            try:
                # Insert orders in batch
                cur2.executemany(
                    "INSERT INTO orders (customer_id,status,total_amount,created_at) VALUES (%s,%s,0,%s)",
                    order_batch
                )
                first_id = cur2.lastrowid
                # Generate items for each order in this batch
                item_rows = []
                update_rows = []
                for j, (cid_b, st_b, cr_b) in enumerate(order_batch):
                    oid = first_id + j
                    items = random.sample(prod_rows, min(random.randint(1, 5), len(prod_rows)))
                    total = 0
                    for pid, price in items:
                        qty = random.randint(1, 10)
                        total += float(price) * qty
                        item_rows.append((oid, pid, qty, price))
                    update_rows.append((total, oid))
                # Batch insert items
                cur2.executemany(
                    "INSERT INTO order_items (order_id,product_id,quantity,unit_price) VALUES (%s,%s,%s,%s)",
                    item_rows
                )
                # Batch update totals
                for total, oid in update_rows:
                    cur2.execute("UPDATE orders SET total_amount=%s WHERE id=%s", (total, oid))
                conn.commit()
            except Error as e:
                conn.rollback()
                log.debug(f"Order batch error: {e}")
            finally:
                cur2.close()
            order_batch = []
            if (i + 1) % 500 == 0 or i == n - 1:
                log.info(f"  {i + 1}/{n} orders...")
    log.info("Orders seeded.")

def seed_sales_facts(conn):
    log.info("Populating sales_facts...")
    execute(conn, """
        INSERT INTO sales_facts (order_id, product_id, customer_id, category, country, tier,
                                  revenue, quantity, sale_date, sale_month, sale_year)
        SELECT oi.order_id, oi.product_id, o.customer_id,
               p.category, c.country, c.tier,
               oi.quantity * oi.unit_price,
               oi.quantity,
               DATE(o.created_at), MONTH(o.created_at), YEAR(o.created_at)
        FROM order_items oi
        JOIN orders o    ON o.id = oi.order_id
        JOIN products p  ON p.id = oi.product_id
        JOIN customers c ON c.id = o.customer_id
    """)
    log.info("sales_facts populated.")

# =============================================================================
# DATA CLEANUP — Remove data older than N days
# =============================================================================

def cleanup_old_data(cfg, days=10):
    """
    Remove data older than `days` days to prevent unbounded table growth.
    Order of deletion matters due to implicit relationships.
    """
    log.info(f"▶ CLEANUP: Removing data older than {days} days...")
    conn = get_connection(cfg)
    cutoff = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff.strftime('%Y-%m-%d %H:%M:%S')
    cutoff_date = cutoff.strftime('%Y-%m-%d')

    try:
        # 1. Delete old events (high-volume, safe to delete)
        cur = conn.cursor()
        cur.execute("DELETE FROM events WHERE created_at < %s", (cutoff_str,))
        events_deleted = cur.rowcount
        conn.commit()
        log.info(f"  Deleted {events_deleted} events")

        # 2. Delete old sessions
        cur.execute("DELETE FROM sessions WHERE created_at < %s", (cutoff_str,))
        sessions_deleted = cur.rowcount
        conn.commit()
        log.info(f"  Deleted {sessions_deleted} sessions")

        # 3. Delete old sales_facts
        cur.execute("DELETE FROM sales_facts WHERE sale_date < %s", (cutoff_date,))
        facts_deleted = cur.rowcount
        conn.commit()
        log.info(f"  Deleted {facts_deleted} sales_facts rows")

        # 4. Get old order IDs before deleting
        cur.execute("SELECT id FROM orders WHERE created_at < %s", (cutoff_str,))
        old_order_ids = [r[0] for r in cur.fetchall()]

        # 5. Delete order_items for old orders
        if old_order_ids:
            placeholders = ','.join(['%s'] * len(old_order_ids))
            cur.execute(f"DELETE FROM order_items WHERE order_id IN ({placeholders})", old_order_ids)
            items_deleted = cur.rowcount
            conn.commit()
            log.info(f"  Deleted {items_deleted} order_items")

            # 6. Delete old orders
            cur.execute(f"DELETE FROM orders WHERE id IN ({placeholders})", old_order_ids)
            orders_deleted = cur.rowcount
            conn.commit()
            log.info(f"  Deleted {orders_deleted} orders")
        else:
            log.info("  No old orders to delete")

        # 7. Delete old customers (only those with no orders)
        cur.execute("""
            DELETE c FROM customers c
            LEFT JOIN orders o ON o.customer_id = c.id
            WHERE c.created_at < %s AND o.id IS NULL
        """, (cutoff_str,))
        customers_deleted = cur.rowcount
        conn.commit()
        log.info(f"  Deleted {customers_deleted} orphaned customers")

        # 8. Delete old products with no order_items
        cur.execute("""
            DELETE p FROM products p
            LEFT JOIN order_items oi ON oi.product_id = p.id
            WHERE p.created_at < %s AND oi.id IS NULL
        """, (cutoff_str,))
        products_deleted = cur.rowcount
        conn.commit()
        log.info(f"  Deleted {products_deleted} orphaned products")

        cur.close()

    except Error as e:
        conn.rollback()
        log.error(f"Cleanup error: {e}")
    finally:
        conn.close()

    log.info("✔ Cleanup complete.")

# =============================================================================
# SCENARIO 1 — Full Table Scan (no index on filter columns)
# Dynatrace: high logical reads, type=ALL in EXPLAIN, slow query log entries
# =============================================================================

def scenario_full_table_scan(cfg, iterations=50):
    log.info("▶ SCENARIO 1: Full table scan (no index on country/tier)")
    conn = get_connection(cfg)
    countries = ['US','GB','DE','IN','FR','JP','CA','AU','BR','MX']

    # Run EXPLAIN first to capture execution plans in Dynatrace
    log.info("  Running EXPLAIN on full-table-scan queries...")
    explain_query(conn, "SELECT * FROM customers WHERE country = %s AND tier = %s",
                  ('US', 'gold'))
    explain_query(conn, "SELECT * FROM customers WHERE tier = %s ORDER BY last_name LIMIT 100",
                  ('gold',))
    explain_query(conn, "SELECT * FROM customers WHERE last_name LIKE %s", ('%Smith%',))

    for i in range(iterations):
        country = random.choice(countries)
        tier = random.choice(['gold','platinum'])
        execute(conn, "SELECT * FROM customers WHERE country = %s AND tier = %s",
                (country, tier), fetch=True)
        execute(conn,
            "SELECT * FROM customers WHERE tier = %s ORDER BY last_name LIMIT 100",
            (tier,), fetch=True)
        # LIKE scan — can't use index prefix
        kw = random.choice(['Smith','Johnson','Williams','Brown','Jones'])
        execute(conn, "SELECT * FROM customers WHERE last_name LIKE %s", (f'%{kw}%',), fetch=True)
        # Periodic EXPLAIN to keep plans visible in Dynatrace
        if i % 10 == 0:
            explain_query(conn, "SELECT * FROM customers WHERE country = %s AND tier = %s",
                          (country, tier))
        time.sleep(random.uniform(0.05, 0.15))
    conn.close()
    log.info("✔ Scenario 1 done.")

# =============================================================================
# SCENARIO 2 — N+1 Query Pattern
# Dynatrace: burst of identical short queries, high QPS per transaction
# =============================================================================

def scenario_n_plus_1(cfg, iterations=15):
    log.info("▶ SCENARIO 2: N+1 query pattern")
    conn = get_connection(cfg)

    # EXPLAIN the N+1 pattern queries
    log.info("  Running EXPLAIN on N+1 queries...")
    explain_query(conn, "SELECT id, customer_id FROM orders LIMIT 50")
    explain_query(conn, "SELECT * FROM customers WHERE id = %s", (1,))
    explain_query(conn,
        "SELECT oi.*, p.name FROM order_items oi JOIN products p ON p.id=oi.product_id WHERE oi.order_id = %s",
        (1,))

    for _ in range(iterations):
        orders = execute(conn, "SELECT id, customer_id FROM orders LIMIT 50", fetch=True) or []
        for order in orders:
            execute(conn, "SELECT * FROM customers WHERE id = %s",
                    (order['customer_id'],), fetch=True)
            execute(conn,
                "SELECT oi.*, p.name FROM order_items oi JOIN products p ON p.id=oi.product_id WHERE oi.order_id = %s",
                (order['id'],), fetch=True)
        time.sleep(random.uniform(0.1, 0.3))
    conn.close()
    log.info("✔ Scenario 2 done.")

# =============================================================================
# SCENARIO 3 — Heavy Aggregation / Reporting
# Dynatrace: long query duration, Using temporary + Using filesort in EXPLAIN
# =============================================================================

def scenario_heavy_aggregation(cfg, iterations=10):
    log.info("▶ SCENARIO 3: Heavy aggregation / reporting queries")
    conn = get_connection(cfg)

    agg_sql = """
        SELECT category, country, tier,
               SUM(revenue) as total_revenue,
               COUNT(DISTINCT customer_id) as unique_customers,
               AVG(revenue) as avg_revenue,
               sale_year, sale_month
        FROM sales_facts
        GROUP BY category, country, tier, sale_year, sale_month
        ORDER BY total_revenue DESC
        LIMIT 200
    """
    corr_sql = """
        SELECT o.id, o.customer_id, o.total_amount,
               (SELECT SUM(o2.total_amount) FROM orders o2
                WHERE o2.customer_id = o.customer_id AND o2.id <= o.id) AS running_total
        FROM orders o
        ORDER BY o.customer_id, o.id
        LIMIT 80
    """

    # EXPLAIN heavy queries so plans appear in Dynatrace
    log.info("  Running EXPLAIN on aggregation queries...")
    explain_query(conn, agg_sql)
    explain_query(conn, corr_sql)

    for _ in range(iterations):
        execute(conn, agg_sql, fetch=True)
        # Correlated subquery — intentionally slow
        execute(conn, corr_sql, fetch=True)
        time.sleep(random.uniform(0.5, 1.2))
    conn.close()
    log.info("✔ Scenario 3 done.")

# =============================================================================
# SCENARIO 4 — Lock Contention
# Dynatrace: lock wait time metrics, transaction wait events
# =============================================================================

def _lock_worker(cfg, order_ids, worker_id, hold_min=1.0, hold_max=3.0):
    conn = get_connection(cfg)
    try:
        for oid in order_ids:
            try:
                conn.start_transaction()
                cur = conn.cursor()
                cur.execute("SELECT id, total_amount, status FROM orders WHERE id = %s FOR UPDATE", (oid,))
                row = cur.fetchone()
                if row:
                    time.sleep(random.uniform(hold_min, hold_max))  # hold lock long enough to cause waits
                    cur.execute(
                        "UPDATE orders SET status='processing', total_amount=%s WHERE id=%s",
                        (float(row[1]) * 1.01, oid)
                    )
                conn.commit()
                cur.close()
            except Error as e:
                conn.rollback()
                log.debug(f"Lock worker {worker_id} rollback: {e}")
    finally:
        conn.close()

def scenario_lock_contention(cfg, iterations=5):
    log.info("▶ SCENARIO 4: Lock contention (6 workers, 1-3s hold, overlapping rows)")
    conn = get_connection(cfg)
    rows = execute(conn, "SELECT id FROM orders LIMIT 50", fetch=True)
    conn.close()
    if not rows:
        log.warning("No orders found, skipping.")
        return
    ids = [r['id'] for r in rows]
    for rnd in range(iterations):
        # 6 workers with heavily overlapping row ranges — guarantees lock waits
        workers = [
            threading.Thread(target=_lock_worker, args=(cfg, ids[:20], 1)),
            threading.Thread(target=_lock_worker, args=(cfg, ids[5:25], 2)),
            threading.Thread(target=_lock_worker, args=(cfg, ids[10:30], 3)),
            threading.Thread(target=_lock_worker, args=(cfg, ids[15:35], 4)),
            threading.Thread(target=_lock_worker, args=(cfg, ids[20:40], 5)),
            threading.Thread(target=_lock_worker, args=(cfg, ids[25:45], 6)),
        ]
        for w in workers: w.start()
        for w in workers: w.join(timeout=60)
        log.info(f"  Round {rnd + 1}/{iterations} complete")
        time.sleep(0.5)
    log.info("✔ Scenario 4 done.")

# =============================================================================
# SCENARIO 5 — High-Frequency Small Queries (connection churn)
# Dynatrace: high connection count, many short transactions
# =============================================================================

def _hf_worker(cfg, count):
    for _ in range(count):
        conn = get_connection(cfg)
        execute(conn, "SELECT 1")
        execute(conn, "SELECT id, email, tier FROM customers WHERE id = %s",
                (random.randint(1, 2000),), fetch=True)
        execute(conn,
            "INSERT INTO sessions (id, customer_id, ip_address) VALUES (%s,%s,%s)",
            (fake.uuid4()[:64], random.randint(1, 2000), fake.ipv4()))
        conn.close()
        time.sleep(random.uniform(0.01, 0.05))

def scenario_high_frequency(cfg, threads=10, per_thread=30):
    log.info(f"▶ SCENARIO 5: High-frequency queries ({threads} threads × {per_thread})")
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(_hf_worker, cfg, per_thread) for _ in range(threads)]
        for f in as_completed(futs): f.result()
    log.info("✔ Scenario 5 done.")

# =============================================================================
# SCENARIO 6 — Row-by-row vs Bulk INSERT comparison
# Dynatrace: write latency, autocommit overhead clearly visible
# =============================================================================

def scenario_slow_inserts(cfg, n=300):
    log.info(f"▶ SCENARIO 6: Row-by-row INSERT vs bulk INSERT ({n} rows each)")
    conn = get_connection(cfg)
    execute(conn, "SET autocommit=1")
    t0 = time.time()
    cur = conn.cursor()
    for _ in range(n):
        cur.execute(
            "INSERT INTO events (event_type, entity_type, entity_id, payload) VALUES (%s,%s,%s,%s)",
            (
                random.choice(['view','click','purchase','search','logout']),
                random.choice(['customer','product','order']),
                random.randint(1, 5000),
                f'{{"session":"{fake.uuid4()[:32]}","ts":{int(time.time())}}}',
            )
        )
        time.sleep(0.003)
    cur.close()
    slow_dur = time.time() - t0

    conn2 = get_connection(cfg)
    t1 = time.time()
    rows = [
        (
            random.choice(['view','click','purchase','search','logout']),
            random.choice(['customer','product','order']),
            random.randint(1, 5000),
            f'{{"session":"{fake.uuid4()[:32]}","ts":{int(time.time())}}}',
        ) for _ in range(n)
    ]
    execute(conn2,
        "INSERT INTO events (event_type, entity_type, entity_id, payload) VALUES (%s,%s,%s,%s)",
        rows, many=True)
    fast_dur = time.time() - t1
    conn.close()
    conn2.close()
    log.info(f"  Row-by-row: {slow_dur:.1f}s | Bulk: {fast_dur:.2f}s | Speedup: {slow_dur/max(fast_dur,0.001):.0f}x")
    log.info("✔ Scenario 6 done.")

# =============================================================================
# SCENARIO 7 — Index Added Mid-Run (execution plan change)
# Dynatrace: query plan regression/improvement event, sudden latency drop
# =============================================================================

def scenario_index_change(cfg, iterations_before=20, iterations_after=20):
    log.info("▶ SCENARIO 7: Execution plan change — index added mid-run")
    conn = get_connection(cfg)
    query = "SELECT id, email FROM customers WHERE country='US' AND tier='gold'"
    try:
        execute(conn, "ALTER TABLE customers DROP INDEX idx_demo_country_tier")
    except: pass

    log.info("  Phase 1: WITHOUT index (full table scan)...")
    # EXPLAIN before index — should show type=ALL
    explain_query(conn, query)
    for _ in range(iterations_before):
        execute(conn, query, fetch=True)
        time.sleep(0.1)

    log.info("  Adding index idx_demo_country_tier ...")
    execute(conn, "ALTER TABLE customers ADD INDEX idx_demo_country_tier (country, tier)")

    log.info("  Phase 2: WITH index (index range scan)...")
    # EXPLAIN after index — should show type=ref with key=idx_demo_country_tier
    explain_query(conn, query)
    for _ in range(iterations_after):
        execute(conn, query, fetch=True)
        time.sleep(0.1)

    execute(conn, "ALTER TABLE customers DROP INDEX idx_demo_country_tier")
    conn.close()
    log.info("✔ Scenario 7 done.")

# =============================================================================
# SCENARIO 8 — Mixed OLTP (realistic baseline traffic)
# Dynatrace: normal throughput pattern for comparison baseline
# =============================================================================

def _oltp_worker(cfg, duration_sec):
    conn = get_connection(cfg)
    end = time.time() + duration_sec
    while time.time() < end:
        action = random.choices(
            ['read_cust','read_order','new_order','update_stock','search_product'],
            weights=[40, 30, 15, 10, 5]
        )[0]
        try:
            if action == 'read_cust':
                execute(conn, "SELECT * FROM customers WHERE id=%s",
                        (random.randint(1,2000),), fetch=True)
            elif action == 'read_order':
                execute(conn,
                    "SELECT o.*, c.email FROM orders o JOIN customers c ON c.id=o.customer_id WHERE o.id=%s",
                    (random.randint(1, 5000),), fetch=True)
            elif action == 'new_order':
                conn.start_transaction()
                cur = conn.cursor()
                cur.execute(
                    "INSERT INTO orders (customer_id,status,total_amount) VALUES (%s,'pending',%s)",
                    (random.randint(1,2000), round(random.uniform(10, 500), 2))
                )
                oid = cur.lastrowid
                cur.execute(
                    "INSERT INTO order_items (order_id,product_id,quantity,unit_price) VALUES (%s,%s,%s,%s)",
                    (oid, random.randint(1,500), random.randint(1,5), round(random.uniform(5,200),2))
                )
                conn.commit()
                cur.close()
            elif action == 'update_stock':
                execute(conn,
                    "UPDATE products SET stock_qty = GREATEST(0, stock_qty - %s) WHERE id=%s",
                    (random.randint(1,3), random.randint(1,500)))
            elif action == 'search_product':
                kw = random.choice(['book','phone','shirt','lamp','toy','cream'])
                execute(conn, "SELECT * FROM products WHERE name LIKE %s LIMIT 20",
                        (f'%{kw}%',), fetch=True)
        except Error as e:
            try: conn.rollback()
            except: pass
            log.debug(f"OLTP error: {e}")
        time.sleep(random.uniform(0.02, 0.1))
    conn.close()

def scenario_mixed_oltp(cfg, threads=8, duration_sec=60):
    log.info(f"▶ SCENARIO 8: Mixed OLTP ({threads} threads, {duration_sec}s)")
    with ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(_oltp_worker, cfg, duration_sec) for _ in range(threads)]
        for f in as_completed(futs): f.result()
    log.info("✔ Scenario 8 done.")

# =============================================================================
# SCENARIO 9 — Temp Table + Filesort
# Dynatrace: high sort_merge_passes, tmp_disk_tables counter spikes
# =============================================================================

def scenario_temp_table_filesort(cfg, iterations=15):
    log.info("▶ SCENARIO 9: Temp table + filesort (GROUP BY without index)")
    conn = get_connection(cfg)

    join_sql = """
        SELECT c.country, c.tier, COUNT(*) as cnt, SUM(o.total_amount) as revenue
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id
        GROUP BY c.country, c.tier
        ORDER BY revenue DESC
    """
    cat_sql = """
        SELECT p.category,
               COUNT(DISTINCT oi.order_id) as order_count,
               SUM(oi.quantity) as total_qty,
               AVG(oi.unit_price) as avg_price
        FROM order_items oi
        JOIN products p ON p.id = oi.product_id
        GROUP BY p.category
        HAVING order_count > 5
        ORDER BY total_qty DESC
    """

    # EXPLAIN to surface Using temporary / Using filesort in Dynatrace
    log.info("  Running EXPLAIN on temp-table/filesort queries...")
    explain_query(conn, join_sql)
    explain_query(conn, cat_sql)

    for _ in range(iterations):
        execute(conn, join_sql, fetch=True)
        execute(conn, cat_sql, fetch=True)
        time.sleep(random.uniform(0.2, 0.5))
    conn.close()
    log.info("✔ Scenario 9 done.")

# =============================================================================
# SCENARIO 10 — Long Transaction (holds locks for extended time)
# Dynatrace: long-running transaction alerts, InnoDB lock metrics
# =============================================================================

def scenario_long_transaction(cfg, hold_sec=8):
    log.info(f"▶ SCENARIO 10: Long transaction (holds locks {hold_sec}s)")
    conn = get_connection(cfg)
    conn.start_transaction()
    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM orders LIMIT 5 FOR UPDATE")
        ids = [r[0] for r in cur.fetchall()]
        log.info(f"  Locked {len(ids)} rows for {hold_sec}s...")
        time.sleep(hold_sec)
        if ids:
            placeholders = ','.join(['%s'] * len(ids))
            cur.execute(f"UPDATE orders SET status='processing' WHERE id IN ({placeholders})", ids)
        conn.commit()
    except Error as e:
        conn.rollback()
        log.warning(f"Long txn error: {e}")
    finally:
        cur.close()
        conn.close()
    log.info("✔ Scenario 10 done.")

# =============================================================================
# SCENARIO 11 — Deadlock (actual InnoDB deadlock with retry)
# Dynatrace: Innodb_deadlocks counter, error events, rollback count
# =============================================================================

def _deadlock_thread(cfg, id_a, id_b, worker_id, barrier, results):
    """Worker that updates two rows in a specific order to cause a deadlock."""
    conn = get_connection(cfg)
    try:
        conn.start_transaction()
        cur = conn.cursor()
        # Lock first row
        cur.execute("UPDATE orders SET total_amount = total_amount + 0.01 WHERE id = %s", (id_a,))
        barrier.wait(timeout=10)  # synchronize so both workers hold one lock
        time.sleep(0.1)
        # Try to lock second row — should cause deadlock for one thread
        cur.execute("UPDATE orders SET total_amount = total_amount + 0.01 WHERE id = %s", (id_b,))
        conn.commit()
        cur.close()
        results[worker_id] = "committed"
    except Error as e:
        conn.rollback()
        results[worker_id] = f"deadlock: {e}"
        log.info(f"  Worker {worker_id} hit deadlock (expected): {str(e)[:80]}")
    finally:
        conn.close()

def scenario_deadlock(cfg, iterations=5):
    log.info("▶ SCENARIO 11: Deadlock generation (InnoDB deadlock detection)")
    conn = get_connection(cfg)
    rows = execute(conn, "SELECT id FROM orders LIMIT 10", fetch=True)
    conn.close()
    if len(rows) < 2:
        log.warning("Not enough orders for deadlock scenario, skipping.")
        return

    deadlock_count = 0
    for i in range(iterations):
        id1, id2 = rows[i % len(rows)]['id'], rows[(i + 1) % len(rows)]['id']
        barrier = threading.Barrier(2)
        results = {}
        # Thread 1: lock id1 then id2 | Thread 2: lock id2 then id1
        t1 = threading.Thread(target=_deadlock_thread, args=(cfg, id1, id2, 1, barrier, results))
        t2 = threading.Thread(target=_deadlock_thread, args=(cfg, id2, id1, 2, barrier, results))
        t1.start(); t2.start()
        t1.join(timeout=15); t2.join(timeout=15)
        for wid, result in results.items():
            if "deadlock" in result.lower():
                deadlock_count += 1
        time.sleep(0.5)

    log.info(f"  Triggered {deadlock_count} deadlocks across {iterations} rounds")
    log.info("✔ Scenario 11 done.")

# =============================================================================
# SCENARIO 12 — Connection Pool Exhaustion
# Dynatrace: Threads_connected spike, connection refused errors, aborted_connects
# =============================================================================

def scenario_connection_exhaustion(cfg, target_connections=80):
    log.info(f"▶ SCENARIO 12: Connection pool exhaustion ({target_connections} concurrent connections)")
    connections = []
    refused_count = 0
    try:
        for i in range(target_connections):
            try:
                c = mysql.connector.connect(
                    host=cfg.host, port=cfg.port,
                    user=cfg.user, password=cfg.password,
                    database=cfg.database,
                    connection_timeout=5, use_pure=True,
                )
                cur = c.cursor()
                cur.execute("SELECT SLEEP(0.1)")  # keep connection alive
                cur.fetchall()
                cur.close()
                connections.append(c)
            except Error as e:
                refused_count += 1
                if i % 10 == 0:
                    log.info(f"  Connection {i} refused: {str(e)[:80]}")
        log.info(f"  Opened {len(connections)} connections, {refused_count} refused")
        # Hold all connections open briefly to register in Dynatrace
        time.sleep(5)
    finally:
        for c in connections:
            try: c.close()
            except: pass
    log.info("✔ Scenario 12 done.")

# =============================================================================
# SCENARIO 13 — Non-sargable Queries (function on indexed column)
# Dynatrace: EXPLAIN type=ALL despite index existing, high logical reads
# =============================================================================

def scenario_non_sargable(cfg, iterations=30):
    log.info("▶ SCENARIO 13: Non-sargable queries (function wrapping indexed column)")
    conn = get_connection(cfg)

    # Ensure there's an index on created_at for orders
    try:
        execute(conn, "ALTER TABLE orders ADD INDEX idx_created_at (created_at)")
    except: pass

    # GOOD query — uses index
    good_sql = "SELECT id, customer_id, total_amount FROM orders WHERE created_at >= '2025-01-01' AND created_at < '2025-07-01'"
    # BAD query — wraps column in function, defeats index (non-sargable)
    bad_sql_year = "SELECT id, customer_id, total_amount FROM orders WHERE YEAR(created_at) = 2025"
    bad_sql_date = "SELECT id, customer_id, total_amount FROM orders WHERE DATE(created_at) = '2025-06-15'"
    bad_sql_calc = "SELECT id, customer_id, total_amount FROM orders WHERE created_at + INTERVAL 1 DAY > NOW()"

    log.info("  Running EXPLAIN on sargable vs non-sargable...")
    log.info("  === GOOD (uses index) ===")
    explain_query(conn, good_sql)
    log.info("  === BAD: YEAR() on column ===")
    explain_query(conn, bad_sql_year)
    log.info("  === BAD: DATE() on column ===")
    explain_query(conn, bad_sql_date)
    log.info("  === BAD: arithmetic on column ===")
    explain_query(conn, bad_sql_calc)

    for _ in range(iterations):
        execute(conn, good_sql, fetch=True)
        execute(conn, bad_sql_year, fetch=True)
        execute(conn, bad_sql_date, fetch=True)
        execute(conn, bad_sql_calc, fetch=True)
        time.sleep(random.uniform(0.05, 0.15))

    conn.close()
    log.info("✔ Scenario 13 done.")

# =============================================================================
# SCENARIO 14 — Implicit Type Conversion (string vs int mismatch)
# Dynatrace: Index bypass, full scan on JOIN or WHERE, high reads
# =============================================================================

def scenario_implicit_conversion(cfg, iterations=30):
    log.info("▶ SCENARIO 14: Implicit type conversion (string vs int)")
    conn = get_connection(cfg)

    # customer_id is INT but we query with string — forces implicit conversion, kills index
    bad_where = "SELECT * FROM orders WHERE customer_id = '42'"          # string literal for int column
    good_where = "SELECT * FROM orders WHERE customer_id = 42"           # correct int
    bad_join = ("SELECT o.*, c.email FROM orders o "
                "JOIN customers c ON c.id = CAST(o.customer_id AS CHAR) "  # force conversion
                "LIMIT 50")

    log.info("  Running EXPLAIN — implicit conversion scenarios...")
    log.info("  === GOOD (int = int) ===")
    explain_query(conn, good_where)
    log.info("  === BAD (int = 'string') ===")
    explain_query(conn, bad_where)
    log.info("  === BAD (CAST in JOIN) ===")
    explain_query(conn, bad_join)

    for _ in range(iterations):
        execute(conn, good_where, fetch=True)
        execute(conn, bad_where, fetch=True)
        execute(conn, bad_join, fetch=True)
        time.sleep(random.uniform(0.05, 0.15))

    conn.close()
    log.info("✔ Scenario 14 done.")

# =============================================================================
# SCENARIO 15 — Cartesian Join (missing JOIN condition)
# Dynatrace: Explosive row count, massive logical reads, long duration
# =============================================================================

def scenario_cartesian_join(cfg, iterations=5):
    log.info("▶ SCENARIO 15: Cartesian join (missing/wrong JOIN condition)")
    conn = get_connection(cfg)

    # Intentional cartesian product — LIMIT keeps it from destroying the server
    cartesian_sql = """
        SELECT c.first_name, p.name, o.total_amount
        FROM customers c, products p, orders o
        WHERE c.tier = 'platinum'
        LIMIT 5000
    """
    # Partial cartesian (missing one condition)
    partial_sql = """
        SELECT c.email, o.total_amount, oi.quantity
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN order_items oi  -- missing: ON oi.order_id = o.id
        WHERE c.country = 'US'
        LIMIT 2000
    """

    log.info("  Running EXPLAIN — cartesian join...")
    explain_query(conn, cartesian_sql)
    log.info("  Running EXPLAIN — partial cartesian (missing JOIN ON)...")
    explain_query(conn, partial_sql)

    for _ in range(iterations):
        execute(conn, cartesian_sql, fetch=True)
        execute(conn, partial_sql, fetch=True)
        time.sleep(random.uniform(0.3, 0.8))

    conn.close()
    log.info("✔ Scenario 15 done.")

# =============================================================================
# SCENARIO 16 — Unbounded SELECT (missing LIMIT on large tables)
# Dynatrace: High network bytes, memory pressure, long query duration
# =============================================================================

def scenario_unbounded_select(cfg, iterations=8):
    log.info("▶ SCENARIO 16: Unbounded SELECT (no LIMIT, full table transfer)")
    conn = get_connection(cfg)

    # These queries return ALL rows — heavy on network and memory
    unbounded_sql = [
        "SELECT * FROM orders",
        "SELECT * FROM order_items",
        "SELECT o.*, c.email, c.first_name, c.last_name FROM orders o JOIN customers c ON c.id = o.customer_id",
        "SELECT * FROM events",
    ]

    for sql in unbounded_sql:
        log.info(f"  EXPLAIN: {sql[:60]}...")
        explain_query(conn, sql)

    for _ in range(iterations):
        sql = random.choice(unbounded_sql)
        rows = execute(conn, sql, fetch=True)
        log.debug(f"  Returned {len(rows)} rows")
        time.sleep(random.uniform(0.3, 0.8))

    conn.close()
    log.info("✔ Scenario 16 done.")

# =============================================================================
# SCENARIO 17 — Metadata Lock / DDL Blocking
# Dynatrace: DDL stalls visible, DML blocked, sudden latency spike
# =============================================================================

def _ddl_blocker_dml(cfg, duration_sec):
    """Run DML in a loop while DDL is happening — will get blocked by metadata lock."""
    conn = get_connection(cfg)
    end = time.time() + duration_sec
    blocked = 0
    while time.time() < end:
        try:
            t0 = time.time()
            execute(conn, "INSERT INTO events (event_type, entity_type, entity_id, payload) VALUES (%s,%s,%s,%s)",
                    ('ddl_test', 'system', random.randint(1, 100), '{"test":"metadata_lock"}'))
            dur = time.time() - t0
            if dur > 1.0:
                blocked += 1
        except Error:
            try: conn.rollback()
            except: pass
        time.sleep(0.05)
    conn.close()
    return blocked

def scenario_metadata_lock(cfg):
    log.info("▶ SCENARIO 17: Metadata lock / DDL blocking")
    # Start DML workers
    with ThreadPoolExecutor(max_workers=3) as ex:
        dml_futs = [ex.submit(_ddl_blocker_dml, cfg, 15) for _ in range(3)]
        time.sleep(2)  # let DML workers start

        # Now run DDL that will acquire metadata lock, blocking DML
        conn = get_connection(cfg)
        log.info("  Running ALTER TABLE on events (will block DML)...")
        try:
            execute(conn, "ALTER TABLE events ADD COLUMN _ddl_test INT NULL")
            execute(conn, "ALTER TABLE events DROP COLUMN _ddl_test")
        except Error as e:
            log.warning(f"  DDL error: {e}")
        conn.close()

        blocked_total = 0
        for f in as_completed(dml_futs):
            blocked_total += f.result()
    log.info(f"  {blocked_total} DML operations experienced metadata lock wait")
    log.info("✔ Scenario 17 done.")

# =============================================================================
# SCENARIO 18 — Query Pattern Drift (simulates bad deployment)
# Dynatrace: New digest appears, response time shifts, plan regression
# =============================================================================

def scenario_query_drift(cfg, iterations_before=20, iterations_after=30):
    log.info("▶ SCENARIO 18: Query pattern drift (simulates bad deployment)")
    conn = get_connection(cfg)

    # Phase 1: "Old code" — efficient query using primary key
    old_query = "SELECT id, email, tier FROM customers WHERE id = %s"
    log.info("  Phase 1: 'Old code' — efficient PK lookup...")
    explain_query(conn, old_query, (1,))
    for _ in range(iterations_before):
        cid = random.randint(1, 2000)
        execute(conn, old_query, (cid,), fetch=True)
        time.sleep(0.05)

    log.info("  === Simulating bad deployment ===")
    time.sleep(2)

    # Phase 2: "New code" — developer rewrote query badly
    # Uses SELECT * (more columns), subquery, no index usage
    bad_query_1 = """
        SELECT * FROM customers
        WHERE email IN (
            SELECT DISTINCT c2.email FROM customers c2
            WHERE c2.tier = 'gold' OR c2.tier = 'platinum'
        )
    """
    bad_query_2 = """
        SELECT c.*, COUNT(o.id) as order_count, SUM(o.total_amount) as lifetime_value
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id
        GROUP BY c.id
        HAVING lifetime_value > 100
        ORDER BY lifetime_value DESC
    """
    bad_query_3 = """
        SELECT * FROM customers
        WHERE CONCAT(first_name, ' ', last_name) LIKE %s
    """

    log.info("  Phase 2: 'New code' — inefficient queries after deployment...")
    explain_query(conn, bad_query_1)
    explain_query(conn, bad_query_2)
    explain_query(conn, bad_query_3, ('%John%',))

    for _ in range(iterations_after):
        execute(conn, bad_query_1, fetch=True)
        execute(conn, bad_query_2, fetch=True)
        name = random.choice(['John', 'Jane', 'Smith', 'Williams', 'Brown'])
        execute(conn, bad_query_3, (f'%{name}%',), fetch=True)
        time.sleep(random.uniform(0.1, 0.3))

    conn.close()
    log.info("✔ Scenario 18 done.")

# =============================================================================
# SCENARIO 19 — Complex Explain Plans (deep node trees for visualizer demos)
# Dynatrace: Multi-node explain plans with subqueries, 5+ table joins,
#            derived tables, UNIONs, and GROUP BY/HAVING
# =============================================================================

def scenario_complex_plans(cfg, iterations=10):
    log.info("▶ SCENARIO 19: Complex explain plans (deep node trees)")
    conn = get_connection(cfg)

    # ── 1. Correlated subquery with nested scalar subquery (select_id 1,2,3) ──
    subquery_sql = """
        SELECT c.id, c.email, c.tier,
               (SELECT COUNT(*)
                FROM orders o
                WHERE o.customer_id = c.id
                  AND o.status IN ('shipped','delivered')) AS completed_orders,
               (SELECT COALESCE(SUM(oi.quantity * oi.unit_price), 0)
                FROM order_items oi
                WHERE oi.order_id IN (
                    SELECT o2.id FROM orders o2 WHERE o2.customer_id = c.id
                )) AS lifetime_value
        FROM customers c
        WHERE c.tier IN ('gold','platinum')
          AND EXISTS (
              SELECT 1 FROM orders o3
              WHERE o3.customer_id = c.id
                AND o3.created_at >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
          )
        ORDER BY lifetime_value DESC
        LIMIT 50
    """

    # ── 2. Five-table JOIN with aggregation (5-node nested_loop) ──────────
    five_join_sql = """
        SELECT c.country, c.tier,
               p.category,
               COUNT(DISTINCT o.id) AS order_count,
               SUM(oi.quantity) AS total_items,
               SUM(oi.quantity * oi.unit_price) AS gross_revenue,
               AVG(sf.revenue) AS avg_fact_revenue,
               MAX(o.created_at) AS last_order_date
        FROM customers c
        JOIN orders o       ON o.customer_id = c.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p     ON p.id = oi.product_id
        JOIN sales_facts sf ON sf.order_id = o.id AND sf.product_id = p.id
        WHERE c.country IN ('US','GB','DE','IN','FR')
          AND o.status != 'cancelled'
          AND p.price > 10.00
        GROUP BY c.country, c.tier, p.category
        HAVING gross_revenue > 100
        ORDER BY gross_revenue DESC
        LIMIT 100
    """

    # ── 3. Derived table / subquery in FROM (materialized_from_subquery) ──
    derived_sql = """
        SELECT ranked.country, ranked.tier,
               ranked.total_revenue, ranked.customer_count,
               ranked.total_revenue / ranked.customer_count AS revenue_per_customer,
               p_stats.top_category, p_stats.category_revenue
        FROM (
            SELECT c.country, c.tier,
                   SUM(o.total_amount) AS total_revenue,
                   COUNT(DISTINCT c.id) AS customer_count
            FROM customers c
            JOIN orders o ON o.customer_id = c.id
            WHERE o.status IN ('shipped','delivered')
            GROUP BY c.country, c.tier
            HAVING total_revenue > 50
        ) AS ranked
        JOIN (
            SELECT sf.country, sf.tier, sf.category AS top_category,
                   SUM(sf.revenue) AS category_revenue,
                   ROW_NUMBER() OVER (PARTITION BY sf.country, sf.tier ORDER BY SUM(sf.revenue) DESC) AS rn
            FROM sales_facts sf
            GROUP BY sf.country, sf.tier, sf.category
        ) AS p_stats ON p_stats.country = ranked.country
                     AND p_stats.tier = ranked.tier
                     AND p_stats.rn = 1
        ORDER BY ranked.total_revenue DESC
        LIMIT 50
    """

    # ── 4. UNION of multiple query branches (union_result) ────────────────
    union_sql = """
        SELECT 'high_value_customer' AS segment, c.id, c.email,
               SUM(o.total_amount) AS metric_value
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE c.tier = 'platinum'
        GROUP BY c.id, c.email
        HAVING metric_value > 500

        UNION ALL

        SELECT 'frequent_buyer' AS segment, c.id, c.email,
               COUNT(o.id) AS metric_value
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)
        GROUP BY c.id, c.email
        HAVING metric_value > 5

        UNION ALL

        SELECT 'big_basket' AS segment, c.id, c.email,
               MAX(item_counts.item_count) AS metric_value
        FROM customers c
        JOIN orders o ON o.customer_id = c.id
        JOIN (
            SELECT order_id, COUNT(*) AS item_count
            FROM order_items
            GROUP BY order_id
        ) AS item_counts ON item_counts.order_id = o.id
        GROUP BY c.id, c.email
        HAVING metric_value > 3

        ORDER BY metric_value DESC
        LIMIT 100
    """

    # ── 5. Multi-level GROUP BY + HAVING with window function ─────────────
    grouping_sql = """
        SELECT category_stats.*,
               CASE
                   WHEN category_stats.avg_order_value > 200 THEN 'premium'
                   WHEN category_stats.avg_order_value > 50  THEN 'standard'
                   ELSE 'budget'
               END AS price_segment
        FROM (
            SELECT p.category,
                   c.country,
                   COUNT(DISTINCT o.id) AS order_count,
                   COUNT(DISTINCT c.id) AS unique_customers,
                   SUM(oi.quantity * oi.unit_price) AS total_revenue,
                   AVG(o.total_amount) AS avg_order_value,
                   SUM(oi.quantity) AS total_units_sold,
                   MAX(o.created_at) AS most_recent_order
            FROM products p
            JOIN order_items oi ON oi.product_id = p.id
            JOIN orders o       ON o.id = oi.order_id
            JOIN customers c    ON c.id = o.customer_id
            WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 1 YEAR)
              AND o.status != 'cancelled'
            GROUP BY p.category, c.country
            HAVING order_count >= 2
               AND total_revenue > 10
        ) AS category_stats
        ORDER BY category_stats.total_revenue DESC
        LIMIT 200
    """

    # ── 6. Complex subquery with EXISTS, IN, and JOIN (deeply nested) ─────
    nested_sql = """
        SELECT c.id, c.email, c.first_name, c.last_name, c.tier,
               order_summary.order_count, order_summary.total_spent
        FROM customers c
        JOIN (
            SELECT o.customer_id,
                   COUNT(*) AS order_count,
                   SUM(o.total_amount) AS total_spent
            FROM orders o
            WHERE o.status IN ('shipped','delivered')
            GROUP BY o.customer_id
        ) AS order_summary ON order_summary.customer_id = c.id
        WHERE c.tier IN ('gold','platinum')
          AND order_summary.total_spent > (
              SELECT AVG(sub_total.customer_total) * 1.5
              FROM (
                  SELECT SUM(o2.total_amount) AS customer_total
                  FROM orders o2
                  WHERE o2.status IN ('shipped','delivered')
                  GROUP BY o2.customer_id
              ) AS sub_total
          )
          AND EXISTS (
              SELECT 1
              FROM order_items oi
              JOIN products p ON p.id = oi.product_id
              WHERE oi.order_id IN (SELECT id FROM orders WHERE customer_id = c.id)
                AND p.category = 'Electronics'
          )
        ORDER BY order_summary.total_spent DESC
        LIMIT 25
    """

    # ── 7. Anti-join pattern with LEFT JOIN / IS NULL + subquery ──────────
    antijoin_sql = """
        SELECT c.id, c.email, c.tier, c.created_at,
               last_order.last_order_date,
               DATEDIFF(NOW(), COALESCE(last_order.last_order_date, c.created_at)) AS days_inactive
        FROM customers c
        LEFT JOIN (
            SELECT customer_id, MAX(created_at) AS last_order_date
            FROM orders
            GROUP BY customer_id
        ) AS last_order ON last_order.customer_id = c.id
        WHERE c.tier IN ('gold','platinum')
          AND (last_order.last_order_date IS NULL
               OR last_order.last_order_date < DATE_SUB(NOW(), INTERVAL 90 DAY))
          AND c.id NOT IN (
              SELECT DISTINCT s.customer_id
              FROM sessions s
              WHERE s.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
                AND s.customer_id IS NOT NULL
          )
        ORDER BY days_inactive DESC
        LIMIT 50
    """

    all_complex_queries = [
        ("Correlated subqueries (3 levels deep)", subquery_sql, None),
        ("5-table JOIN + GROUP BY + HAVING", five_join_sql, None),
        ("Derived tables in FROM", derived_sql, None),
        ("UNION ALL (3 branches + derived table)", union_sql, None),
        ("Multi-level GROUP BY + HAVING + CASE", grouping_sql, None),
        ("Deeply nested subqueries (EXISTS + IN + derived)", nested_sql, None),
        ("Anti-join (LEFT JOIN + IS NULL + NOT IN)", antijoin_sql, None),
    ]

    # Run EXPLAIN on all complex queries to populate Dynatrace explain plans
    log.info("  Running EXPLAIN on 7 complex query patterns...")
    for label, sql, params in all_complex_queries:
        log.info(f"  --- {label} ---")
        explain_query(conn, sql, params)

    # Execute queries repeatedly to build up statistics in performance_schema
    log.info(f"  Executing complex queries ({iterations} rounds)...")
    for i in range(iterations):
        for label, sql, params in all_complex_queries:
            execute(conn, sql, params, fetch=True)
        if i % 3 == 0:
            # Re-run EXPLAIN periodically to keep plans fresh
            for label, sql, params in all_complex_queries:
                explain_query(conn, sql, params)
        time.sleep(random.uniform(0.3, 0.8))

    conn.close()
    log.info("✔ Scenario 19 done.")

# =============================================================================
# SCENARIO 20 — Heavy Lock Contention
# Dynatrace: Innodb_row_lock_waits, Innodb_row_lock_time, lock_wait_timeout
#            errors, SHARE MODE blocking, gap lock contention
# =============================================================================

def _heavy_lock_holder(cfg, order_ids, hold_sec, mode="exclusive"):
    """Hold row locks for an extended time to force other sessions to wait."""
    conn = get_connection(cfg)
    try:
        conn.start_transaction()
        cur = conn.cursor()
        placeholders = ','.join(['%s'] * len(order_ids))
        if mode == "share":
            cur.execute(f"SELECT * FROM orders WHERE id IN ({placeholders}) LOCK IN SHARE MODE", order_ids)
        else:
            cur.execute(f"SELECT * FROM orders WHERE id IN ({placeholders}) FOR UPDATE", order_ids)
        cur.fetchall()
        log.info(f"  Lock holder ({mode}): locked {len(order_ids)} rows, holding {hold_sec}s...")
        time.sleep(hold_sec)
        conn.commit()
        cur.close()
    except Error as e:
        try: conn.rollback()
        except: pass
        log.debug(f"Lock holder ({mode}) error: {e}")
    finally:
        conn.close()

def _heavy_lock_waiter(cfg, order_ids, worker_id, results):
    """Try to update locked rows — will be blocked and may hit lock_wait_timeout."""
    conn = get_connection(cfg)
    waits = 0
    timeouts = 0
    try:
        for oid in order_ids:
            try:
                t0 = time.time()
                conn.start_transaction()
                cur = conn.cursor()
                cur.execute("UPDATE orders SET total_amount = total_amount + 0.01 WHERE id = %s", (oid,))
                conn.commit()
                cur.close()
                wait_ms = (time.time() - t0) * 1000
                if wait_ms > 500:
                    waits += 1
                    log.info(f"  Waiter {worker_id}: waited {wait_ms:.0f}ms for row {oid}")
            except Error as e:
                try: conn.rollback()
                except: pass
                err = str(e).lower()
                if "lock wait timeout" in err:
                    timeouts += 1
                    log.info(f"  Waiter {worker_id}: LOCK WAIT TIMEOUT on row {oid}")
                else:
                    log.debug(f"  Waiter {worker_id} error: {e}")
    finally:
        conn.close()
    results[worker_id] = {"waits": waits, "timeouts": timeouts}

def _gap_lock_worker(cfg, worker_id, range_start, range_end, results):
    """Cause gap lock contention via range queries with FOR UPDATE."""
    conn = get_connection(cfg)
    gap_locks = 0
    try:
        for _ in range(3):
            try:
                conn.start_transaction()
                cur = conn.cursor()
                cur.execute(
                    "SELECT * FROM orders WHERE id BETWEEN %s AND %s FOR UPDATE",
                    (range_start, range_end)
                )
                cur.fetchall()
                time.sleep(random.uniform(1.0, 2.0))
                # Insert into the gap — may conflict with other gap locks
                try:
                    cur.execute(
                        "INSERT INTO events (event_type, entity_type, entity_id, payload) "
                        "VALUES (%s, %s, %s, %s)",
                        ('gap_lock_test', 'order', range_start, f'{{"worker": {worker_id}}}')
                    )
                except Error:
                    pass
                conn.commit()
                cur.close()
                gap_locks += 1
            except Error as e:
                try: conn.rollback()
                except: pass
                log.debug(f"Gap lock worker {worker_id}: {e}")
            time.sleep(0.2)
    finally:
        conn.close()
    results[worker_id] = gap_locks

def scenario_heavy_lock_contention(cfg):
    log.info("▶ SCENARIO 20: Heavy lock contention (SHARE locks, gap locks, timeouts)")
    conn = get_connection(cfg)
    rows = execute(conn, "SELECT id FROM orders ORDER BY id LIMIT 40", fetch=True)
    conn.close()
    if len(rows) < 20:
        log.warning("Not enough orders for heavy lock scenario, skipping.")
        return
    ids = [r['id'] for r in rows]

    # --- Phase 1: Exclusive lock holder blocks multiple writers ---
    log.info("  Phase 1: Exclusive lock holder + 4 blocked writers")
    results = {}
    holder = threading.Thread(
        target=_heavy_lock_holder, args=(cfg, ids[:20], 10, "exclusive")
    )
    holder.start()
    time.sleep(0.5)  # let holder acquire locks
    waiters = []
    for i in range(4):
        w = threading.Thread(
            target=_heavy_lock_waiter,
            args=(cfg, ids[:20], i + 1, results)
        )
        waiters.append(w)
        w.start()
    holder.join(timeout=20)
    for w in waiters: w.join(timeout=20)
    total_waits = sum(r.get("waits", 0) for r in results.values())
    total_timeouts = sum(r.get("timeouts", 0) for r in results.values())
    log.info(f"  Phase 1 result: {total_waits} lock waits, {total_timeouts} timeouts")

    # --- Phase 2: SHARE lock blocks exclusive writers ---
    log.info("  Phase 2: SHARE lock holder blocks UPDATE writers")
    results = {}
    holder = threading.Thread(
        target=_heavy_lock_holder, args=(cfg, ids[10:30], 8, "share")
    )
    holder.start()
    time.sleep(0.5)
    waiters = []
    for i in range(3):
        w = threading.Thread(
            target=_heavy_lock_waiter,
            args=(cfg, ids[10:30], i + 10, results)
        )
        waiters.append(w)
        w.start()
    holder.join(timeout=15)
    for w in waiters: w.join(timeout=15)
    total_waits = sum(r.get("waits", 0) for r in results.values())
    log.info(f"  Phase 2 result: {total_waits} lock waits (SHARE → exclusive conflict)")

    # --- Phase 3: Gap lock contention via overlapping range scans ---
    log.info("  Phase 3: Gap lock contention (overlapping range FOR UPDATE)")
    gap_results = {}
    gap_threads = []
    for i in range(4):
        start_idx = i * 5
        end_idx = start_idx + 15  # overlapping ranges
        t = threading.Thread(
            target=_gap_lock_worker,
            args=(cfg, i + 1, ids[start_idx], ids[min(end_idx, len(ids) - 1)], gap_results)
        )
        gap_threads.append(t)
        t.start()
    for t in gap_threads: t.join(timeout=30)
    log.info(f"  Phase 3 result: gap lock rounds completed")

    # --- Phase 4: Rapid lock acquire/release cycle (high Innodb_row_lock_waits) ---
    log.info("  Phase 4: Rapid lock cycling (8 workers, same 10 rows)")
    hot_ids = ids[:10]
    rapid_workers = []
    for i in range(8):
        t = threading.Thread(
            target=_lock_worker,
            args=(cfg, hot_ids * 2, i + 1, 0.5, 1.5)  # hold 0.5-1.5s each
        )
        rapid_workers.append(t)
        t.start()
    for t in rapid_workers: t.join(timeout=60)
    log.info("  Phase 4 complete")

    log.info("✔ Scenario 20 done.")

# =============================================================================
# MAIN
# =============================================================================

ALL_SCENARIOS = {
    "full_table_scan":     scenario_full_table_scan,
    "n_plus_1":            scenario_n_plus_1,
    "heavy_aggregation":   scenario_heavy_aggregation,
    "lock_contention":     scenario_lock_contention,
    "high_frequency":      scenario_high_frequency,
    "slow_inserts":        scenario_slow_inserts,
    "index_change":        scenario_index_change,
    "mixed_oltp":          scenario_mixed_oltp,
    "temp_table_filesort": scenario_temp_table_filesort,
    "long_transaction":    scenario_long_transaction,
    "deadlock":            scenario_deadlock,
    "connection_exhaustion": scenario_connection_exhaustion,
    "non_sargable":        scenario_non_sargable,
    "implicit_conversion": scenario_implicit_conversion,
    "cartesian_join":      scenario_cartesian_join,
    "unbounded_select":    scenario_unbounded_select,
    "metadata_lock":       scenario_metadata_lock,
    "query_drift":         scenario_query_drift,
    "complex_plans":       scenario_complex_plans,
    "heavy_lock_contention": scenario_heavy_lock_contention,
}

def run_all_scenarios(cfg):
    log.info("=" * 60)
    log.info("  Dynatrace MariaDB Observability Demo — Full Run")
    log.info("=" * 60)
    # Run baseline OLTP in background throughout
    oltp_thread = threading.Thread(
        target=scenario_mixed_oltp, args=(cfg,),
        kwargs={"threads": 5, "duration_sec": 300}, daemon=True
    )
    oltp_thread.start()
    time.sleep(3)

    # --- Original scenarios (1-10) ---
    scenario_full_table_scan(cfg)
    scenario_n_plus_1(cfg)
    scenario_temp_table_filesort(cfg)
    scenario_slow_inserts(cfg)
    scenario_high_frequency(cfg)
    scenario_lock_contention(cfg)
    scenario_long_transaction(cfg)
    scenario_heavy_aggregation(cfg)
    scenario_index_change(cfg)

    # --- New scenarios (11-18) ---
    scenario_deadlock(cfg)
    scenario_connection_exhaustion(cfg)
    scenario_non_sargable(cfg)
    scenario_implicit_conversion(cfg)
    scenario_cartesian_join(cfg)
    scenario_unbounded_select(cfg)
    scenario_metadata_lock(cfg)
    scenario_query_drift(cfg)
    scenario_complex_plans(cfg)
    scenario_heavy_lock_contention(cfg)

    oltp_thread.join()
    log.info("=" * 60)
    log.info("  All scenarios complete!")
    log.info("  Check Dynatrace → Databases → dynatrace_demo")
    log.info("=" * 60)

def main():
    parser = argparse.ArgumentParser(description="MariaDB Dynatrace Test Data Generator")
    parser.add_argument("--host",       default="127.0.0.1")
    parser.add_argument("--port",       type=int, default=3306)
    parser.add_argument("--user",       default="root")
    parser.add_argument("--password",   default="")
    parser.add_argument("--database",   default="dynatrace_demo")
    parser.add_argument("--scenario",
        choices=list(ALL_SCENARIOS.keys()) + ["all", "seed", "cleanup"],
        default="all",
        help="Scenario to run (default: all)")
    parser.add_argument("--skip-seed",  action="store_true",
        help="Skip seeding if data already loaded")
    parser.add_argument("--cleanup-days", type=int, default=10,
        help="Delete data older than N days (default: 10)")
    parser.add_argument("--cleanup-before", action="store_true",
        help="Run cleanup before scenarios")
    parser.add_argument("--cleanup-after", action="store_true",
        help="Run cleanup after scenarios")
    parser.add_argument("--skip-monitoring-setup", action="store_true",
        help="Skip Dynatrace monitoring configuration (performance_schema, slow log)")
    args = parser.parse_args()

    # Ensure DB exists
    try:
        tmp = mysql.connector.connect(
            host=args.host, port=args.port, user=args.user, password=args.password
        )
        cur = tmp.cursor()
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{args.database}`")
        tmp.commit()
        cur.close()
        tmp.close()
    except Error as e:
        log.error(f"Cannot connect to MariaDB: {e}")
        exit(1)

    # Configure MariaDB for Dynatrace observability (performance_schema, slow log, explain plans)
    if not args.skip_monitoring_setup:
        setup_dynatrace_monitoring(args)

    setup_schema(args)

    if not args.skip_seed:
        conn = get_connection(args)
        rows = execute(conn, "SELECT COUNT(*) as c FROM customers", fetch=True)
        count = rows[0]['c'] if rows else 0
        if count < 100:
            seed_customers(conn)
            seed_products(conn)
            seed_orders(conn)
            seed_sales_facts(conn)
        else:
            log.info(f"Existing data found ({count} customers). Use --skip-seed to suppress this check.")
        conn.close()

    if args.scenario == "seed":
        log.info("Seed-only complete.")
        return

    if args.scenario == "cleanup":
        cleanup_old_data(args, days=args.cleanup_days)
        return

    # Optional cleanup before running scenarios
    if args.cleanup_before:
        cleanup_old_data(args, days=args.cleanup_days)

    if args.scenario == "all":
        run_all_scenarios(args)
    else:
        log.info(f"Running: {args.scenario}")
        ALL_SCENARIOS[args.scenario](args)

    # Optional cleanup after running scenarios
    if args.cleanup_after:
        cleanup_old_data(args, days=args.cleanup_days)

if __name__ == "__main__":
    main()
