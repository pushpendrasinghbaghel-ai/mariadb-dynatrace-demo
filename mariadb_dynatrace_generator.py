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
    return mysql.connector.connect(
        host=cfg.host,
        port=cfg.port,
        user=cfg.user,
        password=cfg.password,
        database=cfg.database,
        connection_timeout=10,
        autocommit=False,
    )

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

    for i in range(n):
        cid = random.choice(cust_ids)
        status = random.choice(statuses)
        created = fake.date_time_between(start_date="-1y", end_date="now")
        cur2 = conn.cursor()
        try:
            cur2.execute(
                "INSERT INTO orders (customer_id,status,total_amount,created_at) VALUES (%s,%s,%s,%s)",
                (cid, status, 0, created)
            )
            oid = cur2.lastrowid
            items = random.sample(prod_rows, min(random.randint(1, 5), len(prod_rows)))
            total = 0
            for pid, price in items:
                qty = random.randint(1, 10)
                total += float(price) * qty
                cur2.execute(
                    "INSERT INTO order_items (order_id,product_id,quantity,unit_price) VALUES (%s,%s,%s,%s)",
                    (oid, pid, qty, price)
                )
            cur2.execute("UPDATE orders SET total_amount=%s WHERE id=%s", (total, oid))
            conn.commit()
        except Error as e:
            conn.rollback()
            log.debug(f"Order seed error: {e}")
        finally:
            cur2.close()
        if i % 500 == 0:
            log.info(f"  {i}/{n} orders...")
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

def _lock_worker(cfg, order_ids, worker_id):
    conn = get_connection(cfg)
    try:
        for oid in order_ids:
            try:
                conn.start_transaction()
                cur = conn.cursor()
                cur.execute("SELECT id, total_amount, status FROM orders WHERE id = %s FOR UPDATE", (oid,))
                row = cur.fetchone()
                if row:
                    time.sleep(random.uniform(0.05, 0.2))  # hold lock
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
    log.info("▶ SCENARIO 4: Lock contention (concurrent UPDATE on overlapping rows)")
    conn = get_connection(cfg)
    rows = execute(conn, "SELECT id FROM orders LIMIT 30", fetch=True)
    conn.close()
    if not rows:
        log.warning("No orders found, skipping.")
        return
    ids = [r['id'] for r in rows]
    for _ in range(iterations):
        t1 = threading.Thread(target=_lock_worker, args=(cfg, ids[:15], 1))
        t2 = threading.Thread(target=_lock_worker, args=(cfg, ids[10:25], 2))
        t3 = threading.Thread(target=_lock_worker, args=(cfg, ids[5:20], 3))
        for t in [t1, t2, t3]: t.start()
        for t in [t1, t2, t3]: t.join()
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
}

def run_all_scenarios(cfg):
    log.info("=" * 60)
    log.info("  Dynatrace MariaDB Observability Demo — Full Run")
    log.info("=" * 60)
    # Run baseline OLTP in background throughout
    oltp_thread = threading.Thread(
        target=scenario_mixed_oltp, args=(cfg,),
        kwargs={"threads": 5, "duration_sec": 180}, daemon=True
    )
    oltp_thread.start()
    time.sleep(3)

    scenario_full_table_scan(cfg)
    scenario_n_plus_1(cfg)
    scenario_temp_table_filesort(cfg)
    scenario_slow_inserts(cfg)
    scenario_high_frequency(cfg)
    scenario_lock_contention(cfg)
    scenario_long_transaction(cfg)
    scenario_heavy_aggregation(cfg)
    scenario_index_change(cfg)

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
