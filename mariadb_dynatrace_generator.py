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
import json
import random
import re
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


def _sanitize_digest_text(digest_text):
    """Sanitize performance_schema DIGEST_TEXT so it can be used with EXPLAIN.

    MariaDB normalizes SQL in digest text in ways that break re-execution:
      - COUNT(DISTINCT x) → COUNT(DISTINCTROW x) — invalid syntax
      - Spaces inside function calls: COUNT ( * ), SUM ( x )
      - Backtick quoting everywhere
      - Parameter markers as literal ?
    """
    sql = digest_text

    # Fix DISTINCTROW → DISTINCT (MariaDB digest normalization artifact)
    sql = re.sub(r'\bDISTINCTROW\b', 'DISTINCT', sql, flags=re.IGNORECASE)

    # Replace ? placeholders with safe literal values.
    # We need context-aware replacement:
    #   LIMIT ?  →  LIMIT 10
    #   LIKE ?   →  LIKE '%a%'
    #   = ?      →  = 1
    #   IN (?)   →  IN (1)
    #   Other ?  →  1
    def _replace_placeholder(match):
        before = match.string[:match.start()].rstrip()
        upper = before.upper()
        if upper.endswith('LIMIT'):
            return '10'
        if upper.endswith('LIKE'):
            return "'%a%'"
        if upper.endswith('BETWEEN') or upper.endswith('AND'):
            return "'2025-01-01'"
        return '1'

    sql = re.sub(r'\?', _replace_placeholder, sql)

    return sql


def process_explain_queue(cfg, batch_size=50):
    """Process PENDING items in dt_explain_queue by running real EXPLAIN FORMAT=JSON.

    This is the missing piece of the explain plan pipeline:
      1. Reads PENDING items from dt_explain_queue
      2. Sanitizes the digest text (fixes MariaDB normalization artifacts)
      3. Runs EXPLAIN FORMAT=JSON against the actual database
      4. Parses the JSON plan and extracts key metrics
      5. Stores the real plan in dt_mariadb_explain_plans
      6. Updates the queue item status to COMPLETED / FAILED / SKIPPED
    """
    log.info("▶ Processing explain plan queue...")

    conn = mysql.connector.connect(
        host=cfg.host, port=cfg.port,
        user=cfg.user, password=cfg.password,
        connection_timeout=30, use_pure=True, autocommit=False,
    )
    cur = conn.cursor(dictionary=True)

    # Ensure dt_monitoring database and tables exist
    try:
        cur.execute("USE dt_monitoring")
    except Error:
        log.warning("  dt_monitoring database does not exist — skipping queue processing")
        conn.close()
        return 0

    # Check if queue table exists
    try:
        cur.execute("SELECT COUNT(*) as cnt FROM dt_explain_queue WHERE status = 'PENDING'")
        pending = cur.fetchone()['cnt']
    except Error:
        log.warning("  dt_explain_queue table does not exist — skipping")
        conn.close()
        return 0

    if pending == 0:
        log.info("  No pending items in explain queue")
        conn.close()
        return 0

    log.info(f"  Found {pending} pending items, processing up to {batch_size}...")

    # Fetch PENDING items
    cur.execute(
        "SELECT id, query_digest, query_text, database_name, "
        "avg_execution_time_ms, total_executions "
        "FROM dt_explain_queue "
        "WHERE status = 'PENDING' "
        "ORDER BY avg_execution_time_ms DESC "
        "LIMIT %s",
        (batch_size,)
    )
    items = cur.fetchall()

    completed = 0
    failed = 0
    skipped = 0

    for item in items:
        qid = item['id']
        query_text = item['query_text']
        db_name = item['database_name'] or cfg.database
        digest = item['query_digest']

        # Mark as PROCESSING
        cur.execute(
            "UPDATE dt_explain_queue SET status = 'PROCESSING', processed_at = NOW() "
            "WHERE id = %s", (qid,)
        )
        conn.commit()

        # Skip non-SELECT queries
        stripped = query_text.strip().upper()
        if not stripped.startswith('SELECT'):
            cur.execute(
                "UPDATE dt_explain_queue SET status = 'SKIPPED', "
                "error_message = 'Non-SELECT or system query' WHERE id = %s",
                (qid,)
            )
            conn.commit()
            skipped += 1
            continue

        # Skip queries targeting system schemas
        if any(s in stripped for s in ['PERFORMANCE_SCHEMA', 'INFORMATION_SCHEMA',
                                       'DT_MONITORING', 'MYSQL.']):
            cur.execute(
                "UPDATE dt_explain_queue SET status = 'SKIPPED', "
                "error_message = 'System schema query' WHERE id = %s",
                (qid,)
            )
            conn.commit()
            skipped += 1
            continue

        # Sanitize the digest text for EXPLAIN
        sanitized_sql = _sanitize_digest_text(query_text)

        try:
            # Switch to the target database for correct table resolution
            cur.execute("USE `%s`" % db_name.replace('`', '``'))

            # Run the real EXPLAIN FORMAT=JSON
            explain_sql = "EXPLAIN FORMAT=JSON " + sanitized_sql
            cur.execute(explain_sql)
            row = cur.fetchone()
            if not row:
                raise Error("EXPLAIN returned no rows")

            # The result is a single row with a single column named 'EXPLAIN'
            explain_json_str = list(row.values())[0]
            explain_data = json.loads(explain_json_str)

            # Extract key metrics from the JSON plan
            access_type = None
            rows_estimated = 0
            filtered_pct = 0.0
            key_used = None
            possible_keys = None
            extra_info = None
            cost_estimate = None
            table_name = None

            # Parse the query_block to extract first table's plan details
            qb = explain_data.get('query_block', {})
            nested = qb.get('nested_loop', [])
            if nested:
                first_table = nested[0]
                # Handle read_sorted_file wrapper
                if 'read_sorted_file' in first_table:
                    fs = first_table['read_sorted_file'].get('filesort', {})
                    first_table = fs.get('table', first_table)
                    if not isinstance(first_table, dict) or 'table_name' not in first_table:
                        first_table = fs
                tbl = first_table.get('table', first_table)
                if isinstance(tbl, dict):
                    access_type = tbl.get('access_type')
                    rows_estimated = tbl.get('rows', 0)
                    filtered_pct = tbl.get('filtered', 0.0)
                    key_used = tbl.get('key')
                    possible_keys = ', '.join(tbl.get('possible_keys', []) or [])
                    extra_info = tbl.get('Extra')
                    table_name = tbl.get('table_name')

            # Also check for filesort at top level
            if not nested and 'filesort' in qb:
                fs = qb['filesort']
                fs_nested = fs.get('nested_loop', fs.get('temporary_table', {}).get('nested_loop', []))
                if fs_nested:
                    tbl = fs_nested[0].get('table', {})
                    access_type = tbl.get('access_type')
                    rows_estimated = tbl.get('rows', 0)
                    filtered_pct = tbl.get('filtered', 0.0)
                    table_name = tbl.get('table_name')

            # Store the real explain plan
            cur.execute("USE dt_monitoring")
            cur.execute(
                "INSERT INTO dt_mariadb_explain_plans ("
                "  query_digest, query_text, explain_plan_json, "
                "  table_access_type, rows_estimated, filtered_percentage, "
                "  key_used, possible_keys, extra_info, cost_estimate, "
                "  table_name, database_name, "
                "  avg_execution_time_ms, total_executions"
                ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    digest,
                    query_text,
                    explain_json_str,
                    access_type,
                    rows_estimated or 0,
                    filtered_pct or 0.0,
                    key_used,
                    possible_keys or None,
                    extra_info,
                    cost_estimate,
                    table_name,
                    db_name,
                    item.get('avg_execution_time_ms', 0),
                    item.get('total_executions', 0),
                )
            )
            explain_plan_id = cur.lastrowid

            # Update queue item as COMPLETED
            cur.execute(
                "UPDATE dt_explain_queue SET status = 'COMPLETED', "
                "explain_plan_id = %s, processed_at = NOW() WHERE id = %s",
                (explain_plan_id, qid)
            )
            conn.commit()
            completed += 1

            log.debug(f"  ✓ id={qid} type={access_type} rows={rows_estimated} "
                      f"key={key_used} table={table_name}")

        except (Error, json.JSONDecodeError, Exception) as e:
            try:
                conn.rollback()
            except Exception:
                pass
            err_msg = str(e)[:500]
            try:
                cur.execute("USE dt_monitoring")
                cur.execute(
                    "UPDATE dt_explain_queue SET status = 'FAILED', "
                    "error_message = %s, processed_at = NOW() WHERE id = %s",
                    (err_msg, qid)
                )
                conn.commit()
            except Exception:
                pass
            failed += 1
            log.debug(f"  ✗ id={qid}: {err_msg[:100]}")

    cur.close()
    conn.close()

    log.info(f"  Explain queue processed: {completed} completed, {failed} failed, {skipped} skipped")
    return completed


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

    # ── 8. Reset FAILED explain queue items for retry with improved sanitization ─
    try:
        cur.execute("USE dt_monitoring")
        cur.execute(
            "UPDATE dt_explain_queue SET status = 'PENDING', error_message = NULL "
            "WHERE status = 'FAILED'"
        )
        reset_count = cur.rowcount
        conn.commit()
        if reset_count > 0:
            log.info(f"  ✓ Reset {reset_count} FAILED explain queue items for retry")
    except Error:
        pass  # dt_monitoring may not exist yet

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
    # ── New tables for complex explain plan scenarios ─────────────────────
    """CREATE TABLE IF NOT EXISTS product_categories (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        name        VARCHAR(100) NOT NULL,
        parent_id   INT NULL,
        depth       INT DEFAULT 0,
        path        VARCHAR(500) DEFAULT '',
        INDEX idx_parent (parent_id)
    )""",
    """CREATE TABLE IF NOT EXISTS departments (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        name            VARCHAR(100) NOT NULL,
        parent_dept_id  INT NULL,
        budget          DECIMAL(15,2) DEFAULT 0,
        cost_center     VARCHAR(20),
        INDEX idx_parent_dept (parent_dept_id),
        INDEX idx_cost_center (cost_center)
    )""",
    """CREATE TABLE IF NOT EXISTS employees (
        id                INT AUTO_INCREMENT PRIMARY KEY,
        first_name        VARCHAR(100),
        last_name         VARCHAR(100),
        email             VARCHAR(255) NOT NULL,
        department_id     INT,
        manager_id        INT NULL,
        hire_date         DATE,
        salary            DECIMAL(12,2),
        performance_score DECIMAL(3,2),
        is_active         TINYINT DEFAULT 1,
        INDEX idx_dept (department_id),
        INDEX idx_manager (manager_id),
        INDEX idx_hire_date (hire_date),
        INDEX idx_salary (salary),
        INDEX idx_dept_salary (department_id, salary),
        INDEX idx_active_dept (is_active, department_id)
    )""",
    """CREATE TABLE IF NOT EXISTS product_reviews (
        id            INT AUTO_INCREMENT PRIMARY KEY,
        product_id    INT NOT NULL,
        customer_id   INT NOT NULL,
        rating        TINYINT NOT NULL,
        review_title  VARCHAR(200),
        review_text   TEXT,
        helpful_votes INT DEFAULT 0,
        verified      TINYINT DEFAULT 0,
        created_at    DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_product (product_id),
        INDEX idx_customer (customer_id),
        INDEX idx_rating (rating),
        INDEX idx_product_rating (product_id, rating),
        INDEX idx_verified_rating (verified, rating),
        FULLTEXT INDEX ft_review (review_title, review_text)
    )""",
    """CREATE TABLE IF NOT EXISTS warehouses (
        id        INT AUTO_INCREMENT PRIMARY KEY,
        name      VARCHAR(100),
        region    VARCHAR(50),
        country   VARCHAR(50),
        capacity  INT,
        INDEX idx_region (region),
        INDEX idx_country (country)
    )""",
    """CREATE TABLE IF NOT EXISTS warehouse_stock (
        id               INT AUTO_INCREMENT PRIMARY KEY,
        warehouse_id     INT NOT NULL,
        product_id       INT NOT NULL,
        quantity         INT DEFAULT 0,
        reserved_qty     INT DEFAULT 0,
        last_restock_date DATE,
        INDEX idx_warehouse (warehouse_id),
        INDEX idx_product (product_id),
        INDEX idx_wh_prod (warehouse_id, product_id),
        INDEX idx_quantity (quantity)
    )""",
    """CREATE TABLE IF NOT EXISTS suppliers (
        id             INT AUTO_INCREMENT PRIMARY KEY,
        name           VARCHAR(200),
        country        VARCHAR(50),
        rating         DECIMAL(3,2),
        lead_time_days INT,
        is_active      TINYINT DEFAULT 1,
        INDEX idx_country (country),
        INDEX idx_rating (rating),
        INDEX idx_active (is_active)
    )""",
    """CREATE TABLE IF NOT EXISTS product_suppliers (
        id          INT AUTO_INCREMENT PRIMARY KEY,
        product_id  INT NOT NULL,
        supplier_id INT NOT NULL,
        unit_cost   DECIMAL(10,2),
        is_primary  TINYINT DEFAULT 0,
        INDEX idx_product (product_id),
        INDEX idx_supplier (supplier_id),
        INDEX idx_prod_supp (product_id, supplier_id)
    )""",
    """CREATE TABLE IF NOT EXISTS payment_transactions (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        order_id        INT NOT NULL,
        payment_method  ENUM('credit_card','debit_card','paypal','wire','crypto') DEFAULT 'credit_card',
        amount          DECIMAL(12,2),
        currency        VARCHAR(3) DEFAULT 'USD',
        status          ENUM('pending','authorized','captured','refunded','failed') DEFAULT 'pending',
        gateway_ref     VARCHAR(100),
        fraud_score     DECIMAL(5,4) DEFAULT NULL,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        processed_at    DATETIME,
        INDEX idx_order (order_id),
        INDEX idx_status (status),
        INDEX idx_method (payment_method),
        INDEX idx_created (created_at),
        INDEX idx_gateway (gateway_ref),
        INDEX idx_method_status (payment_method, status),
        INDEX idx_order_status (order_id, status),
        INDEX idx_fraud (fraud_score)
    )""",
    """CREATE TABLE IF NOT EXISTS promotions (
        id               INT AUTO_INCREMENT PRIMARY KEY,
        code             VARCHAR(50) UNIQUE,
        description      VARCHAR(255),
        discount_type    ENUM('percentage','fixed','buy_x_get_y') DEFAULT 'percentage',
        discount_value   DECIMAL(10,2),
        min_order_amount DECIMAL(10,2) DEFAULT 0,
        max_uses         INT DEFAULT NULL,
        current_uses     INT DEFAULT 0,
        valid_from       DATE,
        valid_until      DATE,
        target_tier      ENUM('bronze','silver','gold','platinum') DEFAULT NULL,
        target_category  VARCHAR(100) DEFAULT NULL,
        INDEX idx_code (code),
        INDEX idx_valid (valid_from, valid_until),
        INDEX idx_tier (target_tier)
    )""",
    """CREATE TABLE IF NOT EXISTS order_promotions (
        id              INT AUTO_INCREMENT PRIMARY KEY,
        order_id        INT NOT NULL,
        promotion_id    INT NOT NULL,
        discount_amount DECIMAL(10,2),
        INDEX idx_order (order_id),
        INDEX idx_promo (promotion_id)
    )""",
    """CREATE TABLE IF NOT EXISTS inventory_log (
        id              BIGINT AUTO_INCREMENT PRIMARY KEY,
        product_id      INT NOT NULL,
        warehouse_id    INT NOT NULL,
        change_type     ENUM('receipt','sale','adjustment','transfer','return') DEFAULT 'sale',
        quantity_change INT,
        reference_id    INT,
        created_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
        INDEX idx_product (product_id),
        INDEX idx_warehouse (warehouse_id),
        INDEX idx_type (change_type),
        INDEX idx_created (created_at),
        INDEX idx_prod_wh_created (product_id, warehouse_id, created_at)
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

# ── Seed new complex tables ──────────────────────────────────────────────────

def seed_product_categories(conn):
    """Seed hierarchical product categories (3 levels deep) for recursive CTE demos."""
    log.info("Seeding product_categories (3 levels)...")
    top_level = [
        'Electronics', 'Clothing', 'Home & Garden', 'Sports & Outdoors',
        'Books & Media', 'Food & Beverage', 'Health & Beauty', 'Automotive',
    ]
    rows = []
    cat_id = 1
    id_map = {}  # name -> id
    # Level 0
    for name in top_level:
        rows.append((cat_id, name, None, 0, name))
        id_map[name] = cat_id
        cat_id += 1
    # Level 1
    sub_cats = {
        'Electronics': ['Smartphones', 'Laptops', 'Tablets', 'Cameras', 'Audio', 'Wearables'],
        'Clothing': ['Men', 'Women', 'Kids', 'Accessories', 'Shoes', 'Sportswear'],
        'Home & Garden': ['Furniture', 'Kitchen', 'Bedding', 'Lighting', 'Garden Tools', 'Decor'],
        'Sports & Outdoors': ['Fitness', 'Camping', 'Cycling', 'Running', 'Team Sports', 'Water Sports'],
        'Books & Media': ['Fiction', 'Non-Fiction', 'Textbooks', 'Comics', 'Music', 'Movies'],
        'Food & Beverage': ['Snacks', 'Drinks', 'Organic', 'Frozen', 'Gourmet', 'Supplements'],
        'Health & Beauty': ['Skincare', 'Haircare', 'Makeup', 'Vitamins', 'Personal Care', 'Fragrance'],
        'Automotive': ['Parts', 'Tools', 'Electronics', 'Accessories', 'Tires', 'Oils'],
    }
    for parent, children in sub_cats.items():
        for child in children:
            path = f"{parent}/{child}"
            rows.append((cat_id, child, id_map[parent], 1, path))
            id_map[path] = cat_id
            cat_id += 1
    # Level 2
    deep_cats = {
        'Electronics/Smartphones': ['Budget Phones', 'Flagship Phones', 'Foldable Phones'],
        'Electronics/Laptops': ['Gaming Laptops', 'Ultrabooks', 'Workstations'],
        'Clothing/Men': ['Casual Shirts', 'Formal Wear', 'Outerwear'],
        'Clothing/Women': ['Dresses', 'Activewear', 'Handbags'],
        'Home & Garden/Kitchen': ['Cookware', 'Small Appliances', 'Utensils'],
        'Sports & Outdoors/Fitness': ['Weights', 'Yoga', 'Cardio Machines'],
    }
    for parent_path, children in deep_cats.items():
        if parent_path in id_map:
            for child in children:
                path = f"{parent_path}/{child}"
                rows.append((cat_id, child, id_map[parent_path], 2, path))
                id_map[path] = cat_id
                cat_id += 1
    execute(conn,
        "INSERT INTO product_categories (id, name, parent_id, depth, path) VALUES (%s,%s,%s,%s,%s)",
        rows, many=True)
    log.info(f"  {len(rows)} categories seeded.")

def seed_departments(conn):
    """Seed department hierarchy for self-join + recursive CTE demos."""
    log.info("Seeding departments...")
    depts = [
        (1, 'Company', None, 10000000, 'CC-000'),
        (2, 'Engineering', 1, 5000000, 'CC-100'),
        (3, 'Sales', 1, 3000000, 'CC-200'),
        (4, 'Marketing', 1, 1500000, 'CC-300'),
        (5, 'Finance', 1, 800000, 'CC-400'),
        (6, 'HR', 1, 600000, 'CC-500'),
        (7, 'Backend', 2, 2000000, 'CC-110'),
        (8, 'Frontend', 2, 1500000, 'CC-120'),
        (9, 'DevOps', 2, 1000000, 'CC-130'),
        (10, 'Data', 2, 800000, 'CC-140'),
        (11, 'Enterprise Sales', 3, 1500000, 'CC-210'),
        (12, 'SMB Sales', 3, 800000, 'CC-220'),
        (13, 'Sales Engineering', 3, 700000, 'CC-230'),
        (14, 'Digital Marketing', 4, 800000, 'CC-310'),
        (15, 'Content', 4, 400000, 'CC-320'),
        (16, 'Accounting', 5, 400000, 'CC-410'),
        (17, 'FP&A', 5, 300000, 'CC-420'),
        (18, 'Recruiting', 6, 300000, 'CC-510'),
        (19, 'People Ops', 6, 200000, 'CC-520'),
        (20, 'QA', 2, 500000, 'CC-150'),
    ]
    execute(conn,
        "INSERT INTO departments (id, name, parent_dept_id, budget, cost_center) VALUES (%s,%s,%s,%s,%s)",
        depts, many=True)
    log.info(f"  {len(depts)} departments seeded.")

def seed_employees(conn, n=3000):
    """Seed employees with manager hierarchy for self-join + window function demos."""
    log.info(f"Seeding {n} employees...")
    dept_ids = list(range(7, 21))  # leaf departments
    # First create managers (top ~200 employees)
    rows = []
    for i in range(1, 201):
        rows.append((
            fake.first_name(), fake.last_name(), fake.email(),
            random.choice(dept_ids), None if i <= 20 else random.randint(1, 20),
            fake.date_between(start_date='-10y', end_date='-2y'),
            round(random.uniform(80000, 250000), 2),
            round(random.uniform(2.0, 5.0), 2), 1,
        ))
    # Then regular employees
    for _ in range(n - 200):
        dept = random.choice(dept_ids)
        rows.append((
            fake.first_name(), fake.last_name(), fake.email(),
            dept, random.randint(1, 200),
            fake.date_between(start_date='-5y', end_date='now'),
            round(random.uniform(35000, 150000), 2),
            round(random.uniform(1.0, 5.0), 2),
            random.choices([1, 0], weights=[90, 10])[0],
        ))
    execute(conn,
        "INSERT INTO employees (first_name,last_name,email,department_id,manager_id,"
        "hire_date,salary,performance_score,is_active) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows, many=True)
    log.info("Employees seeded.")

def seed_product_reviews(conn, n=8000):
    """Seed product reviews with fulltext content for FULLTEXT + aggregation demos."""
    log.info(f"Seeding {n} product reviews...")
    cur = conn.cursor()
    cur.execute("SELECT id FROM products LIMIT 400")
    prod_ids = [r[0] for r in cur.fetchall()]
    cur.execute("SELECT id FROM customers LIMIT 1000")
    cust_ids = [r[0] for r in cur.fetchall()]
    cur.close()
    if not prod_ids or not cust_ids:
        log.warning("No products/customers for reviews.")
        return
    sentiments = [
        "Excellent product, exceeded expectations. Build quality is superb and delivery was fast.",
        "Terrible experience. Product broke within a week. Would not recommend to anyone.",
        "Good value for money. Does exactly what it promises. Satisfied with purchase.",
        "Average quality. Nothing special but does the job. Packaging could be better.",
        "Outstanding performance and beautiful design. Best purchase I made this year.",
        "Disappointing quality. The description was misleading. Returning this item.",
        "Perfect gift idea. My family loved it. Will definitely buy more from this brand.",
        "Not worth the price. Cheaper alternatives available with same features.",
        "Incredible durability and comfort. Using it daily for months without any issues.",
        "Poor customer service when I had issues. Product itself is mediocre at best.",
        "This is a game changer. Revolutionary features that competitors lack.",
        "Overpriced for what you get. Expected much better quality at this price point.",
    ]
    titles = [
        "Amazing purchase!", "Total waste of money", "Great value", "Just okay",
        "Best ever!", "Very disappointed", "Perfect gift", "Not impressed",
        "Highly recommend", "Buyer beware", "Five stars!", "One star experience",
        "Solid product", "Could be better", "Exceeded expectations", "Below average",
    ]
    rows = []
    for _ in range(n):
        rows.append((
            random.choice(prod_ids), random.choice(cust_ids),
            random.choices([1, 2, 3, 4, 5], weights=[5, 10, 20, 35, 30])[0],
            random.choice(titles),
            random.choice(sentiments) + " " + fake.sentence(),
            random.randint(0, 200),
            random.choices([0, 1], weights=[30, 70])[0],
            fake.date_time_between(start_date='-1y', end_date='now'),
        ))
    execute(conn,
        "INSERT INTO product_reviews (product_id,customer_id,rating,review_title,"
        "review_text,helpful_votes,verified,created_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
        rows, many=True)
    log.info("Product reviews seeded.")

def seed_warehouses_and_stock(conn):
    """Seed warehouses + stock for multi-table join and inventory demos."""
    log.info("Seeding warehouses and stock...")
    wh_data = [
        ('East Coast Hub', 'East', 'US', 50000),
        ('West Coast Hub', 'West', 'US', 45000),
        ('Central Depot', 'Central', 'US', 30000),
        ('UK Warehouse', 'Europe', 'GB', 25000),
        ('Frankfurt Depot', 'Europe', 'DE', 35000),
        ('Tokyo Fulfillment', 'Asia', 'JP', 20000),
        ('Mumbai Center', 'Asia', 'IN', 28000),
        ('Sydney Depot', 'Oceania', 'AU', 15000),
        ('Sao Paulo Hub', 'LatAm', 'BR', 18000),
        ('Toronto Center', 'NorthAm', 'CA', 22000),
    ]
    execute(conn,
        "INSERT INTO warehouses (name, region, country, capacity) VALUES (%s,%s,%s,%s)",
        wh_data, many=True)

    cur = conn.cursor()
    cur.execute("SELECT id FROM products LIMIT 400")
    prod_ids = [r[0] for r in cur.fetchall()]
    cur.close()

    stock_rows = []
    for wh_id in range(1, 11):
        # Each warehouse carries 60-100% of products
        products_in_wh = random.sample(prod_ids, max(1, int(len(prod_ids) * random.uniform(0.6, 1.0))))
        for pid in products_in_wh:
            qty = random.randint(0, 2000)
            reserved = random.randint(0, min(qty, 200))
            stock_rows.append((wh_id, pid, qty, reserved, fake.date_between('-60d', 'today')))
    execute(conn,
        "INSERT INTO warehouse_stock (warehouse_id,product_id,quantity,reserved_qty,last_restock_date) "
        "VALUES (%s,%s,%s,%s,%s)", stock_rows, many=True)
    log.info(f"  {len(wh_data)} warehouses + {len(stock_rows)} stock rows seeded.")

def seed_suppliers(conn, n=150):
    """Seed suppliers and product-supplier relationships."""
    log.info(f"Seeding {n} suppliers...")
    rows = []
    for _ in range(n):
        rows.append((
            fake.company(), fake.country_code(),
            round(random.uniform(1.0, 5.0), 2),
            random.randint(3, 90),
            random.choices([1, 0], weights=[85, 15])[0],
        ))
    execute(conn,
        "INSERT INTO suppliers (name,country,rating,lead_time_days,is_active) VALUES (%s,%s,%s,%s,%s)",
        rows, many=True)

    cur = conn.cursor()
    cur.execute("SELECT id FROM products LIMIT 400")
    prod_ids = [r[0] for r in cur.fetchall()]
    cur.close()

    ps_rows = []
    for pid in prod_ids:
        n_suppliers = random.randint(1, 4)
        supplier_ids = random.sample(range(1, n + 1), min(n_suppliers, n))
        for i, sid in enumerate(supplier_ids):
            ps_rows.append((pid, sid, round(random.uniform(1.0, 500.0), 2), 1 if i == 0 else 0))
    execute(conn,
        "INSERT INTO product_suppliers (product_id,supplier_id,unit_cost,is_primary) VALUES (%s,%s,%s,%s)",
        ps_rows, many=True)
    log.info(f"  {n} suppliers + {len(ps_rows)} product_supplier links seeded.")

def seed_payment_transactions(conn):
    """Seed payment transactions for every order."""
    log.info("Seeding payment transactions...")
    cur = conn.cursor()
    cur.execute("SELECT id, total_amount, status, created_at FROM orders LIMIT 5000")
    orders = cur.fetchall()
    cur.close()
    if not orders:
        return

    methods = ['credit_card', 'debit_card', 'paypal', 'wire', 'crypto']
    method_weights = [40, 25, 20, 10, 5]
    currencies = ['USD', 'EUR', 'GBP', 'INR', 'JPY']
    statuses_map = {
        'pending': 'pending',
        'processing': 'authorized',
        'shipped': 'captured',
        'delivered': 'captured',
        'cancelled': 'refunded',
    }
    rows = []
    for oid, amount, status, created in orders:
        pay_status = statuses_map.get(status, 'pending')
        method = random.choices(methods, weights=method_weights)[0]
        rows.append((
            oid, method, float(amount) if amount else 0,
            random.choice(currencies), pay_status,
            fake.uuid4()[:32],
            round(random.uniform(0.0, 0.3), 4),
            created,
            created + timedelta(seconds=random.randint(1, 300)) if pay_status != 'pending' else None,
        ))
        # ~10% of orders have a second transaction (refund or retry)
        if random.random() < 0.10:
            rows.append((
                oid, method, float(amount) * random.uniform(0.1, 1.0) if amount else 0,
                random.choice(currencies),
                random.choice(['refunded', 'failed']),
                fake.uuid4()[:32],
                round(random.uniform(0.2, 0.9), 4),
                created + timedelta(days=random.randint(1, 30)),
                created + timedelta(days=random.randint(1, 30), seconds=random.randint(1, 300)),
            ))
    execute(conn,
        "INSERT INTO payment_transactions (order_id,payment_method,amount,currency,status,"
        "gateway_ref,fraud_score,created_at,processed_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows, many=True)
    log.info(f"  {len(rows)} payment transactions seeded.")

def seed_promotions(conn, n=50):
    """Seed promotions and link some orders to promotions."""
    log.info(f"Seeding {n} promotions...")
    cats = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Food', 'Toys', 'Beauty']
    tiers = [None, 'bronze', 'silver', 'gold', 'platinum']
    promo_rows = []
    for i in range(n):
        valid_from = fake.date_between('-6m', '-1m')
        valid_until = valid_from + timedelta(days=random.randint(7, 90))
        promo_rows.append((
            f"PROMO-{i:04d}", fake.catch_phrase()[:100],
            random.choice(['percentage', 'fixed', 'buy_x_get_y']),
            round(random.uniform(5, 40), 2),
            round(random.uniform(0, 100), 2),
            random.choice([None, 100, 500, 1000]),
            0, valid_from, valid_until,
            random.choice(tiers), random.choice([None] + cats),
        ))
    execute(conn,
        "INSERT INTO promotions (code,description,discount_type,discount_value,min_order_amount,"
        "max_uses,current_uses,valid_from,valid_until,target_tier,target_category) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", promo_rows, many=True)

    cur = conn.cursor()
    cur.execute("SELECT id FROM orders LIMIT 5000")
    oids = [r[0] for r in cur.fetchall()]
    cur.close()

    op_rows = []
    for oid in oids:
        if random.random() < 0.25:  # 25% of orders have a promo
            pid = random.randint(1, n)
            op_rows.append((oid, pid, round(random.uniform(2, 50), 2)))
    execute(conn,
        "INSERT INTO order_promotions (order_id,promotion_id,discount_amount) VALUES (%s,%s,%s)",
        op_rows, many=True)
    log.info(f"  {n} promotions + {len(op_rows)} order_promotion links seeded.")

def seed_inventory_log(conn, n=15000):
    """Seed inventory log entries for window function + running total demos."""
    log.info(f"Seeding {n} inventory_log entries...")
    cur = conn.cursor()
    cur.execute("SELECT id FROM products LIMIT 200")
    prod_ids = [r[0] for r in cur.fetchall()]
    cur.close()
    if not prod_ids:
        return

    types = ['receipt', 'sale', 'adjustment', 'transfer', 'return']
    type_weights = [20, 50, 10, 10, 10]
    rows = []
    for _ in range(n):
        change_type = random.choices(types, weights=type_weights)[0]
        qty = random.randint(1, 100) if change_type in ('receipt', 'return') else -random.randint(1, 50)
        if change_type == 'adjustment':
            qty = random.randint(-20, 20)
        rows.append((
            random.choice(prod_ids),
            random.randint(1, 10),
            change_type, qty,
            random.randint(1, 5000),
            fake.date_time_between(start_date='-6m', end_date='now'),
        ))
    execute(conn,
        "INSERT INTO inventory_log (product_id,warehouse_id,change_type,quantity_change,"
        "reference_id,created_at) VALUES (%s,%s,%s,%s,%s,%s)", rows, many=True)
    log.info("Inventory log seeded.")

def seed_all_new_tables(conn):
    """Seed all new tables required for complex explain plan scenarios."""
    seed_product_categories(conn)
    seed_departments(conn)
    seed_employees(conn)
    seed_product_reviews(conn)
    seed_warehouses_and_stock(conn)
    seed_suppliers(conn)
    seed_payment_transactions(conn)
    seed_promotions(conn)
    seed_inventory_log(conn)

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

        # 9. Clean up new tables (time-based entries)
        for tbl, col in [('product_reviews', 'created_at'), ('inventory_log', 'created_at'),
                          ('payment_transactions', 'created_at')]:
            try:
                cur.execute(f"DELETE FROM {tbl} WHERE {col} < %s", (cutoff_str,))
                cnt = cur.rowcount
                conn.commit()
                if cnt > 0:
                    log.info(f"  Deleted {cnt} rows from {tbl}")
            except Error:
                pass

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
# SCENARIO 19 — Complex Explain Plans (comprehensive coverage of ALL plan types)
# Dynatrace: Multi-node explain plans covering every access type (const, eq_ref,
#   ref, range, index, ALL, fulltext, index_merge), every select type (SIMPLE,
#   PRIMARY, SUBQUERY, DEPENDENT SUBQUERY, DERIVED, UNION, MATERIALIZED),
#   and every Extra (Using index, Using temporary, Using filesort, Using join
#   buffer, Using index condition, FirstMatch, LooseScan, etc.)
# Uses 19 tables with deep hierarchies, self-joins, CTEs, window functions,
#   fulltext search, semi-joins, anti-joins, and 8-10 table join chains.
# =============================================================================

def scenario_complex_plans(cfg, iterations=8):
    log.info("▶ SCENARIO 19: Complex explain plans (25 query patterns, all plan types)")
    conn = get_connection(cfg)

    # ── Q1: Recursive CTE — category hierarchy traversal ─────────────────
    # EXPLAIN: DERIVED + UNION + recursive materialization
    q01_recursive_cte = """
        WITH RECURSIVE cat_tree AS (
            SELECT id, name, parent_id, depth, path,
                   CAST(name AS CHAR(500)) AS full_path
            FROM product_categories
            WHERE parent_id IS NULL
            UNION ALL
            SELECT c.id, c.name, c.parent_id, c.depth, c.path,
                   CONCAT(ct.full_path, ' > ', c.name)
            FROM product_categories c
            JOIN cat_tree ct ON ct.id = c.parent_id
        )
        SELECT ct.full_path, ct.depth, COUNT(p.id) AS product_count,
               COALESCE(SUM(p.price), 0) AS total_value
        FROM cat_tree ct
        LEFT JOIN products p ON p.category = ct.name
        GROUP BY ct.id, ct.full_path, ct.depth
        HAVING product_count > 0
        ORDER BY ct.depth, total_value DESC
    """

    # ── Q2: Recursive CTE — employee management chain (self-join hierarchy) ──
    # EXPLAIN: DERIVED + recursive + self-join on manager_id
    q02_mgmt_chain = """
        WITH RECURSIVE mgmt_chain AS (
            SELECT id, first_name, last_name, manager_id, department_id,
                   salary, 1 AS chain_level,
                   CAST(CONCAT(first_name, ' ', last_name) AS CHAR(1000)) AS chain_path
            FROM employees
            WHERE manager_id IS NULL AND is_active = 1
            UNION ALL
            SELECT e.id, e.first_name, e.last_name, e.manager_id, e.department_id,
                   e.salary, mc.chain_level + 1,
                   CONCAT(mc.chain_path, ' > ', e.first_name, ' ', e.last_name)
            FROM employees e
            JOIN mgmt_chain mc ON mc.id = e.manager_id
            WHERE e.is_active = 1
        )
        SELECT mc.chain_level,
               COUNT(*) AS employee_count,
               AVG(mc.salary) AS avg_salary,
               MAX(mc.salary) AS max_salary,
               d.name AS department_name
        FROM mgmt_chain mc
        JOIN departments d ON d.id = mc.department_id
        GROUP BY mc.chain_level, d.name
        ORDER BY mc.chain_level, avg_salary DESC
    """

    # ── Q3: 10-table JOIN chain — full order lifecycle ────────────────────
    # EXPLAIN: 10 nested_loop nodes, mix of eq_ref/ref/ALL
    q03_ten_table_join = """
        SELECT c.email, c.tier, c.country,
               o.id AS order_id, o.status, o.total_amount,
               oi.quantity, oi.unit_price,
               p.name AS product_name, p.category, p.sku,
               pr.rating AS review_rating, pr.review_title,
               pt.payment_method, pt.status AS payment_status, pt.fraud_score,
               ws.quantity AS warehouse_qty, w.name AS warehouse_name, w.region,
               sup.name AS supplier_name, sup.lead_time_days,
               promo.code AS promo_code, op.discount_amount
        FROM customers c
        JOIN orders o            ON o.customer_id = c.id
        JOIN order_items oi      ON oi.order_id = o.id
        JOIN products p          ON p.id = oi.product_id
        LEFT JOIN product_reviews pr ON pr.product_id = p.id AND pr.customer_id = c.id
        JOIN payment_transactions pt ON pt.order_id = o.id
        LEFT JOIN warehouse_stock ws ON ws.product_id = p.id
        LEFT JOIN warehouses w       ON w.id = ws.warehouse_id
        LEFT JOIN product_suppliers ps ON ps.product_id = p.id AND ps.is_primary = 1
        LEFT JOIN suppliers sup      ON sup.id = ps.supplier_id
        LEFT JOIN order_promotions op ON op.order_id = o.id
        LEFT JOIN promotions promo   ON promo.id = op.promotion_id
        WHERE c.country IN ('US', 'GB', 'DE')
          AND o.status IN ('shipped', 'delivered')
          AND p.price > 20.00
        ORDER BY o.total_amount DESC
        LIMIT 100
    """

    # ── Q4: Employee self-join 3 levels — org chart query ─────────────────
    # EXPLAIN: 3 self-joins on employees, each eq_ref or ref
    q04_self_join_3lvl = """
        SELECT e.first_name AS employee,
               e.salary AS emp_salary,
               mgr.first_name AS manager,
               mgr.salary AS mgr_salary,
               dir.first_name AS director,
               dir.salary AS dir_salary,
               d.name AS department,
               d.budget AS dept_budget,
               e.salary / d.budget * 100 AS salary_pct_of_budget
        FROM employees e
        JOIN employees mgr   ON mgr.id = e.manager_id
        JOIN employees dir   ON dir.id = mgr.manager_id
        JOIN departments d   ON d.id = e.department_id
        WHERE e.is_active = 1
          AND e.performance_score >= 3.5
          AND e.salary > mgr.salary * 0.8
        ORDER BY e.salary DESC
        LIMIT 50
    """

    # ── Q5: Semi-join with EXISTS — FirstMatch/LooseScan optimization ─────
    # EXPLAIN: FirstMatch or MaterializeScan in Extra
    q05_semijoin_exists = """
        SELECT c.id, c.email, c.tier, c.country
        FROM customers c
        WHERE EXISTS (
            SELECT 1 FROM orders o
            WHERE o.customer_id = c.id
              AND o.status = 'delivered'
              AND o.total_amount > 200
        )
        AND EXISTS (
            SELECT 1 FROM product_reviews pr
            JOIN order_items oi ON oi.product_id = pr.product_id
            JOIN orders o2 ON o2.id = oi.order_id AND o2.customer_id = c.id
            WHERE pr.rating >= 4
        )
        AND c.tier IN ('gold', 'platinum')
        ORDER BY c.created_at DESC
        LIMIT 50
    """

    # ── Q6: Semi-join with IN + subquery — Materialize optimization ──────
    # EXPLAIN: MATERIALIZED select_type, materialized subquery
    q06_semijoin_in = """
        SELECT p.id, p.name, p.category, p.price, p.stock_qty
        FROM products p
        WHERE p.id IN (
            SELECT oi.product_id
            FROM order_items oi
            JOIN orders o ON o.id = oi.order_id
            WHERE o.status = 'delivered'
              AND o.created_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)
        )
        AND p.id IN (
            SELECT pr.product_id
            FROM product_reviews pr
            WHERE pr.rating >= 4 AND pr.verified = 1
            GROUP BY pr.product_id
            HAVING COUNT(*) >= 2
        )
        AND p.price BETWEEN 10 AND 500
        ORDER BY p.stock_qty ASC
        LIMIT 100
    """

    # ── Q7: NOT EXISTS anti-join vs LEFT JOIN IS NULL comparison ──────────
    # EXPLAIN: anti-join optimization, LEFT JOIN + IS NULL
    q07_antijoin = """
        SELECT c.id, c.email, c.tier, c.created_at,
               DATEDIFF(NOW(), c.created_at) AS account_age_days
        FROM customers c
        WHERE NOT EXISTS (
            SELECT 1 FROM orders o WHERE o.customer_id = c.id
        )
        AND NOT EXISTS (
            SELECT 1 FROM sessions s
            WHERE s.customer_id = c.id
              AND s.created_at >= DATE_SUB(NOW(), INTERVAL 60 DAY)
        )
        AND c.created_at < DATE_SUB(NOW(), INTERVAL 30 DAY)
        ORDER BY c.created_at ASC
        LIMIT 100
    """

    # ── Q8: Correlated subquery 4 levels deep ────────────────────────────
    # EXPLAIN: DEPENDENT SUBQUERY at multiple levels, select_id 1-4
    q08_correlated_4deep = """
        SELECT c.id, c.email, c.tier,
            (SELECT COUNT(DISTINCT o.id)
             FROM orders o
             WHERE o.customer_id = c.id
               AND o.total_amount > (
                   SELECT AVG(o2.total_amount) * 1.5
                   FROM orders o2
                   WHERE o2.customer_id = c.id
               )
               AND EXISTS (
                   SELECT 1 FROM order_items oi
                   WHERE oi.order_id = o.id
                     AND oi.unit_price > (
                         SELECT AVG(p.price)
                         FROM products p
                         WHERE p.category = (
                             SELECT p2.category FROM products p2
                             JOIN order_items oi2 ON oi2.product_id = p2.id
                             WHERE oi2.order_id = o.id
                             LIMIT 1
                         )
                     )
               )
            ) AS premium_order_count
        FROM customers c
        WHERE c.tier IN ('gold', 'platinum')
        ORDER BY premium_order_count DESC
        LIMIT 30
    """

    # ── Q9: Window functions — RANK, ROW_NUMBER, LAG, LEAD, running totals ─
    # EXPLAIN: DERIVED with window function materialization
    q09_window_functions = """
        SELECT *
        FROM (
            SELECT e.id, e.first_name, e.last_name,
                   d.name AS department,
                   e.salary,
                   RANK() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS salary_rank,
                   ROW_NUMBER() OVER (PARTITION BY e.department_id ORDER BY e.hire_date) AS seniority_num,
                   e.salary - LAG(e.salary) OVER (PARTITION BY e.department_id ORDER BY e.salary) AS salary_gap_to_prev,
                   LEAD(e.first_name) OVER (PARTITION BY e.department_id ORDER BY e.salary DESC) AS next_lower_paid,
                   SUM(e.salary) OVER (PARTITION BY e.department_id ORDER BY e.salary DESC
                                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_salary_total,
                   AVG(e.salary) OVER (PARTITION BY e.department_id) AS dept_avg_salary,
                   COUNT(*) OVER (PARTITION BY e.department_id) AS dept_headcount,
                   e.salary / AVG(e.salary) OVER (PARTITION BY e.department_id) AS salary_vs_avg_ratio
            FROM employees e
            JOIN departments d ON d.id = e.department_id
            WHERE e.is_active = 1
        ) AS ranked
        WHERE salary_rank <= 5
        ORDER BY department, salary_rank
    """

    # ── Q10: FULLTEXT search with relevance ranking + joins ──────────────
    # EXPLAIN: type=fulltext, ft_review index
    q10_fulltext = """
        SELECT p.id, p.name, p.category, p.price,
               pr.review_title, pr.review_text,
               MATCH(pr.review_title, pr.review_text) AGAINST('excellent quality durable' IN NATURAL LANGUAGE MODE) AS relevance,
               pr.rating, pr.helpful_votes,
               c.email AS reviewer_email,
               sup.name AS supplier_name
        FROM product_reviews pr
        JOIN products p ON p.id = pr.product_id
        JOIN customers c ON c.id = pr.customer_id
        LEFT JOIN product_suppliers ps ON ps.product_id = p.id AND ps.is_primary = 1
        LEFT JOIN suppliers sup ON sup.id = ps.supplier_id
        WHERE MATCH(pr.review_title, pr.review_text) AGAINST('excellent quality durable' IN NATURAL LANGUAGE MODE)
          AND pr.verified = 1
        ORDER BY relevance DESC, pr.helpful_votes DESC
        LIMIT 50
    """

    # ── Q11: Index merge (intersection) — multiple single-column indexes ──
    # EXPLAIN: type=index_merge, Using intersect(idx_a, idx_b)
    q11_index_merge = """
        SELECT id, first_name, last_name, email, salary, hire_date
        FROM employees
        WHERE department_id = 7
          AND salary > 100000
          AND is_active = 1
        ORDER BY salary DESC
    """

    # ── Q12: Covering index scan — Using index only ──────────────────────
    # EXPLAIN: Extra = Using index (no table lookup needed)
    q12_covering_index = """
        SELECT product_id, rating
        FROM product_reviews
        WHERE product_id BETWEEN 10 AND 50
          AND rating >= 4
    """

    # ── Q13: UNION ALL with 4 diverse branches ──────────────────────────
    # EXPLAIN: PRIMARY, UNION, UNION RESULT, DERIVED select types
    q13_union_4branch = """
        (SELECT 'top_spender' AS segment, c.id, c.email,
                SUM(o.total_amount) AS metric_value, c.country
         FROM customers c
         JOIN orders o ON o.customer_id = c.id
         WHERE o.status = 'delivered'
         GROUP BY c.id, c.email, c.country
         HAVING metric_value > 500)

        UNION ALL

        (SELECT 'prolific_reviewer' AS segment, c.id, c.email,
                COUNT(pr.id) AS metric_value, c.country
         FROM customers c
         JOIN product_reviews pr ON pr.customer_id = c.id
         WHERE pr.verified = 1
         GROUP BY c.id, c.email, c.country
         HAVING metric_value >= 5)

        UNION ALL

        (SELECT 'multi_method_payer' AS segment, c.id, c.email,
                COUNT(DISTINCT pt.payment_method) AS metric_value, c.country
         FROM customers c
         JOIN orders o ON o.customer_id = c.id
         JOIN payment_transactions pt ON pt.order_id = o.id
         GROUP BY c.id, c.email, c.country
         HAVING metric_value >= 3)

        UNION ALL

        (SELECT 'promo_hunter' AS segment, c.id, c.email,
                COUNT(DISTINCT op.promotion_id) AS metric_value, c.country
         FROM customers c
         JOIN orders o ON o.customer_id = c.id
         JOIN order_promotions op ON op.order_id = o.id
         GROUP BY c.id, c.email, c.country
         HAVING metric_value >= 2)

        ORDER BY metric_value DESC
        LIMIT 100
    """

    # ── Q14: Multi-level aggregation (aggregation of aggregation) ────────
    # EXPLAIN: 2 DERIVED layers, Using temporary + Using filesort
    q14_multi_agg = """
        SELECT region_summary.region,
               AVG(region_summary.warehouse_revenue) AS avg_warehouse_revenue,
               SUM(region_summary.warehouse_revenue) AS total_region_revenue,
               MAX(region_summary.product_count) AS max_products_per_warehouse,
               COUNT(*) AS warehouse_count
        FROM (
            SELECT w.region, w.name AS warehouse_name,
                   COUNT(DISTINCT ws.product_id) AS product_count,
                   SUM(ws.quantity * p.price) AS warehouse_revenue,
                   AVG(ws.quantity) AS avg_stock_level
            FROM warehouses w
            JOIN warehouse_stock ws ON ws.warehouse_id = w.id
            JOIN products p ON p.id = ws.product_id
            WHERE ws.quantity > 0
            GROUP BY w.id, w.region, w.name
            HAVING warehouse_revenue > 100
        ) AS region_summary
        GROUP BY region_summary.region
        ORDER BY total_region_revenue DESC
    """

    # ── Q15: Pivot via conditional aggregation — cross-tab report ────────
    # EXPLAIN: Multi-table JOIN + GROUP BY + CASE aggregation + filesort
    q15_pivot = """
        SELECT p.category,
               COUNT(CASE WHEN c.tier = 'bronze'   THEN 1 END) AS bronze_orders,
               COUNT(CASE WHEN c.tier = 'silver'   THEN 1 END) AS silver_orders,
               COUNT(CASE WHEN c.tier = 'gold'     THEN 1 END) AS gold_orders,
               COUNT(CASE WHEN c.tier = 'platinum' THEN 1 END) AS platinum_orders,
               SUM(CASE WHEN c.tier = 'platinum' THEN oi.quantity * oi.unit_price ELSE 0 END) AS platinum_revenue,
               SUM(CASE WHEN c.tier = 'gold' THEN oi.quantity * oi.unit_price ELSE 0 END) AS gold_revenue,
               AVG(CASE WHEN pr.rating IS NOT NULL THEN pr.rating END) AS avg_review_rating,
               COUNT(DISTINCT sup.id) AS supplier_count
        FROM products p
        JOIN order_items oi ON oi.product_id = p.id
        JOIN orders o ON o.id = oi.order_id
        JOIN customers c ON c.id = o.customer_id
        LEFT JOIN product_reviews pr ON pr.product_id = p.id
        LEFT JOIN product_suppliers ps ON ps.product_id = p.id
        LEFT JOIN suppliers sup ON sup.id = ps.supplier_id
        WHERE o.status NOT IN ('cancelled')
        GROUP BY p.category
        ORDER BY platinum_revenue DESC
    """

    # ── Q16: Subquery with ALL/ANY range comparison ──────────────────────
    # EXPLAIN: SUBQUERY + range comparison, dependent/non-dependent
    q16_all_any = """
        SELECT e.id, e.first_name, e.last_name, e.salary,
               d.name AS department, d.budget
        FROM employees e
        JOIN departments d ON d.id = e.department_id
        WHERE e.salary > ALL (
            SELECT AVG(e2.salary)
            FROM employees e2
            WHERE e2.department_id != e.department_id
              AND e2.is_active = 1
            GROUP BY e2.department_id
        )
        AND e.performance_score >= ANY (
            SELECT MAX(e3.performance_score) - 0.5
            FROM employees e3
            WHERE e3.department_id = e.department_id
        )
        ORDER BY e.salary DESC
        LIMIT 20
    """

    # ── Q17: Inventory running totals with window frames ─────────────────
    # EXPLAIN: DERIVED with complex window, filesort for ORDER BY
    q17_running_totals = """
        SELECT inv.*,
               p.name AS product_name, p.category,
               w.name AS warehouse_name
        FROM (
            SELECT il.product_id, il.warehouse_id,
                   il.change_type, il.quantity_change,
                   il.created_at,
                   SUM(il.quantity_change) OVER (
                       PARTITION BY il.product_id, il.warehouse_id
                       ORDER BY il.created_at
                       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                   ) AS running_balance,
                   AVG(il.quantity_change) OVER (
                       PARTITION BY il.product_id, il.warehouse_id
                       ORDER BY il.created_at
                       ROWS BETWEEN 5 PRECEDING AND CURRENT ROW
                   ) AS moving_avg_6,
                   ROW_NUMBER() OVER (
                       PARTITION BY il.product_id, il.warehouse_id
                       ORDER BY il.created_at DESC
                   ) AS recency_rank
            FROM inventory_log il
            WHERE il.created_at >= DATE_SUB(NOW(), INTERVAL 3 MONTH)
        ) AS inv
        JOIN products p ON p.id = inv.product_id
        JOIN warehouses w ON w.id = inv.warehouse_id
        WHERE inv.recency_rank <= 10
        ORDER BY inv.product_id, inv.warehouse_id, inv.created_at DESC
        LIMIT 200
    """

    # ── Q18: Complex CASE with scalar subqueries per row ─────────────────
    # EXPLAIN: Multiple DEPENDENT SUBQUERY nodes, heavy per-row overhead
    q18_case_subqueries = """
        SELECT p.id, p.name, p.category, p.price,
               CASE
                   WHEN (SELECT AVG(pr.rating) FROM product_reviews pr WHERE pr.product_id = p.id) >= 4.5
                        AND (SELECT COUNT(*) FROM product_reviews pr2 WHERE pr2.product_id = p.id) >= 10
                   THEN 'star_product'
                   WHEN (SELECT SUM(oi.quantity) FROM order_items oi WHERE oi.product_id = p.id) >
                        (SELECT AVG(total_qty) FROM (
                            SELECT SUM(oi2.quantity) AS total_qty
                            FROM order_items oi2
                            GROUP BY oi2.product_id
                        ) AS avg_sales)
                   THEN 'high_volume'
                   WHEN (SELECT COUNT(DISTINCT ws.warehouse_id) FROM warehouse_stock ws WHERE ws.product_id = p.id AND ws.quantity > 0) >= 5
                   THEN 'widely_stocked'
                   ELSE 'standard'
               END AS product_tier,
               (SELECT GROUP_CONCAT(DISTINCT sup.country ORDER BY sup.country)
                FROM product_suppliers ps2
                JOIN suppliers sup ON sup.id = ps2.supplier_id
                WHERE ps2.product_id = p.id) AS supplier_countries
        FROM products p
        WHERE p.price > 10
        ORDER BY p.price DESC
        LIMIT 100
    """

    # ── Q19: Non-recursive CTE with multiple references ──────────────────
    # EXPLAIN: CTE materialized once, referenced twice
    q19_cte_multi_ref = """
        WITH customer_metrics AS (
            SELECT c.id AS customer_id, c.email, c.tier, c.country,
                   COUNT(DISTINCT o.id) AS order_count,
                   COALESCE(SUM(o.total_amount), 0) AS total_spent,
                   MAX(o.created_at) AS last_order_date
            FROM customers c
            LEFT JOIN orders o ON o.customer_id = c.id AND o.status != 'cancelled'
            GROUP BY c.id, c.email, c.tier, c.country
        ),
        tier_benchmarks AS (
            SELECT tier,
                   AVG(total_spent) AS avg_spent,
                   AVG(order_count) AS avg_orders,
                   MAX(total_spent) AS max_spent,
                   MIN(total_spent) AS min_spent
            FROM customer_metrics
            GROUP BY tier
        )
        SELECT cm.customer_id, cm.email, cm.tier, cm.country,
               cm.order_count, cm.total_spent,
               tb.avg_spent AS tier_avg_spent,
               cm.total_spent - tb.avg_spent AS vs_tier_avg,
               CASE WHEN cm.total_spent > tb.avg_spent * 2 THEN 'whale'
                    WHEN cm.total_spent > tb.avg_spent     THEN 'above_avg'
                    ELSE 'below_avg'
               END AS classification
        FROM customer_metrics cm
        JOIN tier_benchmarks tb ON tb.tier = cm.tier
        WHERE cm.order_count > 0
        ORDER BY cm.total_spent DESC
        LIMIT 100
    """

    # ── Q20: Complex UPDATE with subquery (DML explain) ──────────────────
    # EXPLAIN: Shows UPDATE plan with subquery for SET and WHERE
    q20_update_subquery = """
        SELECT p.id, p.stock_qty,
            (SELECT COALESCE(SUM(ws.quantity - ws.reserved_qty), 0)
             FROM warehouse_stock ws
             WHERE ws.product_id = p.id) AS available_stock,
            (SELECT COUNT(DISTINCT oi.order_id)
             FROM order_items oi
             JOIN orders o ON o.id = oi.order_id
             WHERE oi.product_id = p.id
               AND o.status = 'pending') AS pending_orders
        FROM products p
        WHERE p.stock_qty != (
            SELECT COALESCE(SUM(ws2.quantity - ws2.reserved_qty), 0)
            FROM warehouse_stock ws2
            WHERE ws2.product_id = p.id
        )
        LIMIT 50
    """

    # ── Q21: 8-table supply chain analytics ──────────────────────────────
    # EXPLAIN: 8 tables joined, mix of INNER/LEFT, aggregation, filesort
    q21_supply_chain = """
        SELECT sup.name AS supplier_name, sup.country AS supplier_country,
               sup.rating AS supplier_rating, sup.lead_time_days,
               COUNT(DISTINCT p.id) AS products_supplied,
               COUNT(DISTINCT o.id) AS orders_fulfilled,
               SUM(oi.quantity * oi.unit_price) AS total_revenue_generated,
               AVG(pr.rating) AS avg_product_rating,
               SUM(ws.quantity) AS total_warehouse_stock,
               COUNT(DISTINCT w.id) AS warehouses_stocked
        FROM suppliers sup
        JOIN product_suppliers ps ON ps.supplier_id = sup.id
        JOIN products p ON p.id = ps.product_id
        JOIN order_items oi ON oi.product_id = p.id
        JOIN orders o ON o.id = oi.order_id AND o.status IN ('shipped', 'delivered')
        LEFT JOIN product_reviews pr ON pr.product_id = p.id AND pr.verified = 1
        LEFT JOIN warehouse_stock ws ON ws.product_id = p.id AND ws.quantity > 0
        LEFT JOIN warehouses w ON w.id = ws.warehouse_id
        WHERE sup.is_active = 1
        GROUP BY sup.id, sup.name, sup.country, sup.rating, sup.lead_time_days
        HAVING total_revenue_generated > 100
        ORDER BY total_revenue_generated DESC
        LIMIT 50
    """

    # ── Q22: DEPENDENT UNION — correlated union branches ─────────────────
    # EXPLAIN: DEPENDENT UNION select_type
    q22_dependent_union = """
        SELECT c.id, c.email, c.tier,
               (SELECT MAX(val) FROM (
                   SELECT SUM(o.total_amount) AS val
                   FROM orders o
                   WHERE o.customer_id = c.id AND o.status = 'delivered'
                   UNION ALL
                   SELECT COUNT(*) * 100 AS val
                   FROM product_reviews pr
                   WHERE pr.customer_id = c.id AND pr.rating = 5
                   UNION ALL
                   SELECT COUNT(DISTINCT s.id) * 50 AS val
                   FROM sessions s
                   WHERE s.customer_id = c.id
               ) AS scores) AS engagement_score
        FROM customers c
        WHERE c.tier IN ('gold', 'platinum')
        ORDER BY engagement_score DESC
        LIMIT 50
    """

    # ── Q23: Fraud detection — complex multi-table analytics ─────────────
    # EXPLAIN: 7 tables, subqueries in WHERE, HAVING, derived tables
    q23_fraud_detection = """
        SELECT c.id AS customer_id, c.email, c.tier, c.country,
               order_stats.order_count,
               order_stats.total_amount,
               payment_stats.failed_payments,
               payment_stats.avg_fraud_score,
               review_stats.review_count,
               review_stats.avg_rating
        FROM customers c
        JOIN (
            SELECT o.customer_id,
                   COUNT(*) AS order_count,
                   SUM(o.total_amount) AS total_amount,
                   AVG(o.total_amount) AS avg_order_value
            FROM orders o
            WHERE o.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
            GROUP BY o.customer_id
            HAVING order_count >= 3
        ) AS order_stats ON order_stats.customer_id = c.id
        JOIN (
            SELECT o2.customer_id,
                   SUM(CASE WHEN pt.status = 'failed' THEN 1 ELSE 0 END) AS failed_payments,
                   AVG(pt.fraud_score) AS avg_fraud_score,
                   COUNT(DISTINCT pt.payment_method) AS methods_used
            FROM payment_transactions pt
            JOIN orders o2 ON o2.id = pt.order_id
            GROUP BY o2.customer_id
        ) AS payment_stats ON payment_stats.customer_id = c.id
        LEFT JOIN (
            SELECT pr.customer_id,
                   COUNT(*) AS review_count,
                   AVG(pr.rating) AS avg_rating
            FROM product_reviews pr
            GROUP BY pr.customer_id
        ) AS review_stats ON review_stats.customer_id = c.id
        WHERE payment_stats.avg_fraud_score > 0.1
           OR payment_stats.failed_payments >= 2
           OR (order_stats.order_count > 10 AND payment_stats.methods_used >= 3)
        ORDER BY payment_stats.avg_fraud_score DESC
        LIMIT 50
    """

    # ── Q24: Department budget analysis — cross-level hierarchy ──────────
    # EXPLAIN: Self-join on departments + employees aggregation + HAVING
    q24_dept_budget = """
        SELECT parent_d.name AS division,
               child_d.name AS department,
               child_d.budget,
               emp_stats.headcount,
               emp_stats.total_salary,
               emp_stats.avg_salary,
               emp_stats.total_salary / child_d.budget * 100 AS salary_budget_pct,
               emp_stats.top_performer_salary,
               CASE
                   WHEN emp_stats.total_salary > child_d.budget * 0.9 THEN 'OVER_BUDGET_RISK'
                   WHEN emp_stats.total_salary > child_d.budget * 0.7 THEN 'ON_TRACK'
                   ELSE 'UNDER_UTILIZED'
               END AS budget_status
        FROM departments parent_d
        JOIN departments child_d ON child_d.parent_dept_id = parent_d.id
        JOIN (
            SELECT e.department_id,
                   COUNT(*) AS headcount,
                   SUM(e.salary) AS total_salary,
                   AVG(e.salary) AS avg_salary,
                   MAX(CASE WHEN e.performance_score >= 4.5 THEN e.salary END) AS top_performer_salary
            FROM employees e
            WHERE e.is_active = 1
            GROUP BY e.department_id
            HAVING headcount >= 5
        ) AS emp_stats ON emp_stats.department_id = child_d.id
        ORDER BY salary_budget_pct DESC
    """

    # ── Q25: Promotion effectiveness — full funnel analysis ──────────────
    # EXPLAIN: 9 tables joined, CASE aggregation, GROUP BY, HAVING, ORDER BY
    q25_promo_effectiveness = """
        SELECT promo.code, promo.description,
               promo.discount_type, promo.discount_value,
               promo.target_tier, promo.target_category,
               COUNT(DISTINCT o.id) AS orders_using_promo,
               COUNT(DISTINCT c.id) AS unique_customers,
               SUM(o.total_amount) AS total_order_revenue,
               SUM(op.discount_amount) AS total_discount_given,
               SUM(o.total_amount) - SUM(op.discount_amount) AS net_revenue,
               AVG(o.total_amount) AS avg_order_value,
               SUM(CASE WHEN pt.status = 'captured' THEN pt.amount ELSE 0 END) AS captured_payments,
               SUM(CASE WHEN pt.status = 'refunded' THEN pt.amount ELSE 0 END) AS refunded_amount,
               AVG(pr.rating) AS avg_product_rating_on_promo_orders,
               COUNT(DISTINCT p.category) AS categories_purchased
        FROM promotions promo
        JOIN order_promotions op ON op.promotion_id = promo.id
        JOIN orders o ON o.id = op.order_id
        JOIN customers c ON c.id = o.customer_id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p ON p.id = oi.product_id
        LEFT JOIN payment_transactions pt ON pt.order_id = o.id AND pt.status IN ('captured', 'refunded')
        LEFT JOIN product_reviews pr ON pr.product_id = p.id AND pr.customer_id = c.id
        WHERE promo.valid_from >= DATE_SUB(NOW(), INTERVAL 6 MONTH)
        GROUP BY promo.id, promo.code, promo.description,
                 promo.discount_type, promo.discount_value,
                 promo.target_tier, promo.target_category
        HAVING orders_using_promo >= 2
        ORDER BY net_revenue DESC
        LIMIT 30
    """

    all_complex_queries = [
        ("Q01: Recursive CTE — category hierarchy",           q01_recursive_cte, None),
        ("Q02: Recursive CTE — management chain",             q02_mgmt_chain, None),
        ("Q03: 10-table JOIN — full order lifecycle",          q03_ten_table_join, None),
        ("Q04: Self-join 3 levels — org chart",                q04_self_join_3lvl, None),
        ("Q05: Semi-join EXISTS — FirstMatch",                 q05_semijoin_exists, None),
        ("Q06: Semi-join IN — Materialize",                    q06_semijoin_in, None),
        ("Q07: NOT EXISTS anti-join",                          q07_antijoin, None),
        ("Q08: Correlated subquery 4 levels deep",             q08_correlated_4deep, None),
        ("Q09: Window functions (RANK/LAG/LEAD/running)",      q09_window_functions, None),
        ("Q10: FULLTEXT search + relevance ranking",           q10_fulltext, None),
        ("Q11: Index merge (intersection)",                    q11_index_merge, None),
        ("Q12: Covering index scan",                           q12_covering_index, None),
        ("Q13: UNION ALL 4 branches",                          q13_union_4branch, None),
        ("Q14: Multi-level aggregation",                       q14_multi_agg, None),
        ("Q15: Pivot via conditional aggregation",             q15_pivot, None),
        ("Q16: ALL/ANY subquery comparison",                   q16_all_any, None),
        ("Q17: Window frame running totals",                   q17_running_totals, None),
        ("Q18: CASE with scalar subqueries",                   q18_case_subqueries, None),
        ("Q19: CTE with multiple references",                  q19_cte_multi_ref, None),
        ("Q20: DML-style complex SELECT",                      q20_update_subquery, None),
        ("Q21: 8-table supply chain analytics",                q21_supply_chain, None),
        ("Q22: DEPENDENT UNION — correlated branches",         q22_dependent_union, None),
        ("Q23: Fraud detection — multi-derived-table",         q23_fraud_detection, None),
        ("Q24: Dept budget — self-join + HAVING + CASE",       q24_dept_budget, None),
        ("Q25: Promo effectiveness — 9-table funnel",          q25_promo_effectiveness, None),
    ]

    # Run EXPLAIN on all complex queries to populate Dynatrace explain plans
    log.info(f"  Running EXPLAIN on {len(all_complex_queries)} complex query patterns...")
    for label, sql, params in all_complex_queries:
        log.info(f"  --- {label} ---")
        explain_query(conn, sql, params)

    # Execute queries repeatedly to build up statistics in performance_schema
    log.info(f"  Executing complex queries ({iterations} rounds)...")
    for i in range(iterations):
        for label, sql, params in all_complex_queries:
            try:
                execute(conn, sql, params, fetch=True)
            except Exception as e:
                log.debug(f"  Query error ({label}): {e}")
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

    # Process explain plan queue — runs real EXPLAIN FORMAT=JSON on queued queries
    process_explain_queue(cfg, batch_size=200)

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
        choices=list(ALL_SCENARIOS.keys()) + ["all", "seed", "cleanup", "process_explains"],
        default="all",
        help="Scenario to run (default: all). Use 'process_explains' to only process the explain queue.")
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
            seed_all_new_tables(conn)
        else:
            # Check if new tables need seeding (upgrade path)
            new_check = execute(conn, "SELECT COUNT(*) as c FROM employees", fetch=True)
            new_count = new_check[0]['c'] if new_check else 0
            if new_count < 100:
                log.info("Seeding new tables for complex explain plan scenarios...")
                seed_all_new_tables(conn)
            log.info(f"Existing data found ({count} customers). Use --skip-seed to suppress this check.")
        conn.close()

    if args.scenario == "seed":
        log.info("Seed-only complete.")
        return

    if args.scenario == "cleanup":
        cleanup_old_data(args, days=args.cleanup_days)
        return

    if args.scenario == "process_explains":
        process_explain_queue(args, batch_size=500)
        return

    # Optional cleanup before running scenarios
    if args.cleanup_before:
        cleanup_old_data(args, days=args.cleanup_days)

    if args.scenario == "all":
        run_all_scenarios(args)
    else:
        log.info(f"Running: {args.scenario}")
        ALL_SCENARIOS[args.scenario](args)

    # Process explain plan queue after scenarios (unless it was the explicit scenario)
    if args.scenario != "process_explains":
        process_explain_queue(args, batch_size=200)

    # Optional cleanup after running scenarios
    if args.cleanup_after:
        cleanup_old_data(args, days=args.cleanup_days)

if __name__ == "__main__":
    main()
