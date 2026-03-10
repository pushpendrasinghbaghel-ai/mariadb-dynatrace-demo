# MariaDB Dynatrace Observability Demo - Deployment Guide

This guide covers scheduled execution and automatic data cleanup for the MariaDB Dynatrace demo.

## New CLI Options

```bash
# Run cleanup only (delete data older than 10 days)
python mariadb_dynatrace_generator.py --scenario cleanup --cleanup-days 10

# Run all scenarios with cleanup before
python mariadb_dynatrace_generator.py --cleanup-before --skip-seed

# Custom cleanup retention period (e.g., 7 days)
python mariadb_dynatrace_generator.py --cleanup-before --cleanup-days 7 --skip-seed
```

---

## Option 1: GitHub Actions (Recommended)

**Best for:** Cloud-hosted MariaDB, minimal maintenance, easy monitoring via GitHub UI.

### Setup

1. **Push the workflow file** to your repository:
   ```
   .github/workflows/mariadb-dynatrace-demo.yml
   ```

2. **Add repository secrets** (Settings → Secrets → Actions):
   | Secret | Description |
   |--------|-------------|
   | `MARIADB_HOST` | Database hostname |
   | `MARIADB_PORT` | Port (default: 3306) |
   | `MARIADB_USER` | Database user |
   | `MARIADB_PASSWORD` | Database password |
   | `MARIADB_DATABASE` | Database name (default: dynatrace_demo) |

3. **Schedule runs automatically** every 4 hours, or trigger manually from Actions tab.

### Manual Run

Go to **Actions** → **MariaDB Dynatrace Demo** → **Run workflow** to:
- Choose specific scenario
- Toggle cleanup/seeding
- Set retention days

---

## Option 2: Ubuntu Systemd Service

**Best for:** Self-hosted MariaDB, on-premise deployments, air-gapped environments.

### Quick Setup

```bash
# Copy files to your server
scp mariadb_dynatrace_generator.py setup_systemd.sh user@server:~/

# SSH to server and run setup
ssh user@server
chmod +x setup_systemd.sh

# Configure credentials via environment variables
export MARIADB_HOST="your-db-host"
export MARIADB_PORT="3306"
export MARIADB_USER="demo_user"
export MARIADB_PASSWORD="your-password"
export MARIADB_DATABASE="dynatrace_demo"

# Run setup
sudo -E ./setup_systemd.sh
```

### Manage the Service

```bash
# Check next scheduled run
systemctl list-timers mariadb-dynatrace-demo.timer

# View logs
journalctl -u mariadb-dynatrace-demo.service -f

# Run immediately
sudo systemctl start mariadb-dynatrace-demo.service

# Stop scheduled runs
sudo systemctl stop mariadb-dynatrace-demo.timer

# Update credentials
sudo nano /opt/mariadb-dynatrace-demo/mariadb-demo.env
```

### Change Schedule

Edit `/etc/systemd/system/mariadb-dynatrace-demo.timer`:

```ini
[Timer]
# Every 2 hours
OnCalendar=*-*-* 00/2:00:00

# Every 6 hours
OnCalendar=*-*-* 00/6:00:00

# Daily at 3 AM
OnCalendar=*-*-* 03:00:00
```

Then reload:
```bash
sudo systemctl daemon-reload
sudo systemctl restart mariadb-dynatrace-demo.timer
```

---

## Option 3: Simple Cron (Alternative)

For quick setup without systemd:

```bash
# Install dependencies
pip install mysql-connector-python faker

# Add to crontab (run every 4 hours)
crontab -e
```

Add this line:
```cron
0 */4 * * * cd /path/to/demo && /usr/bin/python3 mariadb_dynatrace_generator.py --host DB_HOST --user USER --password PASS --skip-seed --cleanup-before >> /var/log/mariadb-demo.log 2>&1
```

---

## Data Cleanup Details

The `--cleanup-before` flag removes data older than N days (default: 10) from:

| Table | Cleanup Criteria |
|-------|-----------------|
| `events` | `created_at < cutoff` |
| `sessions` | `created_at < cutoff` |
| `sales_facts` | `sale_date < cutoff` |
| `order_items` | Linked to deleted orders |
| `orders` | `created_at < cutoff` |
| `customers` | Orphaned (no orders) + `created_at < cutoff` |
| `products` | Orphaned (no order_items) + `created_at < cutoff` |

This keeps the database at a manageable size while preserving recent data for monitoring.

---

## Recommended Production Settings

```bash
python mariadb_dynatrace_generator.py \
    --host $DB_HOST \
    --user $DB_USER \
    --password $DB_PASSWORD \
    --scenario all \
    --skip-seed \
    --cleanup-before \
    --cleanup-days 10
```

- `--skip-seed`: Avoids duplicate seed data across runs
- `--cleanup-before`: Removes old data before generating new workloads
- `--cleanup-days 10`: 10-day retention balances demo data volume vs. cleanup frequency
