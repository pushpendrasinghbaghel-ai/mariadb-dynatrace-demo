# MariaDB Dynatrace Observability Demo

Generate synthetic database workloads against MariaDB to showcase Dynatrace Database monitoring capabilities.

## Features

- **10 realistic database scenarios** — Full table scans, N+1 queries, lock contention, heavy aggregations, and more
- **Automatic data cleanup** — Removes data older than N days to prevent unbounded growth
- **Scheduled execution** — GitHub Actions workflow or systemd timer for recurring runs
- **Seed data generation** — Creates ~2000 customers, 500 products, 5000 orders with realistic Faker data

## Quick Start

```bash
pip install mysql-connector-python faker

# Run all scenarios
python mariadb_dynatrace_generator.py --host 127.0.0.1 --user root --password secret

# Run specific scenario
python mariadb_dynatrace_generator.py --scenario lock_contention

# Cleanup old data
python mariadb_dynatrace_generator.py --scenario cleanup --cleanup-days 10
```

## Scenarios

| Scenario | Dynatrace Signal |
|----------|-----------------|
| `full_table_scan` | High logical reads, type=ALL in EXPLAIN |
| `n_plus_1` | Burst of identical short queries |
| `heavy_aggregation` | Long query duration, temp tables |
| `lock_contention` | Lock wait time, transaction waits |
| `high_frequency` | High connection count, short transactions |
| `slow_inserts` | Write latency, autocommit overhead |
| `index_change` | Query plan regression/improvement |
| `mixed_oltp` | Normal throughput baseline |
| `temp_table_filesort` | sort_merge_passes, tmp_disk_tables |
| `long_transaction` | Long-running transaction alerts |

## Deployment

See [DEPLOYMENT.md](DEPLOYMENT.md) for:
- GitHub Actions setup (recommended)
- Ubuntu systemd service/timer
- Simple cron alternative

## CLI Options

```
--host          Database host (default: 127.0.0.1)
--port          Database port (default: 3306)
--user          Database user (default: root)
--password      Database password
--database      Database name (default: dynatrace_demo)
--scenario      Scenario to run: all, seed, cleanup, or specific name
--skip-seed     Skip seeding if data exists
--cleanup-days  Delete data older than N days (default: 10)
--cleanup-before Run cleanup before scenarios
--cleanup-after  Run cleanup after scenarios
```

## License

MIT
