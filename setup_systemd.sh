#!/bin/bash
# =============================================================================
# MariaDB Dynatrace Demo - Ubuntu Systemd Setup
# =============================================================================
# This script sets up a systemd timer to run the demo every 4 hours
#
# Usage:
#   chmod +x setup_systemd.sh
#   sudo ./setup_systemd.sh
#
# Configuration: Edit the variables below before running
# =============================================================================

set -e

# ─── Configuration ────────────────────────────────────────────────────────────
INSTALL_DIR="/opt/mariadb-dynatrace-demo"
SERVICE_USER="mariadb-demo"
VENV_DIR="${INSTALL_DIR}/venv"

# Database credentials (will be stored in environment file)
DB_HOST="${MARIADB_HOST:-127.0.0.1}"
DB_PORT="${MARIADB_PORT:-3306}"
DB_USER="${MARIADB_USER:-root}"
DB_PASSWORD="${MARIADB_PASSWORD:-}"
DB_NAME="${MARIADB_DATABASE:-dynatrace_demo}"

# Schedule: every 4 hours
TIMER_SCHEDULE="*-*-* 00/4:00:00"

# ─── Color output ─────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log() { echo -e "${GREEN}[✓]${NC} $1"; }
warn() { echo -e "${YELLOW}[!]${NC} $1"; }
error() { echo -e "${RED}[✗]${NC} $1"; exit 1; }

# ─── Pre-flight checks ────────────────────────────────────────────────────────
[[ $EUID -ne 0 ]] && error "This script must be run as root (use sudo)"

log "Starting MariaDB Dynatrace Demo setup..."

# ─── Create service user ──────────────────────────────────────────────────────
if ! id -u "${SERVICE_USER}" &>/dev/null; then
    log "Creating service user: ${SERVICE_USER}"
    useradd --system --no-create-home --shell /usr/sbin/nologin "${SERVICE_USER}"
else
    log "Service user ${SERVICE_USER} already exists"
fi

# ─── Create installation directory ────────────────────────────────────────────
log "Creating installation directory: ${INSTALL_DIR}"
mkdir -p "${INSTALL_DIR}"

# ─── Copy script ──────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [[ -f "${SCRIPT_DIR}/mariadb_dynatrace_generator.py" ]]; then
    cp "${SCRIPT_DIR}/mariadb_dynatrace_generator.py" "${INSTALL_DIR}/"
    log "Copied mariadb_dynatrace_generator.py"
else
    error "mariadb_dynatrace_generator.py not found in ${SCRIPT_DIR}"
fi

# ─── Install Python dependencies ──────────────────────────────────────────────
log "Setting up Python virtual environment..."
apt-get update -qq
apt-get install -y python3 python3-venv python3-pip -qq

python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/pip" install --quiet --upgrade pip
"${VENV_DIR}/bin/pip" install --quiet mysql-connector-python faker
log "Python dependencies installed"

# ─── Create environment file ──────────────────────────────────────────────────
ENV_FILE="${INSTALL_DIR}/mariadb-demo.env"
log "Creating environment file: ${ENV_FILE}"
cat > "${ENV_FILE}" << EOF
# MariaDB connection settings
MARIADB_HOST=${DB_HOST}
MARIADB_PORT=${DB_PORT}
MARIADB_USER=${DB_USER}
MARIADB_PASSWORD=${DB_PASSWORD}
MARIADB_DATABASE=${DB_NAME}
EOF
chmod 600 "${ENV_FILE}"

# ─── Create systemd service ───────────────────────────────────────────────────
SERVICE_FILE="/etc/systemd/system/mariadb-dynatrace-demo.service"
log "Creating systemd service: ${SERVICE_FILE}"
cat > "${SERVICE_FILE}" << EOF
[Unit]
Description=MariaDB Dynatrace Observability Demo
After=network-online.target
Wants=network-online.target

[Service]
Type=oneshot
User=${SERVICE_USER}
Group=${SERVICE_USER}
WorkingDirectory=${INSTALL_DIR}
EnvironmentFile=${ENV_FILE}

ExecStart=${VENV_DIR}/bin/python ${INSTALL_DIR}/mariadb_dynatrace_generator.py \\
    --host \${MARIADB_HOST} \\
    --port \${MARIADB_PORT} \\
    --user \${MARIADB_USER} \\
    --password \${MARIADB_PASSWORD} \\
    --database \${MARIADB_DATABASE} \\
    --scenario all \\
    --skip-seed \\
    --cleanup-before \\
    --cleanup-days 10

# Logging
StandardOutput=journal
StandardError=journal
SyslogIdentifier=mariadb-demo

# Hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadOnlyPaths=/
ReadWritePaths=${INSTALL_DIR}

[Install]
WantedBy=multi-user.target
EOF

# ─── Create systemd timer ─────────────────────────────────────────────────────
TIMER_FILE="/etc/systemd/system/mariadb-dynatrace-demo.timer"
log "Creating systemd timer: ${TIMER_FILE}"
cat > "${TIMER_FILE}" << EOF
[Unit]
Description=Run MariaDB Dynatrace Demo every 4 hours
Requires=mariadb-dynatrace-demo.service

[Timer]
# Run every 4 hours (00:00, 04:00, 08:00, 12:00, 16:00, 20:00)
OnCalendar=${TIMER_SCHEDULE}
# Randomize start within 5 minutes to avoid thundering herd
RandomizedDelaySec=300
# Run missed jobs if system was off
Persistent=true

[Install]
WantedBy=timers.target
EOF

# ─── Set permissions ──────────────────────────────────────────────────────────
chown -R "${SERVICE_USER}:${SERVICE_USER}" "${INSTALL_DIR}"
chmod 755 "${INSTALL_DIR}"
chmod 644 "${INSTALL_DIR}/mariadb_dynatrace_generator.py"

# ─── Enable and start ─────────────────────────────────────────────────────────
log "Enabling and starting timer..."
systemctl daemon-reload
systemctl enable mariadb-dynatrace-demo.timer
systemctl start mariadb-dynatrace-demo.timer

# ─── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "============================================================================="
log "Setup complete!"
echo "============================================================================="
echo ""
echo "Useful commands:"
echo "  Check timer status:    systemctl status mariadb-dynatrace-demo.timer"
echo "  Check service status:  systemctl status mariadb-dynatrace-demo.service"
echo "  View logs:             journalctl -u mariadb-dynatrace-demo.service -f"
echo "  Run manually:          systemctl start mariadb-dynatrace-demo.service"
echo "  Stop timer:            systemctl stop mariadb-dynatrace-demo.timer"
echo "  Next scheduled run:    systemctl list-timers mariadb-dynatrace-demo.timer"
echo ""
echo "Configuration:"
echo "  Install directory:     ${INSTALL_DIR}"
echo "  Environment file:      ${ENV_FILE} (edit to update DB credentials)"
echo "  Service file:          ${SERVICE_FILE}"
echo "  Timer file:            ${TIMER_FILE}"
echo ""
warn "Remember to update ${ENV_FILE} with your actual database credentials!"
echo ""

# Show timer status
systemctl list-timers mariadb-dynatrace-demo.timer --no-pager
