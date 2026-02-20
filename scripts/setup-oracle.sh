#!/usr/bin/env bash
# =============================================================================
# setup-oracle.sh — Start Oracle in Docker and create the rate_limiter schema
# =============================================================================
#
# Usage:
#   ./scripts/setup-oracle.sh
#
# What it does:
#   1. Starts Oracle 19c via docker-compose
#   2. Waits for the database to be ready (up to 5 minutes)
#   3. Creates the rate_limiter user in the ORCLPDB1 pluggable database
#   4. Grants necessary permissions
#
# Prerequisites:
#   - Docker and Docker Compose installed
#   - Port 1521 available on localhost
# =============================================================================

set -euo pipefail

# Ensure common install paths are on PATH (Docker Desktop on macOS)
export PATH="/Applications/Docker.app/Contents/Resources/bin:/usr/local/bin:/opt/homebrew/bin:$HOME/.docker/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

CONTAINER_NAME="rate-limiter-oracle"
ORACLE_PWD="rate_limiter_pwd"
APP_USER="rate_limiter"
APP_PASSWORD="rate_limiter"
PDB_NAME="ORCLPDB1"
MAX_WAIT_SECONDS=300

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ---- Step 1: Start Oracle via docker-compose ----
log_info "Starting Oracle container..."
cd "$PROJECT_DIR"

if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log_warn "Container '${CONTAINER_NAME}' is already running."
else
    docker compose up -d oracle
    log_info "Container '${CONTAINER_NAME}' started."
fi

# ---- Step 2: Wait for Oracle to be ready ----
log_info "Waiting for Oracle to be ready (this can take 2-4 minutes on first run)..."

elapsed=0
interval=10
while [ $elapsed -lt $MAX_WAIT_SECONDS ]; do
    # Try connecting to the PDB as SYS
    if docker exec "$CONTAINER_NAME" bash -c \
        "echo 'SELECT 1 FROM DUAL;' | sqlplus -S sys/${ORACLE_PWD}@//localhost:1521/${PDB_NAME} as sysdba" \
        2>/dev/null | grep -q "1"; then
        log_info "Oracle PDB (${PDB_NAME}) is ready after ${elapsed}s."
        break
    fi

    sleep $interval
    elapsed=$((elapsed + interval))
    echo -n "."
done

if [ $elapsed -ge $MAX_WAIT_SECONDS ]; then
    log_error "Oracle did not become ready within ${MAX_WAIT_SECONDS}s. Check: docker logs ${CONTAINER_NAME}"
    exit 1
fi

# ---- Step 3: Create application user ----
log_info "Creating application user '${APP_USER}' in ${PDB_NAME}..."

docker exec "$CONTAINER_NAME" bash -c "sqlplus -S sys/${ORACLE_PWD}@//localhost:1521/${PDB_NAME} as sysdba" <<EOF
-- Drop user if exists (ignore errors on first run)
BEGIN
    EXECUTE IMMEDIATE 'DROP USER ${APP_USER} CASCADE';
EXCEPTION
    WHEN OTHERS THEN
        IF SQLCODE != -1918 THEN RAISE; END IF;
END;
/

-- Create the application user
CREATE USER ${APP_USER} IDENTIFIED BY ${APP_PASSWORD}
    DEFAULT TABLESPACE USERS
    TEMPORARY TABLESPACE TEMP
    QUOTA UNLIMITED ON USERS;

-- Grant permissions needed by the application
GRANT CONNECT TO ${APP_USER};
GRANT RESOURCE TO ${APP_USER};
GRANT CREATE TABLE TO ${APP_USER};
GRANT CREATE SEQUENCE TO ${APP_USER};
GRANT CREATE SESSION TO ${APP_USER};

EXIT;
EOF

log_info "User '${APP_USER}' created successfully."

# ---- Step 4: Verify connectivity ----
log_info "Verifying application user connectivity..."

docker exec "$CONTAINER_NAME" bash -c \
    "echo 'SELECT 1 FROM DUAL;' | sqlplus -S ${APP_USER}/${APP_PASSWORD}@//localhost:1521/${PDB_NAME}" \
    2>/dev/null | grep -q "1" && \
    log_info "Application user '${APP_USER}' can connect to ${PDB_NAME}." || \
    { log_error "Failed to connect as '${APP_USER}'. Check the logs."; exit 1; }

echo ""
log_info "Oracle setup complete!"
log_info ""
log_info "Connection details:"
log_info "  JDBC URL:  jdbc:oracle:thin:@localhost:1521/${PDB_NAME}"
log_info "  Username:  ${APP_USER}"
log_info "  Password:  ${APP_PASSWORD}"
log_info ""
log_info "Next steps:"
log_info "  1. Run the application: ./gradlew quarkusDev"
log_info "     (Flyway will create the tables automatically)"
log_info "  2. Seed the default config:"
log_info "     curl -X POST http://localhost:8080/admin/rate-limit/config \\"
log_info "       -H 'Content-Type: application/json' \\"
log_info "       -d '{\"configName\":\"default\",\"maxPerWindow\":100,\"windowSizeSecs\":4}'"
