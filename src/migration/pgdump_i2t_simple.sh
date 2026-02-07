#!/bin/bash
# Simple time-based chunked migration - NO COUNT QUERIES
# Migrates by hour to keep chunks small

set -e

SOURCE_HOST="192.168.1.72"
SOURCE_PORT="5430"
DEST_HOST="192.168.1.40"
DEST_PORT="5432"
DB_USER="sd_admin"
DB_NAME="stock-dumps"
export PGPASSWORD="sdadmin@postgres"

LOG_DIR="/home/cgraaaj/Projects/cgr-trades/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/pgdump_simple_${TIMESTAMP}.log"

# Trading hours: 9:15 to 15:30 IST
HOURS=("09" "10" "11" "12" "13" "14" "15")
DATES=("2026-01-08" "2026-01-09" "2026-01-10" "2026-01-13" "2026-01-14" "2026-01-15")

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

migrate_hour() {
    local DATE=$1
    local HOUR=$2
    
    local START_TS="${DATE} ${HOUR}:00:00"
    local END_TS="${DATE} ${HOUR}:59:59.999999"
    
    log "  Hour $HOUR: Copying..."
    
    # Stream COPY - no counts, just copy
    psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY (SELECT * FROM options.instrument_to_ticker 
                WHERE created_on >= '${START_TS}'::timestamp 
                AND created_on < '${END_TS}'::timestamp + interval '1 second') 
         TO STDOUT WITH (FORMAT binary)" 2>/dev/null | \
    psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY options.instrument_to_ticker FROM STDIN WITH (FORMAT binary)" 2>/dev/null
    
    log "  Hour $HOUR: ✅ Done"
}

log "============================================================"
log "SIMPLE HOURLY MIGRATION - instrument_to_ticker"
log "Started: $(date)"
log "Log: $LOG_FILE"
log "============================================================"

# Disable triggers
log "Disabling triggers..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker DISABLE TRIGGER ALL;" 2>/dev/null
log "✅ Triggers disabled"
log ""

# Process each date
for DATE in "${DATES[@]}"; do
    log "============================================================"
    log "Processing $DATE"
    log "============================================================"
    
    # Process each trading hour
    for HOUR in "${HOURS[@]}"; do
        migrate_hour "$DATE" "$HOUR"
    done
    
    log "✅ $DATE complete"
    log ""
done

# Re-enable triggers
log "============================================================"
log "Re-enabling triggers..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker ENABLE TRIGGER ALL;" 2>/dev/null
log "✅ Triggers enabled"

# Now do a single verification at the end
log ""
log "============================================================"
log "FINAL VERIFICATION"
log "============================================================"
for DATE in "${DATES[@]}"; do
    SRC=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${DATE} 00:00:00'::timestamp 
         AND created_on < '${DATE} 00:00:00'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    DST=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${DATE} 00:00:00'::timestamp 
         AND created_on < '${DATE} 00:00:00'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    if [ "$SRC" == "$DST" ]; then
        log "$DATE: ✅ $DST records (matches)"
    else
        log "$DATE: ⚠️ Src=$SRC, Dst=$DST"
    fi
done

log ""
log "============================================================"
log "MIGRATION COMPLETE: $(date)"
log "============================================================"






