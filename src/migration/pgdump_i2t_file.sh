#!/bin/bash
# File-based migration - dump to file, then restore
# More reliable for large datasets

set -e

SOURCE_HOST="192.168.1.72"
SOURCE_PORT="5430"
DEST_HOST="192.168.1.40"
DEST_PORT="5432"
DB_USER="sd_admin"
DB_NAME="stock-dumps"
export PGPASSWORD="sdadmin@postgres"

LOG_DIR="/home/cgraaaj/Projects/cgr-trades/logs"
TMP_DIR="/tmp/migration"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/pgdump_file_${TIMESTAMP}.log"

# Created_on dates to migrate (missing dates only)
DATES=("2026-01-15" "2026-01-18")

mkdir -p "$TMP_DIR"

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

migrate_date() {
    local DATE=$1
    local DUMP_FILE="${TMP_DIR}/i2t_${DATE}.dump"
    
    log "============================================================"
    log "Processing created_on date: $DATE"
    log "============================================================"
    
    # Get source count
    local SRC_CNT=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${DATE} 00:00:00'::timestamp 
         AND created_on < '${DATE} 00:00:00'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    log "Source count: $SRC_CNT records"
    
    if [ "$SRC_CNT" == "0" ] || [ -z "$SRC_CNT" ]; then
        log "No data for this date, skipping"
        return 0
    fi
    
    # Step 1: Dump to file
    log "Step 1: Dumping to file..."
    local DUMP_START=$(date +%s)
    
    psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY (SELECT * FROM options.instrument_to_ticker 
                WHERE created_on >= '${DATE} 00:00:00'::timestamp 
                AND created_on < '${DATE} 00:00:00'::timestamp + interval '1 day') 
         TO '${DUMP_FILE}' WITH (FORMAT csv, HEADER false)" 2>/dev/null
    
    local DUMP_END=$(date +%s)
    local DUMP_TIME=$((DUMP_END - DUMP_START))
    local FILE_SIZE=$(ls -lh "$DUMP_FILE" 2>/dev/null | awk '{print $5}')
    log "Dump complete: $FILE_SIZE in ${DUMP_TIME}s"
    
    # Step 2: Clear destination for this date
    log "Step 2: Clearing destination..."
    psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
        "DELETE FROM options.instrument_to_ticker 
         WHERE created_on >= '${DATE} 00:00:00'::timestamp 
         AND created_on < '${DATE} 00:00:00'::timestamp + interval '1 day'" 2>/dev/null
    
    # Step 3: Load from file
    log "Step 3: Loading to destination..."
    local LOAD_START=$(date +%s)
    
    psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY options.instrument_to_ticker 
         FROM '${DUMP_FILE}' WITH (FORMAT csv, HEADER false)" 2>/dev/null
    
    local LOAD_END=$(date +%s)
    local LOAD_TIME=$((LOAD_END - LOAD_START))
    log "Load complete in ${LOAD_TIME}s"
    
    # Cleanup
    rm -f "$DUMP_FILE"
    
    log "✅ $DATE complete (dump: ${DUMP_TIME}s, load: ${LOAD_TIME}s)"
    log ""
}

log "============================================================"
log "FILE-BASED MIGRATION - instrument_to_ticker"
log "Started: $(date)"
log "Temp dir: $TMP_DIR"
log "Log: $LOG_FILE"
log "============================================================"
log ""

# Disable triggers
log "Disabling triggers on destination..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker DISABLE TRIGGER ALL;" 2>/dev/null
log "✅ Triggers disabled"
log ""

# Process each date
for DATE in "${DATES[@]}"; do
    migrate_date "$DATE"
done

# Re-enable triggers
log "============================================================"
log "Re-enabling triggers..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker ENABLE TRIGGER ALL;" 2>/dev/null
log "✅ Triggers enabled"

# Final verification
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

# Cleanup temp directory
rm -rf "$TMP_DIR"

log ""
log "============================================================"
log "MIGRATION COMPLETE: $(date)"
log "============================================================"






