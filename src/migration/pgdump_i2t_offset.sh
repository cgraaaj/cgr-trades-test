#!/bin/bash
# OFFSET/LIMIT based chunked migration for options.instrument_to_ticker
# Migrates in 3M record batches

set -e

SOURCE_HOST="192.168.1.72"
SOURCE_PORT="5430"
DEST_HOST="192.168.1.40"
DEST_PORT="5432"
DB_USER="sd_admin"
DB_NAME="stock-dumps"
export PGPASSWORD="sdadmin@postgres"

CHUNK_SIZE=3000000  # 3 million per batch
LOG_DIR="/home/cgraaaj/Projects/cgr-trades/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/pgdump_offset_${TIMESTAMP}.log"

# Dates to migrate (using created_on ranges based on batch insertion times)
# 2026-01-08: contains trade_date 2026-01-05, 2026-01-06, 2026-01-07, 2026-01-08
# Need to migrate records where created_on >= '2026-01-08' AND created_on < '2026-01-16'

START_DATE="2026-01-08"
END_DATE="2026-01-16"

log() {
    echo "[$(date '+%H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

log "============================================================"
log "OFFSET/LIMIT MIGRATION - instrument_to_ticker"
log "Chunk size: $CHUNK_SIZE records"
log "Date range: $START_DATE to $END_DATE"
log "Started: $(date)"
log "Log: $LOG_FILE"
log "============================================================"

# Get total count from source
log "Getting total count from source..."
TOTAL=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
    "SELECT COUNT(*) FROM options.instrument_to_ticker 
     WHERE created_on >= '${START_DATE} 00:00:00'::timestamp 
     AND created_on < '${END_DATE} 00:00:00'::timestamp" 2>/dev/null | tr -d ' ')

log "Total records to migrate: $TOTAL"

# Calculate number of chunks
NUM_CHUNKS=$(( (TOTAL + CHUNK_SIZE - 1) / CHUNK_SIZE ))
log "Will process in $NUM_CHUNKS chunks"
log ""

# Disable triggers
log "Disabling triggers on destination..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker DISABLE TRIGGER ALL;" 2>/dev/null
log "✅ Triggers disabled"
log ""

# Clear any existing data in the range (clean slate)
log "Clearing destination data in date range..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "DELETE FROM options.instrument_to_ticker 
     WHERE created_on >= '${START_DATE} 00:00:00'::timestamp 
     AND created_on < '${END_DATE} 00:00:00'::timestamp" 2>/dev/null
log "✅ Cleared"
log ""

# Process each chunk
OFFSET=0
CHUNK_NUM=1

while [ $OFFSET -lt $TOTAL ]; do
    log "============================================================"
    log "Chunk $CHUNK_NUM/$NUM_CHUNKS (offset: $OFFSET, limit: $CHUNK_SIZE)"
    log "============================================================"
    
    START_TIME=$(date +%s)
    
    # Stream COPY with OFFSET/LIMIT
    # Order by id ensures consistent results across chunks
    psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY (SELECT * FROM options.instrument_to_ticker 
                WHERE created_on >= '${START_DATE} 00:00:00'::timestamp 
                AND created_on < '${END_DATE} 00:00:00'::timestamp
                ORDER BY id
                LIMIT $CHUNK_SIZE OFFSET $OFFSET) 
         TO STDOUT WITH (FORMAT binary)" 2>/dev/null | \
    psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY options.instrument_to_ticker FROM STDIN WITH (FORMAT binary)" 2>/dev/null
    
    END_TIME=$(date +%s)
    ELAPSED=$((END_TIME - START_TIME))
    
    log "✅ Chunk $CHUNK_NUM done in ${ELAPSED}s"
    
    OFFSET=$((OFFSET + CHUNK_SIZE))
    CHUNK_NUM=$((CHUNK_NUM + 1))
    
    # Progress estimate
    REMAINING_CHUNKS=$((NUM_CHUNKS - CHUNK_NUM + 1))
    ETA_SECONDS=$((REMAINING_CHUNKS * ELAPSED))
    ETA_MINS=$((ETA_SECONDS / 60))
    log "ETA: ~${ETA_MINS} minutes remaining"
    log ""
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

SRC_TOTAL=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
    "SELECT COUNT(*) FROM options.instrument_to_ticker 
     WHERE created_on >= '${START_DATE} 00:00:00'::timestamp 
     AND created_on < '${END_DATE} 00:00:00'::timestamp" 2>/dev/null | tr -d ' ')

DST_TOTAL=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
    "SELECT COUNT(*) FROM options.instrument_to_ticker 
     WHERE created_on >= '${START_DATE} 00:00:00'::timestamp 
     AND created_on < '${END_DATE} 00:00:00'::timestamp" 2>/dev/null | tr -d ' ')

log "Source: $SRC_TOTAL"
log "Dest:   $DST_TOTAL"

if [ "$SRC_TOTAL" == "$DST_TOTAL" ]; then
    log "✅ MIGRATION SUCCESSFUL - counts match!"
else
    log "⚠️ COUNT MISMATCH - please verify"
fi

log ""
log "============================================================"
log "MIGRATION COMPLETE: $(date)"
log "============================================================"






