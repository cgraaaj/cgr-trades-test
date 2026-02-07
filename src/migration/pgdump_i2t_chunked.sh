#!/bin/bash
# Chunked migration script for options.instrument_to_ticker
# Migrates in 3M record batches using time-based chunks

set -e

# Configuration
SOURCE_HOST="192.168.1.72"
SOURCE_PORT="5430"
DEST_HOST="192.168.1.40"
DEST_PORT="5432"
DB_USER="sd_admin"
DB_NAME="stock-dumps"
export PGPASSWORD="sdadmin@postgres"

CHUNK_SIZE=3000000  # 3 million records per chunk
LOG_DIR="/home/cgraaaj/Projects/cgr-trades/logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/pgdump_i2t_chunked_${TIMESTAMP}.log"

# Dates to migrate
DATES=("2026-01-08" "2026-01-09" "2026-01-10" "2026-01-13" "2026-01-14" "2026-01-15")

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

migrate_chunk() {
    local DATE=$1
    local START_TS=$2
    local END_TS=$3
    local CHUNK_NUM=$4
    local EXPECTED=$5
    
    log "  Chunk $CHUNK_NUM: $START_TS to $END_TS (expecting ~$EXPECTED records)"
    
    # Stream COPY from source to destination
    psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY (SELECT * FROM options.instrument_to_ticker 
                WHERE created_on >= '${START_TS}'::timestamp 
                AND created_on < '${END_TS}'::timestamp) 
         TO STDOUT WITH (FORMAT binary)" 2>/dev/null | \
    psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY options.instrument_to_ticker FROM STDIN WITH (FORMAT binary)" 2>/dev/null
    
    # Verify chunk
    local INSERTED=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${START_TS}'::timestamp 
         AND created_on < '${END_TS}'::timestamp" 2>/dev/null | tr -d ' ')
    
    log "  ✅ Chunk $CHUNK_NUM done: $INSERTED records inserted"
    echo "$INSERTED"
}

migrate_date() {
    local DATE=$1
    local START_TS="${DATE} 00:00:00"
    local END_TS="${DATE} 23:59:59.999999"
    
    log "============================================================"
    log "Processing $DATE"
    log "============================================================"
    
    # Check if already migrated
    local DEST_CNT=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${START_TS}'::timestamp 
         AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    # Get source count
    local SOURCE_CNT=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${START_TS}'::timestamp 
         AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    log "Source: $SOURCE_CNT | Destination: $DEST_CNT"
    
    if [ "$DEST_CNT" == "$SOURCE_CNT" ] && [ "$SOURCE_CNT" != "0" ]; then
        log "✅ Already migrated, skipping"
        return 0
    fi
    
    # Delete partial data if exists
    if [ "$DEST_CNT" != "0" ] && [ -n "$DEST_CNT" ]; then
        log "Deleting $DEST_CNT partial records..."
        psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
            "DELETE FROM options.instrument_to_ticker 
             WHERE created_on >= '${START_TS}'::timestamp 
             AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null
        log "Deleted"
    fi
    
    # Get time boundaries for this date
    local TIME_BOUNDS=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT MIN(created_on)::text || '|' || MAX(created_on)::text 
         FROM options.instrument_to_ticker 
         WHERE created_on >= '${START_TS}'::timestamp 
         AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    local MIN_TIME=$(echo "$TIME_BOUNDS" | cut -d'|' -f1)
    local MAX_TIME=$(echo "$TIME_BOUNDS" | cut -d'|' -f2)
    
    log "Time range: $MIN_TIME to $MAX_TIME"
    
    # Calculate number of chunks needed
    local NUM_CHUNKS=$(( (SOURCE_CNT + CHUNK_SIZE - 1) / CHUNK_SIZE ))
    log "Will process in $NUM_CHUNKS chunks of ~$CHUNK_SIZE records each"
    
    # Calculate time interval per chunk (in seconds)
    local MIN_EPOCH=$(date -d "$MIN_TIME" +%s 2>/dev/null || echo "0")
    local MAX_EPOCH=$(date -d "$MAX_TIME" +%s 2>/dev/null || echo "0")
    local TOTAL_SECONDS=$((MAX_EPOCH - MIN_EPOCH + 1))
    local SECONDS_PER_CHUNK=$((TOTAL_SECONDS / NUM_CHUNKS))
    
    # Ensure minimum 60 seconds per chunk
    if [ "$SECONDS_PER_CHUNK" -lt 60 ]; then
        SECONDS_PER_CHUNK=60
    fi
    
    log "Time interval per chunk: ~$SECONDS_PER_CHUNK seconds"
    
    local CHUNK_START="$MIN_TIME"
    local CHUNK_NUM=1
    local TOTAL_INSERTED=0
    
    while [ "$CHUNK_NUM" -le "$NUM_CHUNKS" ]; do
        # Calculate chunk end time
        local CHUNK_START_EPOCH=$(date -d "$CHUNK_START" +%s 2>/dev/null)
        local CHUNK_END_EPOCH=$((CHUNK_START_EPOCH + SECONDS_PER_CHUNK))
        
        # For last chunk, use max time + 1 second
        if [ "$CHUNK_NUM" -eq "$NUM_CHUNKS" ]; then
            CHUNK_END_EPOCH=$((MAX_EPOCH + 1))
        fi
        
        local CHUNK_END=$(date -d "@$CHUNK_END_EPOCH" '+%Y-%m-%d %H:%M:%S' 2>/dev/null)
        
        # Get expected count for this chunk
        local EXPECTED=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
            "SELECT COUNT(*) FROM options.instrument_to_ticker 
             WHERE created_on >= '${CHUNK_START}'::timestamp 
             AND created_on < '${CHUNK_END}'::timestamp" 2>/dev/null | tr -d ' ')
        
        if [ "$EXPECTED" != "0" ] && [ -n "$EXPECTED" ]; then
            local INSERTED=$(migrate_chunk "$DATE" "$CHUNK_START" "$CHUNK_END" "$CHUNK_NUM" "$EXPECTED")
            TOTAL_INSERTED=$((TOTAL_INSERTED + INSERTED))
        fi
        
        CHUNK_START="$CHUNK_END"
        CHUNK_NUM=$((CHUNK_NUM + 1))
    done
    
    # Final verification
    local FINAL_CNT=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${START_TS}'::timestamp 
         AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    if [ "$FINAL_CNT" == "$SOURCE_CNT" ]; then
        log "✅ $DATE COMPLETE: $FINAL_CNT records (matches source)"
    else
        log "⚠️ $DATE MISMATCH: Dest=$FINAL_CNT, Source=$SOURCE_CNT"
    fi
    
    log ""
}

# Main execution
log "============================================================"
log "CHUNKED MIGRATION - instrument_to_ticker"
log "Chunk size: $CHUNK_SIZE records"
log "Started: $(date)"
log "Log: $LOG_FILE"
log "============================================================"

# Disable triggers
log "Disabling triggers..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker DISABLE TRIGGER ALL;" 2>/dev/null
log "✅ Triggers disabled"

# Process each date
for DATE in "${DATES[@]}"; do
    migrate_date "$DATE"
done

# Re-enable triggers and reindex
log "============================================================"
log "Re-enabling triggers..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker ENABLE TRIGGER ALL;" 2>/dev/null
log "✅ Triggers enabled"

log "Reindexing table..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "REINDEX TABLE options.instrument_to_ticker;" 2>/dev/null
log "✅ Reindex complete"

log ""
log "============================================================"
log "MIGRATION COMPLETE"
log "Finished: $(date)"
log "============================================================"

# Final summary
log ""
log "=== FINAL COUNTS ==="
for DATE in "${DATES[@]}"; do
    CNT=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${DATE} 00:00:00'::timestamp 
         AND created_on < '${DATE} 00:00:00'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    log "$DATE: $CNT records"
done

