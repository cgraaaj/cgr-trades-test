#!/bin/bash
# Optimized pg_dump migration for instrument_to_ticker
# Uses timestamp ranges, binary COPY, single transaction per day

set -e

LOG_FILE="/home/cgraaaj/Projects/cgr-trades/logs/pgdump_i2t_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

SOURCE_HOST="192.168.1.72"
SOURCE_PORT="5430"
DEST_HOST="192.168.1.40"
DEST_PORT="5432"
DB_NAME="stock-dumps"
DB_USER="sd_admin"
export PGPASSWORD="sdadmin@postgres"

echo "============================================================"
echo "OPTIMIZED PG_DUMP MIGRATION - instrument_to_ticker"
echo "Started: $(date)"
echo "Log: $LOG_FILE"
echo "============================================================"

# Disable triggers on destination for faster inserts
echo ""
echo "Disabling triggers on destination..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker DISABLE TRIGGER ALL;" 2>/dev/null
echo "✅ Triggers disabled"

migrate_date() {
    local DATE=$1
    local EXPECTED=$2
    local START_TS="${DATE} 00:00:00"
    
    echo ""
    echo "============================================================"
    echo "Processing $DATE (expected: $EXPECTED records)"
    echo "Started: $(date)"
    echo "============================================================"
    
    # Check current dest count using timestamp range
    local DEST_CNT=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${START_TS}'::timestamp 
         AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    echo "Current destination count: $DEST_CNT"
    
    if [ "$DEST_CNT" == "$EXPECTED" ]; then
        echo "✅ Already complete - skipping"
        return
    fi
    
    # Delete partial data if exists using timestamp range
    if [ "$DEST_CNT" != "0" ] && [ -n "$DEST_CNT" ]; then
        echo "⚠️ Partial data found ($DEST_CNT records) - deleting..."
        psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
            "DELETE FROM options.instrument_to_ticker 
             WHERE created_on >= '${START_TS}'::timestamp 
             AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null
        echo "   Deleted."
    fi
    
    echo "Migrating $EXPECTED records via binary COPY..."
    local START_TIME=$(date +%s)
    
    # Single binary COPY - streams directly from source to destination
    psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY (SELECT * FROM options.instrument_to_ticker 
                WHERE created_on >= '${START_TS}'::timestamp 
                AND created_on < '${START_TS}'::timestamp + interval '1 day') 
         TO STDOUT WITH (FORMAT binary)" 2>/dev/null | \
    psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
        "\COPY options.instrument_to_ticker FROM STDIN WITH (FORMAT binary)" 2>/dev/null
    
    local END_TIME=$(date +%s)
    local DURATION=$((END_TIME - START_TIME))
    local RATE=$((EXPECTED / (DURATION + 1)))
    
    # Verify using timestamp range
    local NEW_CNT=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
        "SELECT COUNT(*) FROM options.instrument_to_ticker 
         WHERE created_on >= '${START_TS}'::timestamp 
         AND created_on < '${START_TS}'::timestamp + interval '1 day'" 2>/dev/null | tr -d ' ')
    
    echo ""
    echo "Verification: $NEW_CNT / $EXPECTED"
    echo "Duration: ${DURATION}s (~${RATE} records/sec)"
    if [ "$NEW_CNT" == "$EXPECTED" ]; then
        echo "✅ $DATE COMPLETE!"
    else
        echo "⚠️ Mismatch - may need retry"
    fi
}

# Run migrations in order
echo ""
echo "Starting migration of 6 dates (~37M records total)..."

migrate_date "2026-01-08" "7790480"
migrate_date "2026-01-10" "12492244"
migrate_date "2026-01-13" "4451074"
migrate_date "2026-01-14" "4285053"
migrate_date "2026-01-15" "4516417"
migrate_date "2026-01-18" "4899879"

# Re-enable triggers and reindex
echo ""
echo "============================================================"
echo "Re-enabling triggers and reindexing..."
echo "============================================================"
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "ALTER TABLE options.instrument_to_ticker ENABLE TRIGGER ALL;" 2>/dev/null
echo "✅ Triggers enabled"

echo "Reindexing table (this may take a moment)..."
psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -c \
    "REINDEX TABLE options.instrument_to_ticker;" 2>/dev/null
echo "✅ Reindex complete"

# Final verification
echo ""
echo "============================================================"
echo "FINAL VERIFICATION"
echo "============================================================"

TOTAL_SRC=$(psql -h $SOURCE_HOST -p $SOURCE_PORT -U $DB_USER -d "$DB_NAME" -t -c \
    "SELECT COUNT(*) FROM options.instrument_to_ticker 
     WHERE created_on >= '2026-01-02 00:00:00'::timestamp" 2>/dev/null | tr -d ' ')

TOTAL_DEST=$(psql -h $DEST_HOST -p $DEST_PORT -U $DB_USER -d "$DB_NAME" -t -c \
    "SELECT COUNT(*) FROM options.instrument_to_ticker 
     WHERE created_on >= '2026-01-02 00:00:00'::timestamp" 2>/dev/null | tr -d ' ')

echo "Source (>=2026-01-02): $TOTAL_SRC"
echo "Dest (>=2026-01-02): $TOTAL_DEST"

if [ "$TOTAL_SRC" == "$TOTAL_DEST" ]; then
    echo ""
    echo "🎉 MIGRATION COMPLETE - ALL RECORDS TRANSFERRED!"
else
    DIFF=$((TOTAL_SRC - TOTAL_DEST))
    echo "⚠️ Difference: $DIFF records"
fi

echo ""
echo "Completed: $(date)"
