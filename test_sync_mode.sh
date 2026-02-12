#!/bin/bash

# Test Sync Mode Demonstration Script
# This script shows how to test sync mode, but requires YOUR API key

set -e

echo "================================================"
echo "tap-perplexity Sync Mode Test"
echo "================================================"
echo ""

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if config.json exists
if [ ! -f "config.json" ]; then
    echo -e "${RED}❌ ERROR: config.json not found${NC}"
    echo ""
    echo "You need to create config.json with YOUR Perplexity API key."
    echo ""
    echo "Steps:"
    echo "1. Get an API key from https://www.perplexity.ai/"
    echo "2. Create config.json:"
    echo ""
    echo "   cat > config.json << 'CONFIGEOF'"
    echo '   {'
    echo '     "api_key": "pplx-YOUR-ACTUAL-KEY-HERE",'
    echo '     "start_date": "2024-01-01T00:00:00Z",'
    echo '     "user_agent": "tap-perplexity/1.0.0"'
    echo '   }'
    echo "   CONFIGEOF"
    echo ""
    echo "3. Run this script again"
    echo ""
    exit 1
fi

# Check if API key looks real
API_KEY=$(cat config.json | grep -o '"api_key": "[^"]*"' | cut -d'"' -f4)

if [[ "$API_KEY" == "pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" ]]; then
    echo -e "${RED}❌ ERROR: You're using the example API key${NC}"
    echo ""
    echo "Please replace the example API key with YOUR real key:"
    echo "1. Get your key from https://www.perplexity.ai/"
    echo "2. Edit config.json and replace the api_key value"
    echo ""
    exit 1
fi

if [[ ! "$API_KEY" =~ ^pplx- ]]; then
    echo -e "${YELLOW}⚠️  WARNING: API key doesn't start with 'pplx-'${NC}"
    echo "Are you sure this is a valid Perplexity API key?"
    echo ""
    read -p "Continue anyway? [y/N] " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

echo -e "${GREEN}✓${NC} config.json found with API key"
echo ""

# Step 1: Run Discovery
echo "Step 1: Running Discovery Mode"
echo "================================"
echo ""

if tap-perplexity --config config.json --discover > catalog.json 2>&1; then
    echo -e "${GREEN}✓${NC} Discovery completed successfully"
    echo ""
    
    # Show available streams
    echo "Available streams:"
    cat catalog.json | jq -r '.streams[].stream' | while read stream; do
        echo "  - $stream"
    done
    echo ""
else
    echo -e "${RED}❌ Discovery failed${NC}"
    echo "This might mean:"
    echo "  - Your API key is invalid"
    echo "  - Network connectivity issues"
    echo "  - Perplexity API is down"
    exit 1
fi

# Step 2: Select streams in catalog
echo "Step 2: Selecting Streams"
echo "========================="
echo ""

# Select the models stream
cat catalog.json | jq '.streams[0].metadata[0].metadata.selected = true' > catalog-selected.json
mv catalog-selected.json catalog.json

echo -e "${GREEN}✓${NC} Selected 'models' stream"
echo ""

# Step 3: Run Sync Mode
echo "Step 3: Running Sync Mode"
echo "========================="
echo ""

OUTPUT_FILE="sync-output-$(date +%Y%m%d-%H%M%S).json"

echo "Running sync and saving to $OUTPUT_FILE..."
echo ""

if tap-perplexity --config config.json --catalog catalog.json > "$OUTPUT_FILE" 2>&1; then
    echo -e "${GREEN}✓${NC} Sync completed successfully"
    echo ""
    
    # Analyze output
    echo "Sync Results:"
    echo "-------------"
    
    SCHEMA_COUNT=$(cat "$OUTPUT_FILE" | grep '"type": "SCHEMA"' | wc -l)
    RECORD_COUNT=$(cat "$OUTPUT_FILE" | grep '"type": "RECORD"' | wc -l)
    STATE_COUNT=$(cat "$OUTPUT_FILE" | grep '"type": "STATE"' | wc -l)
    
    echo "  SCHEMA messages: $SCHEMA_COUNT"
    echo "  RECORD messages: $RECORD_COUNT"
    echo "  STATE messages: $STATE_COUNT"
    echo ""
    
    if [ $RECORD_COUNT -gt 0 ]; then
        echo "Sample records:"
        cat "$OUTPUT_FILE" | grep '"type": "RECORD"' | head -3 | jq -c '.record' || true
        echo ""
    fi
    
    echo "Full output saved to: $OUTPUT_FILE"
    echo ""
    
else
    echo -e "${RED}❌ Sync failed${NC}"
    echo ""
    echo "Check the output file for errors: $OUTPUT_FILE"
    exit 1
fi

# Summary
echo "================================================"
echo "Sync Mode Test Complete!"
echo "================================================"
echo ""
echo "What was tested:"
echo "  ✓ Discovery mode with real API"
echo "  ✓ Catalog generation"
echo "  ✓ Stream selection"
echo "  ✓ Sync mode execution"
echo "  ✓ Singer message format"
echo ""
echo "Output file: $OUTPUT_FILE"
echo ""
echo "Next steps:"
echo "  - Review the output file"
echo "  - Try piping to a Singer target (e.g., target-csv)"
echo "  - Test with state file for incremental syncs"
echo ""
