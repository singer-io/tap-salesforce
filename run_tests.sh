#!/bin/bash

# Test runner script for tap-perplexity

set -e

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo "================================================"
echo "tap-perplexity Test Suite"
echo "================================================"
echo ""

# Parse command line arguments
RUN_UNIT=true
RUN_INTEGRATION=false
RUN_COVERAGE=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --integration)
            RUN_INTEGRATION=true
            shift
            ;;
        --coverage)
            RUN_COVERAGE=true
            shift
            ;;
        --unit-only)
            RUN_INTEGRATION=false
            shift
            ;;
        --all)
            RUN_UNIT=true
            RUN_INTEGRATION=true
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Usage: $0 [--integration] [--coverage] [--unit-only] [--all]"
            exit 1
            ;;
    esac
done

# Check if pytest is installed
if ! python -m pytest --version &> /dev/null; then
    echo -e "${RED}❌ pytest not found. Installing...${NC}"
    pip install pytest pytest-cov
fi

# Run unit tests
if [ "$RUN_UNIT" = true ]; then
    echo -e "${BLUE}Running unit tests...${NC}"
    if [ "$RUN_COVERAGE" = true ]; then
        python -m pytest tests/unit -v --cov=tap_perplexity --cov-report=term --cov-report=html
        echo ""
        echo -e "${GREEN}✓${NC} Unit tests completed with coverage report"
        echo "HTML coverage report: htmlcov/index.html"
    else
        python -m pytest tests/unit -v
        echo ""
        echo -e "${GREEN}✓${NC} Unit tests completed"
    fi
    echo ""
fi

# Run integration tests
if [ "$RUN_INTEGRATION" = true ]; then
    if [ ! -f "config.json" ]; then
        echo -e "${RED}❌ config.json not found${NC}"
        echo "Integration tests require a valid config.json file with API credentials"
        echo "Create one using: cp config.json.example config.json"
        exit 1
    fi
    
    echo -e "${BLUE}Running integration tests...${NC}"
    python -m pytest tests/integration -v
    echo ""
    echo -e "${GREEN}✓${NC} Integration tests completed"
    echo ""
fi

echo "================================================"
echo "All tests completed successfully!"
echo "================================================"
