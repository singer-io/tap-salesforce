#!/bin/bash

# Quickstart script for tap-perplexity
# This script helps you quickly set up and test the tap

set -e  # Exit on error

echo "================================================"
echo "tap-perplexity Quickstart"
echo "================================================"
echo ""

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check if Python is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.7 or higher."
    exit 1
fi

echo -e "${GREEN}✓${NC} Python 3 found: $(python3 --version)"

# Check if pip is installed
if ! command -v pip3 &> /dev/null && ! command -v pip &> /dev/null; then
    echo "❌ pip is not installed. Please install pip."
    exit 1
fi

echo -e "${GREEN}✓${NC} pip found"

# Create virtual environment
if [ ! -d "venv" ]; then
    echo -e "${BLUE}Creating virtual environment...${NC}"
    python3 -m venv venv
    echo -e "${GREEN}✓${NC} Virtual environment created"
else
    echo -e "${YELLOW}Virtual environment already exists${NC}"
fi

# Activate virtual environment
echo -e "${BLUE}Activating virtual environment...${NC}"
source venv/bin/activate

# Install the tap
echo -e "${BLUE}Installing tap-perplexity...${NC}"
pip install -e . > /dev/null 2>&1
echo -e "${GREEN}✓${NC} tap-perplexity installed"

# Install development dependencies (optional)
read -p "Install development dependencies (pytest, pylint, etc.)? [y/N] " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo -e "${BLUE}Installing development dependencies...${NC}"
    pip install -e '.[dev]' > /dev/null 2>&1
    echo -e "${GREEN}✓${NC} Development dependencies installed"
fi

# Check if config.json exists
if [ ! -f "config.json" ]; then
    echo ""
    echo -e "${YELLOW}⚠ config.json not found${NC}"
    echo ""
    read -p "Do you want to create a config.json file now? [y/N] " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo "Please enter your Perplexity API key (starts with pplx-):"
        read -r API_KEY
        
        cat > config.json << CONFIGEOF
{
  "api_key": "$API_KEY",
  "start_date": "2024-01-01T00:00:00Z",
  "user_agent": "tap-perplexity/1.0.0"
}
CONFIGEOF
        echo -e "${GREEN}✓${NC} config.json created"
    else
        echo ""
        echo "You can create config.json later using:"
        echo "  cp config.json.example config.json"
        echo "  # Edit config.json with your API key"
        echo ""
    fi
fi

echo ""
echo "================================================"
echo "Installation Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo ""
echo "1. Run discovery to see available streams:"
echo "   ${BLUE}tap-perplexity --config config.json --discover > catalog.json${NC}"
echo ""
echo "2. Run unit tests:"
echo "   ${BLUE}python -m pytest tests/unit -v${NC}"
echo ""
echo "3. Sync data (if you have a valid API key):"
echo "   ${BLUE}tap-perplexity --config config.json --catalog catalog.json${NC}"
echo ""
echo "For more information, see:"
echo "  - README.md"
echo "  - SETUP_GUIDE.md"
echo ""
echo "Virtual environment is activated. To deactivate, run: deactivate"
echo ""
