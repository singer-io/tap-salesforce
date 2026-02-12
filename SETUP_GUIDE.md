# Setup Guide for tap-perplexity

This guide will help you set up and test the Perplexity AI tap.

## Prerequisites

- Python 3.7 or higher
- pip package manager
- A Perplexity AI account with API access

## Getting a Perplexity API Key

1. Visit https://www.perplexity.ai/
2. Sign up for an account or log in
3. Navigate to Settings > API
4. Generate an API key
5. Copy the API key (starts with `pplx-`)

**Note:** Perplexity AI offers different pricing tiers. Check their pricing page for details on free tier limits and paid options.

## Installation

### Option 1: Install from Source (Recommended for Development)

```bash
# Clone the repository
git clone https://github.com/singer-io/tap-perplexity.git
cd tap-perplexity

# Create a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install the tap in development mode
pip install -e .

# Install development dependencies
pip install -e '.[dev]'
```

### Option 2: Install from PyPI

```bash
pip install tap-perplexity
```

## Configuration

Create a `config.json` file with your API credentials:

```json
{
  "api_key": "pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "start_date": "2024-01-01T00:00:00Z",
  "user_agent": "tap-perplexity/1.0.0"
}
```

### Configuration Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `api_key` | Yes | Your Perplexity AI API key |
| `start_date` | Yes | RFC3339 formatted date-time for filtering data |
| `user_agent` | No | User agent string for API requests (default: "tap-perplexity") |

## Running the Tap

### 1. Discovery Mode

Discovery mode outputs a catalog of available streams and their schemas:

```bash
tap-perplexity --config config.json --discover > catalog.json
```

This creates a `catalog.json` file containing all available streams.

### 2. Select Streams

Edit `catalog.json` to select which streams to sync. Set `"selected": true` in each stream's metadata:

```json
{
  "streams": [
    {
      "stream": "models",
      "tap_stream_id": "models",
      "metadata": [
        {
          "breadcrumb": [],
          "metadata": {
            "selected": true
          }
        }
      ]
    }
  ]
}
```

### 3. Sync Data

Run the tap to extract data:

```bash
tap-perplexity --config config.json --catalog catalog.json
```

With state tracking:

```bash
tap-perplexity --config config.json --catalog catalog.json --state state.json
```

## Testing

### Running Unit Tests

```bash
# Run all unit tests
python -m pytest tests/unit -v

# Run with coverage
python -m pytest tests/unit --cov=tap_perplexity --cov-report=html

# View coverage report
open htmlcov/index.html  # On macOS
xdg-open htmlcov/index.html  # On Linux
```

### Running Integration Tests

Integration tests require a valid API key in `config.json`:

```bash
# Create config.json with your API key
cp config.json.example config.json
# Edit config.json with your actual API key

# Run integration tests
python -m pytest tests/integration -v
```

**Note:** Integration tests will make real API calls to Perplexity AI and may count against your rate limits.

### Running All Tests

```bash
python -m pytest tests/ -v
```

## Verification Steps

1. **Verify Installation**
   ```bash
   tap-perplexity --help
   ```

2. **Verify Discovery**
   ```bash
   tap-perplexity --config config.json --discover | jq '.streams[].stream'
   ```
   Should output:
   ```
   "models"
   "chat_completions"
   ```

3. **Verify Models Stream**
   ```bash
   # Select only models stream
   tap-perplexity --config config.json --discover | \
     jq '.streams[0].metadata[0].metadata.selected = true' | \
     tap-perplexity --config config.json --catalog /dev/stdin
   ```

## Using with Singer Targets

The tap follows the Singer specification and can pipe data to any Singer target:

### Example: CSV Output

```bash
pip install target-csv
tap-perplexity --config config.json --catalog catalog.json | target-csv
```

### Example: JSON Lines Output

```bash
pip install target-jsonl
tap-perplexity --config config.json --catalog catalog.json | target-jsonl
```

### Example: PostgreSQL

```bash
pip install target-postgres
tap-perplexity --config config.json --catalog catalog.json | \
  target-postgres --config target-config.json
```

## Troubleshooting

### Issue: "Invalid API key" error

**Solution:** 
- Verify your API key is correct in `config.json`
- Ensure the key starts with `pplx-`
- Check that your key hasn't expired

### Issue: Rate limit errors

**Solution:**
- The tap automatically retries with exponential backoff
- Reduce sync frequency
- Contact Perplexity AI to increase rate limits

### Issue: Import errors

**Solution:**
```bash
# Reinstall the tap
pip uninstall tap-perplexity
pip install -e .
```

### Issue: Schema validation errors

**Solution:**
- Run discovery again to get latest schemas
- Verify your catalog.json is properly formatted JSON

## API Endpoints Used

The tap uses the following Perplexity AI API endpoints:

1. `GET /models` - Lists available models
2. *(Future)* Chat completions endpoint would be added when Perplexity provides a history/list endpoint

## Rate Limits

Perplexity AI has rate limits on API requests. The tap includes:

- Automatic retry with exponential backoff
- Rate limit detection (429 status codes)
- Configurable delays between requests

Check Perplexity AI's documentation for current rate limits on your account tier.

## Support

For issues and questions:

1. Check the [GitHub Issues](https://github.com/singer-io/tap-perplexity/issues)
2. Review the [Singer.io documentation](https://www.singer.io/)
3. Contact Perplexity AI support for API-specific questions

## Next Steps

- Set up a Singer target for your data destination
- Configure automated syncs using a scheduler (cron, Airflow, etc.)
- Monitor sync performance and adjust configuration as needed
- Explore additional Perplexity AI features as they become available
