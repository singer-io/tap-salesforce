# tap-perplexity

[![PyPI version](https://badge.fury.io/py/tap-perplexity.svg)](https://badge.fury.io/py/tap-perplexity)

[Singer](https://www.singer.io/) tap that extracts data from the [Perplexity AI API](https://docs.perplexity.ai/) and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification).

## 🚀 Two Ways to Get Started

### Option 1: Mock Mode (Instant - No Registration!) ⚡

Test the tap **without creating an account or API key**:

```bash
# Install
pip install -e .

# Run with mock mode (works immediately!)
tap-perplexity --config config-mock.json --discover
tap-perplexity --config config-mock.json --catalog catalog.json
```

✅ No API key needed  
✅ Works in 30 seconds  
✅ Sample data included  

See [MOCK_MODE_GUIDE.md](MOCK_MODE_GUIDE.md) for details.

### Option 2: Real API (5 minutes - Real Data) 🔑

Get your own API key from https://www.perplexity.ai/ (free tier available):

```bash
# Create config with YOUR key
cat > config.json << 'EOF'
{
  "api_key": "pplx-YOUR-KEY-HERE",
  "start_date": "2024-01-01T00:00:00Z"
}
EOF

# Run with real API
tap-perplexity --config config.json --discover
```

See [QUICK_START.md](QUICK_START.md) for full guide.

## Features

This tap extracts the following streams from Perplexity AI:

- **models**: List of available AI models
- **chat_completions**: Chat completion requests and responses (read from log/history if available)

## Quickstart

### Install the tap

```bash
pip install tap-perplexity
```

Or install from source:

```bash
git clone https://github.com/singer-io/tap-perplexity.git
cd tap-perplexity
pip install -e .
```

### Create a Config file

You'll need a Perplexity AI API key. Get one by:

1. Visit https://www.perplexity.ai/
2. Sign up for an account
3. Navigate to API settings
4. Generate an API key

Create a `config.json` file:

```json
{
  "api_key": "pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
  "start_date": "2024-01-01T00:00:00Z",
  "user_agent": "tap-perplexity/1.0.0"
}
```

**Configuration Parameters:**

- `api_key` (required): Your Perplexity AI API key
- `start_date` (required): RFC3339 formatted date-time for filtering data
- `user_agent` (optional): User agent string for API requests (default: "tap-perplexity")

### Run Discovery

Discovery mode will output a catalog of available streams and their schemas:

```bash
tap-perplexity --config config.json --discover > catalog.json
```

### Select Streams

Edit the `catalog.json` to select which streams to sync. Set `"selected": true` in the stream's metadata:

```json
{
  "streams": [
    {
      "stream": "models",
      "tap_stream_id": "models",
      "schema": {...},
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

### Sync Data

Run the tap to extract data:

```bash
tap-perplexity --config config.json --catalog catalog.json
```

Or with state tracking:

```bash
tap-perplexity --config config.json --catalog catalog.json --state state.json > output.json
```

### Use with a Singer Target

Pipe the output to a Singer target:

```bash
tap-perplexity --config config.json --catalog catalog.json | target-csv
```

## Streams

### models

Lists all available Perplexity AI models with their details.

**Primary Key:** `id`

**Replication Method:** FULL_TABLE

**Schema:**
- `id` (string): Model identifier
- `object` (string): Object type
- `created` (integer): Creation timestamp
- `owned_by` (string): Organization that owns the model

## Development

### Install Development Dependencies

```bash
pip install -e '.[dev]'
```

### Run Tests

```bash
# Unit tests
python -m pytest tests/unit -v

# Integration tests (requires valid API key in config.json)
python -m pytest tests/integration -v

# All tests
python -m pytest tests/ -v
```

## License

Copyright &copy; 2024 Singer.io

Licensed under the Apache License, Version 2.0
