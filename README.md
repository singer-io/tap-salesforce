# tap-perplexity

[![PyPI version](https://badge.fury.io/py/tap-perplexity.svg)](https://badge.fury.io/py/tap-perplexity)

[Singer](https://www.singer.io/) tap that extracts data from the [Perplexity AI API](https://docs.perplexity.ai/) and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md#singer-specification).

## ⚠️ Important: API Key Required

**To use this tap, you need your own Perplexity AI API key.** 

- Get one from https://www.perplexity.ai/ (free tier available)
- **NEVER share your API key** - it's like a password
- See [IMPORTANT_SECURITY_NOTE.md](IMPORTANT_SECURITY_NOTE.md) for details
- The example config uses a dummy key - replace it with your real key

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
