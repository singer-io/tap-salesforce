# Project Structure

This document explains the organization and purpose of files in tap-perplexity.

## Directory Layout

```
tap-perplexity/
│
├── tap_perplexity/           # Main package directory
│   ├── __init__.py           # Entry point and main orchestration
│   ├── client.py             # Perplexity API client
│   ├── streams.py            # Stream definitions and sync logic
│   ├── sync.py               # Sync coordination functions
│   └── schemas/              # JSON schema definitions
│       ├── models.json       # Schema for models stream
│       └── chat_completions.json  # Schema for chat_completions stream
│
├── tests/                    # Test suite
│   ├── __init__.py
│   ├── unit/                 # Unit tests (fast, no external dependencies)
│   │   ├── __init__.py
│   │   ├── test_client.py    # Tests for API client
│   │   └── test_streams.py   # Tests for stream classes
│   └── integration/          # Integration tests (require API key)
│       ├── __init__.py
│       └── test_integration.py
│
├── setup.py                  # Package configuration and dependencies
├── MANIFEST.in               # Files to include in distribution
├── requirements.txt          # Runtime dependencies
├── requirements-dev.txt      # Development dependencies
│
├── README.md                 # Overview and quick start
├── SETUP_GUIDE.md            # Detailed setup instructions
├── PROJECT_STRUCTURE.md      # This file
├── CHANGELOG.md              # Version history
├── CONTRIBUTING.md           # Contribution guidelines
├── LICENSE                   # Apache 2.0 license
│
├── config.json.example       # Sample configuration file
├── catalog.json.example      # Sample catalog file
├── state.json.example        # Sample state file
│
├── pytest.ini                # Pytest configuration
├── .pylintrc                 # Pylint configuration
├── .gitignore                # Git ignore rules
│
├── quickstart.sh             # Quick setup script
└── run_tests.sh              # Test runner script
```

## Core Components

### tap_perplexity/__init__.py

**Purpose:** Main entry point for the tap

**Key Functions:**
- `main()` - Entry point called by CLI
- `main_impl()` - Main implementation logic
- `discover()` - Discovery mode to output catalog
- `do_sync()` - Sync mode to extract data
- `sync_stream()` - Sync a single stream

**Flow:**
1. Parse command-line arguments
2. Initialize Perplexity API client
3. Run discovery or sync based on arguments
4. Handle errors and logging

### tap_perplexity/client.py

**Purpose:** Perplexity API client with retry logic

**Key Components:**
- `PerplexityClient` class
  - Handles authentication (API key)
  - Makes HTTP requests with retry
  - Implements exponential backoff for rate limits
  - Provides methods for each API endpoint

**Features:**
- Automatic retry on transient errors (429, 500, 502, 503, 504)
- Connection error and timeout handling
- Rate limit detection
- Request logging

### tap_perplexity/streams.py

**Purpose:** Stream definitions and sync logic

**Key Components:**

1. `Stream` (base class)
   - Common functionality for all streams
   - Schema loading
   - Metadata generation
   - Abstract sync method

2. `ModelsStream`
   - Lists available Perplexity AI models
   - FULL_TABLE replication
   - Primary key: `id`

3. `ChatCompletionsStream`
   - Placeholder for chat completions
   - INCREMENTAL replication
   - Replication key: `created_at`
   - Note: Requires custom data source

4. `STREAMS` dictionary
   - Registry of all available streams

### tap_perplexity/sync.py

**Purpose:** Sync coordination and utilities

**Key Functions:**
- Stream selection checking
- Sync orchestration helpers

### tap_perplexity/schemas/

**Purpose:** JSON schema definitions for each stream

**Format:** Standard JSON Schema (Draft 4)

**Properties:**
- Type definitions
- Descriptions
- Required fields
- Format specifications (e.g., date-time)

## Singer.io Specification Compliance

The tap follows the [Singer specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md):

### Messages

1. **SCHEMA** - Describes stream structure
   ```json
   {
     "type": "SCHEMA",
     "stream": "models",
     "schema": {...},
     "key_properties": ["id"]
   }
   ```

2. **RECORD** - Contains data
   ```json
   {
     "type": "RECORD",
     "stream": "models",
     "record": {...}
   }
   ```

3. **STATE** - Tracks sync progress
   ```json
   {
     "type": "STATE",
     "value": {...}
   }
   ```

### Catalog Structure

```json
{
  "streams": [
    {
      "stream": "stream_name",
      "tap_stream_id": "stream_name",
      "schema": {...},
      "metadata": [...],
      "key_properties": ["id"],
      "replication_method": "FULL_TABLE|INCREMENTAL",
      "replication_key": "updated_at"  // if INCREMENTAL
    }
  ]
}
```

### Metadata

Metadata provides information about streams and fields:

- `table-key-properties` - Primary key fields
- `forced-replication-method` - Replication strategy
- `valid-replication-keys` - Available bookmark fields
- `inclusion` - Field selection (automatic, available, unsupported)
- `selected` - Whether to sync this stream/field

## Testing Architecture

### Unit Tests

**Location:** `tests/unit/`

**Characteristics:**
- Fast execution (< 2 seconds total)
- No external dependencies
- Use mocks for API calls
- Test individual components in isolation

**Coverage:**
- Client initialization and configuration
- API request handling and retries
- Error handling and backoff logic
- Stream properties and metadata
- Schema loading

### Integration Tests

**Location:** `tests/integration/`

**Characteristics:**
- Require valid API credentials
- Make real API calls
- Test end-to-end functionality
- Slower execution

**Coverage:**
- Real API connectivity
- Actual data retrieval
- Full sync flow
- Error handling with real API

### Running Tests

```bash
# All unit tests
python -m pytest tests/unit -v

# With coverage
python -m pytest tests/unit --cov=tap_perplexity --cov-report=html

# Integration tests (requires config.json)
python -m pytest tests/integration -v

# All tests
python -m pytest tests/ -v

# Using the helper script
./run_tests.sh --all --coverage
```

## Configuration Files

### setup.py

Defines package metadata and dependencies:
- Package name, version, description
- Author and URL
- Dependencies (install_requires)
- Development dependencies (extras_require)
- Entry points for CLI
- Package discovery

### pytest.ini

Pytest configuration:
- Test discovery patterns
- Default options
- Output formatting

### .pylintrc

Pylint code quality configuration:
- Enabled/disabled checks
- Code style rules
- Line length limits

### .gitignore

Files to exclude from Git:
- Python cache files (`__pycache__`, `*.pyc`)
- Virtual environments (`venv/`)
- IDE files (`.vscode/`, `.idea/`)
- Sensitive data (`config.json`, `state.json`)
- Build artifacts (`dist/`, `*.egg-info`)

## Data Flow

### Discovery Mode

```
tap-perplexity --config config.json --discover
    ↓
main_impl()
    ↓
discover(client)
    ↓
For each stream in STREAMS:
    - Load schema from JSON
    - Generate metadata
    - Build catalog entry
    ↓
Output catalog as JSON
```

### Sync Mode

```
tap-perplexity --config config.json --catalog catalog.json
    ↓
main_impl()
    ↓
do_sync(client, config, catalog, state)
    ↓
For each selected stream:
    - Write SCHEMA message
    - sync_stream()
        ↓
        Stream.sync()
            - Get data from API
            - Transform records
            - Write RECORD messages
            - Update state
    - Write STATE message
```

## Adding New Features

### Adding a New Stream

1. Create schema file: `tap_perplexity/schemas/new_stream.json`
2. Create stream class in `tap_perplexity/streams.py`
3. Register in `STREAMS` dictionary
4. Add tests in `tests/unit/test_streams.py`
5. Update documentation

### Adding a New API Endpoint

1. Add method to `PerplexityClient` in `client.py`
2. Add tests in `tests/unit/test_client.py`
3. Use in stream's sync method

### Modifying Schema

1. Update JSON file in `schemas/`
2. Run discovery to verify changes
3. Update tests if needed
4. Document breaking changes

## Dependencies

### Runtime Dependencies

- **requests** (2.32.5) - HTTP client for API calls
- **singer-python** (6.3.0) - Singer specification implementation
- **backoff** (2.2.1) - Exponential backoff for retries

### Development Dependencies

- **pytest** (7.4.3) - Testing framework
- **pytest-cov** (4.1.0) - Coverage reporting
- **pylint** (3.0.3) - Code linting
- **black** (23.12.1) - Code formatting

## Best Practices

### Code Style

- Follow PEP 8
- Use `black` for formatting
- Use type hints where appropriate
- Write docstrings for public APIs

### Testing

- Write tests for all new features
- Maintain > 80% code coverage
- Mock external dependencies in unit tests
- Add integration tests for API changes

### Documentation

- Update README for user-facing changes
- Update CHANGELOG for all changes
- Add docstrings for new functions
- Include examples in documentation

### Error Handling

- Use appropriate exception types
- Log errors with context
- Provide helpful error messages
- Implement retry logic for transient errors

## Troubleshooting

### Common Issues

1. **Import Errors**
   - Reinstall: `pip install -e .`
   - Check Python version

2. **Test Failures**
   - Update dependencies: `pip install -r requirements-dev.txt`
   - Check for API changes

3. **API Errors**
   - Verify API key
   - Check rate limits
   - Review Perplexity API documentation

## Resources

- [Singer.io Specification](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md)
- [Perplexity AI API Docs](https://docs.perplexity.ai/)
- [Python Packaging Guide](https://packaging.python.org/)
- [Pytest Documentation](https://docs.pytest.org/)
