# Contributing to tap-perplexity

Thank you for your interest in contributing to tap-perplexity! This document provides guidelines and instructions for contributing.

## Code of Conduct

Please be respectful and constructive in all interactions.

## How to Contribute

### Reporting Bugs

Before creating a bug report:
1. Check existing issues to avoid duplicates
2. Verify you're using the latest version
3. Collect relevant information (error messages, logs, config)

Create a bug report with:
- Clear, descriptive title
- Steps to reproduce
- Expected vs actual behavior
- Version information
- Relevant logs or error messages

### Suggesting Enhancements

Enhancement suggestions are welcome! Please:
1. Check if the enhancement is already suggested
2. Provide a clear use case
3. Explain why this would be useful to most users
4. Include examples if applicable

### Pull Requests

1. **Fork the repository**
   ```bash
   git clone https://github.com/YOUR-USERNAME/tap-perplexity.git
   cd tap-perplexity
   ```

2. **Create a branch**
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**
   - Follow the code style (use `black` for formatting)
   - Add tests for new functionality
   - Update documentation as needed

4. **Run tests**
   ```bash
   python -m pytest tests/ -v
   ```

5. **Commit your changes**
   ```bash
   git add .
   git commit -m "Add feature: description"
   ```

6. **Push to your fork**
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request**
   - Provide a clear description
   - Reference related issues
   - Include test results

## Development Setup

### Prerequisites
- Python 3.7+
- pip
- virtualenv (recommended)

### Setup

```bash
# Clone the repo
git clone https://github.com/singer-io/tap-perplexity.git
cd tap-perplexity

# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install in development mode
pip install -e '.[dev]'

# Set up pre-commit hooks (optional)
pip install pre-commit
pre-commit install
```

## Code Style

### Python Code Style
- Follow PEP 8
- Use `black` for formatting:
  ```bash
  black tap_perplexity tests
  ```
- Use `pylint` for linting:
  ```bash
  pylint tap_perplexity
  ```

### Documentation Style
- Use clear, concise language
- Include code examples
- Keep README up to date
- Document all public APIs

## Testing

### Writing Tests

1. **Unit Tests** - Test individual components
   - Place in `tests/unit/`
   - Mock external dependencies
   - Fast execution

2. **Integration Tests** - Test API integration
   - Place in `tests/integration/`
   - Require valid API credentials
   - May be slower

### Running Tests

```bash
# Unit tests only
python -m pytest tests/unit -v

# Integration tests only
python -m pytest tests/integration -v

# All tests
python -m pytest tests/ -v

# With coverage
python -m pytest tests/ --cov=tap_perplexity --cov-report=html
```

## Project Structure

```
tap-perplexity/
├── tap_perplexity/
│   ├── __init__.py       # Main entry point
│   ├── client.py         # API client
│   ├── streams.py        # Stream definitions
│   ├── sync.py           # Sync logic
│   └── schemas/          # JSON schemas
│       ├── models.json
│       └── chat_completions.json
├── tests/
│   ├── unit/             # Unit tests
│   └── integration/      # Integration tests
├── setup.py              # Package configuration
├── README.md             # User documentation
├── SETUP_GUIDE.md        # Setup instructions
├── CHANGELOG.md          # Version history
└── CONTRIBUTING.md       # This file
```

## Adding New Streams

To add a new stream:

1. **Create schema file**
   ```json
   // tap_perplexity/schemas/new_stream.json
   {
     "type": "object",
     "properties": {
       "id": {"type": ["null", "string"]},
       ...
     }
   }
   ```

2. **Create stream class**
   ```python
   # In tap_perplexity/streams.py
   class NewStream(Stream):
       tap_stream_id = 'new_stream'
       key_properties = ['id']
       replication_method = 'INCREMENTAL'
       replication_key = 'updated_at'
       
       @classmethod
       def sync(cls, client, config, state, catalog_stream):
           # Implementation
           pass
   ```

3. **Register stream**
   ```python
   STREAMS = {
       'models': ModelsStream(),
       'new_stream': NewStream(),
   }
   ```

4. **Add tests**
   - Unit tests in `tests/unit/test_streams.py`
   - Integration tests in `tests/integration/test_integration.py`

5. **Update documentation**
   - Add stream description to README.md
   - Update CHANGELOG.md

## Release Process

1. Update version in `setup.py`
2. Update `CHANGELOG.md` with changes
3. Run full test suite
4. Create git tag: `git tag -a v1.0.0 -m "Version 1.0.0"`
5. Push tag: `git push origin v1.0.0`
6. Create GitHub release
7. Publish to PyPI (maintainers only)

## Questions?

- Open an issue for questions
- Check existing issues and documentation
- Review Singer.io documentation

Thank you for contributing!
