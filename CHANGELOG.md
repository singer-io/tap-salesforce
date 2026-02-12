# Changelog

All notable changes to tap-perplexity will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.0] - 2024-02-12

### Added
- Initial release of tap-perplexity
- Support for Perplexity AI API authentication
- Models stream with FULL_TABLE replication
- Chat completions stream (placeholder for future implementation)
- Comprehensive unit test suite
- Integration test framework
- Automatic retry with exponential backoff
- Rate limit handling (429 status codes)
- JSON schema definitions for all streams
- Singer.io specification compliance
- Discovery mode for stream catalog generation
- State management for incremental syncs
- Configurable user agent
- Detailed documentation and setup guide

### Streams
- `models`: Lists all available Perplexity AI models
  - Primary key: `id`
  - Replication method: FULL_TABLE
  
- `chat_completions`: Placeholder for chat completion history
  - Primary key: `id`
  - Replication method: INCREMENTAL
  - Replication key: `created_at`
  - Note: Requires custom data source implementation

### Dependencies
- requests==2.32.5
- singer-python==6.3.0
- backoff==2.2.1

### Development Dependencies
- pytest==7.4.3
- pytest-cov==4.1.0
- pylint==3.0.3
- black==23.12.1

[1.0.0]: https://github.com/singer-io/tap-perplexity/releases/tag/v1.0.0
