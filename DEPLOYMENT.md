# Deployment Guide

This guide covers deploying tap-perplexity in production environments.

## Installation Methods

### Package Installation

```bash
# Install from PyPI (when published)
pip install tap-perplexity

# Or install from GitHub
pip install git+https://github.com/singer-io/tap-perplexity.git
```

### Docker Deployment

Create a `Dockerfile`:

```dockerfile
FROM python:3.11-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY tap_perplexity /app/tap_perplexity
COPY setup.py /app/
RUN pip install -e .

ENTRYPOINT ["tap-perplexity"]
CMD ["--config", "config.json", "--catalog", "catalog.json"]
```

## Running in Production

### Basic Sync

```bash
tap-perplexity \
  --config /etc/tap-perplexity/config.json \
  --catalog /etc/tap-perplexity/catalog.json \
  --state /var/lib/tap-perplexity/state.json
```

### With Singer Target

```bash
tap-perplexity \
  --config config.json \
  --catalog catalog.json \
  --state state.json | \
target-postgres \
  --config target-config.json > state-new.json
```

## Scheduling

### Cron

```bash
# Runs every 6 hours
0 */6 * * * /opt/tap-perplexity/bin/run-sync.sh >> /var/log/tap-perplexity/cron.log 2>&1
```

### Apache Airflow

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'tap_perplexity_sync',
    start_date=datetime(2024, 1, 1),
    schedule_interval='0 */6 * * *',
    catchup=False,
)

sync_task = BashOperator(
    task_id='sync_perplexity',
    bash_command='/opt/tap-perplexity/bin/run-sync.sh',
    dag=dag,
)
```

## Security Best Practices

1. **Never commit API keys** - Use environment variables or secrets managers
2. **Restrict file permissions** - `chmod 600 config.json`
3. **Use HTTPS only** - The tap uses HTTPS by default
4. **Rotate API keys regularly**

## Monitoring

### Key Metrics

- Sync duration
- Records synced per stream
- API request count
- Error rate

### Health Check

```bash
#!/bin/bash
if tap-perplexity --config config.json --discover > /dev/null 2>&1; then
  echo "OK"
  exit 0
else
  echo "FAILED"
  exit 1
fi
```

## Troubleshooting

### Authentication Errors

Verify API key:
```bash
curl https://api.perplexity.ai/models \
  -H "Authorization: Bearer $PERPLEXITY_API_KEY"
```

### State File Issues

Validate state:
```bash
python -m json.tool state.json
```

Reset state (full resync):
```bash
rm state.json
```

## Backup Strategy

```bash
# Backup state before sync
cp state.json "state-$(date +%Y%m%d-%H%M%S).json.bak"

# Keep last 7 backups
find . -name "state-*.json.bak" -mtime +7 -delete
```
