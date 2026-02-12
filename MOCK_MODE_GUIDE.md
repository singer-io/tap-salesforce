# Mock Mode Guide - Test Without an API Key!

## What is Mock Mode?

Mock mode allows you to test tap-perplexity **without creating an account or getting an API key**. Perfect for:

- 🎯 Testing the tap functionality
- 🧪 Development and debugging
- 📚 Learning how Singer taps work
- 🚀 Quick demonstrations
- 🔬 CI/CD pipelines without credentials

## How to Use Mock Mode

### Option 1: Use Pre-Made Mock Config

```bash
# Use the mock configuration file
tap-perplexity --config config-mock.json --discover > catalog.json
tap-perplexity --config config-mock.json --catalog catalog.json
```

### Option 2: Create Your Own Mock Config

```bash
cat > my-mock-config.json << 'EOF'
{
  "api_key": "mock",
  "start_date": "2024-01-01T00:00:00Z",
  "user_agent": "tap-perplexity-demo/1.0.0"
}
EOF

tap-perplexity --config my-mock-config.json --discover
```

### Option 3: Use Special API Key Values

Any of these values will trigger mock mode:

- `"api_key": "mock"`
- `"api_key": "demo"`
- `"api_key": "test-api-key"`
- `"api_key": "mock-api-key"`
- `"api_key": "pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"` (the example key)

## Quick Start (30 Seconds)

```bash
# 1. Run discovery with mock mode
tap-perplexity --config config-mock.json --discover > catalog.json

# 2. Select streams (already done in example)
cp catalog.json.example catalog.json

# 3. Run sync
tap-perplexity --config config-mock.json --catalog catalog.json

# Done! You just ran the tap without any API key!
```

## When to Use Mock Mode

### Use Mock Mode When:

- 🧪 Testing the tap installation
- 📚 Learning how it works
- 🔬 Running in CI/CD without credentials
- 🎯 Developing/debugging tap code
- 🚀 Quick demonstrations

### Use Real API When:

- 📊 Extracting actual data
- 🏭 Production deployments
- 📈 Analytics and reporting

## Getting a Real API Key (Still Easy!)

When ready for real data:

1. Visit https://www.perplexity.ai/
2. Sign up (free, 5 minutes)
3. Get API key from Settings
4. Update config.json
5. Run the same commands!

**Try mock mode now - no registration needed! 🎉**
