# ⚠️ IMPORTANT: API Key Security Notice

## What Happened During Development

During the development of tap-perplexity, I (the AI assistant) did **NOT** use a real Perplexity API key because:

1. **Security Best Practice**: API keys should NEVER be shared or exposed
2. **No Real API Access**: I don't have access to actual Perplexity API credentials
3. **Testing Strategy**: All testing was done with mocked data and dummy credentials

## What Was Actually Tested

### ✅ What I Tested
1. **Discovery Mode** - Using example/dummy API key
2. **Unit Tests** - All 15 tests using mocked API responses
3. **Code Structure** - Verified tap follows Singer.io specification
4. **Schema Validation** - Verified JSON schemas are valid

### ❌ What I Did NOT Test
1. **Sync Mode with Real API** - No real API calls were made
2. **Real Data Extraction** - No actual data was synced
3. **Production API Integration** - No live testing with Perplexity servers

## How YOU Can Test Sync Mode

### Step 1: Get Your Own API Key

**NEVER use someone else's API key!** Get your own from Perplexity:

1. Visit https://www.perplexity.ai/
2. Sign up or log in
3. Navigate to Settings → API
4. Generate your own API key (starts with `pplx-`)
5. **Keep it secret!** Never commit it to git or share it

### Step 2: Create Your Config

```bash
# Copy the example
cp config.json.example config.json

# Edit with YOUR API key (not someone else's!)
# Replace pplx-xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx with your actual key
nano config.json  # or use your favorite editor
```

### Step 3: Run Discovery Mode

```bash
tap-perplexity --config config.json --discover > catalog.json
```

### Step 4: Run Sync Mode

```bash
# Sync all selected streams
tap-perplexity --config config.json --catalog catalog.json

# Or sync to a file
tap-perplexity --config config.json --catalog catalog.json > output.json

# Or pipe to a Singer target
tap-perplexity --config config.json --catalog catalog.json | target-csv
```

## Why I Cannot Share an API Key

### Security Reasons

1. **Personal Account Risk**: API keys are tied to personal/company accounts
2. **Cost Implications**: Someone else could run up charges on your account
3. **Rate Limits**: Sharing keys causes rate limit conflicts
4. **Terms of Service**: Most APIs prohibit sharing credentials
5. **Security Best Practice**: Keys should be treated like passwords

### The Right Way

Each user/organization should:
- ✅ Get their own API key
- ✅ Store it securely (environment variables, secrets manager)
- ✅ Keep it out of version control
- ✅ Rotate it regularly
- ✅ Never share it with others

## Testing Without Real API Key

If you want to test the tap without a real API key, you can:

### Option 1: Unit Tests (Already Working)

```bash
# Run all unit tests with mocked data
python -m pytest tests/unit -v

# All 15 tests pass without needing a real API key
```

### Option 2: Mock the API Client

```python
from unittest.mock import Mock
from tap_perplexity.client import PerplexityClient

# Create a mock client
client = Mock(spec=PerplexityClient)
client.get_models.return_value = [
    {'id': 'test-model', 'object': 'model', 'created': 123, 'owned_by': 'test'}
]

# Test your code with the mock
```

### Option 3: Local Mock Server

Create a local server that mimics Perplexity API for testing.

## What to Do Next

### If You Want to Test the Tap

1. **Get your own Perplexity API key** (free tier available)
2. **Create config.json** with YOUR key
3. **Run discovery mode** to see available streams
4. **Run sync mode** to extract data
5. **Report any issues** you find

### If You Don't Have an API Key

1. **Review the code** - All source code is available
2. **Run unit tests** - No API key needed
3. **Read documentation** - See how it works
4. **Try with mock data** - Use the test fixtures

## Example: Testing with Your Own Key

Here's a complete example workflow:

```bash
# 1. Get code
git clone <this-repo>
cd tap-perplexity

# 2. Install
pip install -e .

# 3. Create config with YOUR key
cat > config.json << 'CONFIGEOF'
{
  "api_key": "pplx-YOUR-ACTUAL-KEY-HERE",
  "start_date": "2024-01-01T00:00:00Z",
  "user_agent": "tap-perplexity/1.0.0"
}
CONFIGEOF

# 4. Make sure config.json is in .gitignore (it already is)
# 5. Run discovery
tap-perplexity --config config.json --discover > catalog.json

# 6. Edit catalog.json to select streams you want

# 7. Run sync
tap-perplexity --config config.json --catalog catalog.json > output.json

# 8. Check the output
head -20 output.json
```

## Summary

- ❌ I did NOT use a real API key during development
- ❌ I cannot and will not share any API keys
- ✅ All unit tests pass with mocked data
- ✅ Discovery mode works with dummy credentials
- ✅ You need to get your own API key to test sync mode
- ✅ Instructions provided for how to do this safely

## Questions?

If you have questions about:
- **Getting an API key**: Check https://docs.perplexity.ai/
- **Using the tap**: See SETUP_GUIDE.md and HOW_TO_USE.md
- **Code issues**: Open a GitHub issue
- **Security concerns**: Never share your API key!

---

**Remember**: API keys are like passwords. Never share them!
