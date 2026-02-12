# Quick Start Guide - Choose Your Path

## Two Ways to Get Started

You have **TWO options** - pick the one that works best for you:

### 🚀 Option 1: Mock Mode (Instant - No Registration)

**Time: 30 seconds**

Perfect if you want to:
- Test the tap immediately
- See how it works
- Learn Singer.io taps
- Develop without API credentials

```bash
# Clone and install
git clone https://github.com/singer-io/tap-perplexity.git
cd tap-perplexity
pip install -e .

# Run discovery (mock mode)
tap-perplexity --config config-mock.json --discover > catalog.json

# Run sync (mock mode)
tap-perplexity --config config-mock.json --catalog catalog.json

# Done! You just extracted sample data without any API key!
```

**Output:**
```json
{"type": "RECORD", "stream": "models", "record": {"id": "pplx-7b-online", ...}}
{"type": "RECORD", "stream": "models", "record": {"id": "pplx-70b-online", ...}}
{"type": "RECORD", "stream": "models", "record": {"id": "pplx-7b-chat", ...}}
{"type": "RECORD", "stream": "models", "record": {"id": "pplx-70b-chat", ...}}
```

✅ No registration needed  
✅ Works offline  
✅ Sample data included  

---

### 🔑 Option 2: Real API (5 minutes - Real Data)

**Time: 5 minutes**

Perfect if you want to:
- Extract real data
- Use in production
- Get actual Perplexity models
- Build real pipelines

**Step 1: Get API Key (3 minutes)**

1. Visit https://www.perplexity.ai/
2. Sign up (email or OAuth)
3. Go to Settings → API
4. Generate API key
5. Copy the key (starts with `pplx-`)

**Step 2: Configure (30 seconds)**

```bash
# Create config with YOUR key
cat > config.json << 'CONFIGEOF'
{
  "api_key": "pplx-YOUR-REAL-KEY-HERE",
  "start_date": "2024-01-01T00:00:00Z",
  "user_agent": "tap-perplexity/1.0.0"
}
CONFIGEOF
```

**Step 3: Run (1 minute)**

```bash
# Discovery
tap-perplexity --config config.json --discover > catalog.json

# Sync
tap-perplexity --config config.json --catalog catalog.json

# Done! Real data extracted!
```

✅ Real data from Perplexity  
✅ Production ready  
✅ Full API access  

---

## Comparison

| Feature | Mock Mode | Real API |
|---------|-----------|----------|
| **Setup Time** | 30 seconds | 5 minutes |
| **Registration** | ❌ None | ✅ Required |
| **API Key** | ❌ None | ✅ Required |
| **Data** | Sample | Real |
| **Network** | ❌ None | ✅ Required |
| **Cost** | Free | Free tier |
| **Use Case** | Testing, Learning | Production |

---

## Which Should I Choose?

### Choose Mock Mode If:

- ⚡ You want to start RIGHT NOW
- 🧪 Just testing/learning
- 🔬 Developing tap code
- 📚 Following a tutorial
- 🚫 Don't want to register

### Choose Real API If:

- 📊 Need actual data
- 🏭 Production use
- 📈 Real analytics
- ✅ Have 5 minutes to setup

---

## Try Mock Mode First!

**We recommend starting with mock mode** to see how it works, then switching to real API when you're ready.

```bash
# Start with mock mode (instant)
tap-perplexity --config config-mock.json --discover

# See how it works...

# Then get real API key and switch
tap-perplexity --config config.json --discover
```

---

## Next Steps

### After Mock Mode:

1. Read [MOCK_MODE_GUIDE.md](MOCK_MODE_GUIDE.md) for details
2. When ready, get real API key
3. Switch to real config.json
4. Extract real data!

### After Real API:

1. Read [SETUP_GUIDE.md](SETUP_GUIDE.md) for advanced features
2. Configure stream selection
3. Set up scheduling
4. Choose a Singer target

---

## Need Help?

- **Mock Mode**: See [MOCK_MODE_GUIDE.md](MOCK_MODE_GUIDE.md)
- **Real API**: See [SETUP_GUIDE.md](SETUP_GUIDE.md)
- **Why No Account?**: See [WHY_I_CANNOT_CREATE_ACCOUNT.md](WHY_I_CANNOT_CREATE_ACCOUNT.md)
- **General Help**: See [HOW_TO_USE.md](HOW_TO_USE.md)

---

**Start now with mock mode - no registration required! 🎉**

```bash
tap-perplexity --config config-mock.json --discover
```
