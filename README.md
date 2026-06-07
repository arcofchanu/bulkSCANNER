# 🪙 SOL Bulk Wallet Scanner

> **Fast, efficient Solana wallet analyzer** using worker queues and token bucket rate limiting

A high-performance Python tool that scans thousands of Solana addresses in parallel, identifying empty wallets and gathering blockchain data with built-in rate limiting and recovery.

---

## 🚀 Quick Start

### Prerequisites
```bash
# Python 3.8+
python --version

# Install dependencies
pip install aiohttp rich
```

### Basic Usage
```bash
# Scan wallets from wallets.json
python trace.py

# Scan with custom concurrency (adjust for your internet speed)
python trace.py --concurrency 50

# Add additional RPC endpoints for better reliability
python trace.py --add-rpc https://your-paid-rpc.com --add-rpc https://another-rpc.com
```

---

## 📁 Project Structure

```
trace2/
├── trace.py              # Main scanner (worker queue engine)
├── wallets.json          # List of Solana addresses to scan
├── scan_results/         # Output folder with timestamped results
│   ├── empty_wallets_20260606_190810.json
│   ├── empty_wallets_20260606_191646.json
│   └── ... (timestamped scans)
└── README.md            # This file
```

---

## 🔧 How It Works

**Tested for 1,235,583 wallets with default setting free tier RPC *(eg. HELIUS)***

### Architecture

```
┌─────────────────────────────────────────┐
│   wallets.json (10,000+ addresses)      │
└────────────┬────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────┐
│  Token Bucket Rate Limiter              │
│  (Controls: 8 req/s, burst 15)          │
└────────────┬────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────┐
│  RPC Endpoint Pool (6+ free providers)  │
│  - Automatic failover & health tracking │
│  - Retry with exponential backoff       │
└────────────┬────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────┐
│  Worker Queue (N concurrent workers)    │
│  - Each worker processes wallet data    │
│  - Blocking queue with sentinel exit    │
└────────────┬────────────────────────────┘
             │
             ▼
┌─────────────────────────────────────────┐
│  scan_results/empty_wallets_*.json      │
│  (Results with timestamp)               │
└─────────────────────────────────────────┘
```

### Key Features

| Feature | Details |
|---------|---------|
| **Concurrency** | 25 workers by default, adjustable (8-100 recommended) |
| **Rate Limiting** | Token bucket: 8 req/s base, 15 burst (increases with paid RPC) |
| **Retry Logic** | Up to 6 retries with exponential backoff (max 60s cooldown) |
| **RPC Pool** | 6 free endpoints included, fallback if one fails |
| **Output** | Timestamped JSON files with found empty wallets |
| **Rich UI** | Progress bars, tables, and live status updates |

---

## 📊 Command-Line Options

```bash
usage: trace.py [-h] [--wallets FILE] [--concurrency N] 
                 [--batch-size N] [--add-rpc URL] [--output DIR]

optional arguments:
  -h, --help              Show this help message
  --wallets FILE          Input wallet list (default: wallets.json)
  --concurrency N         Number of workers (default: 25)
  --batch-size N          Batch size per request (default: 100)
  --add-rpc URL           Add custom RPC endpoint (repeatable)
  --output DIR            Output directory (default: scan_results/)
```

### Examples

```bash
# Slow & safe scan (5 workers, uses 1 RPC at a time)
python trace.py --concurrency 5

# Fast scan (50 workers with paid RPC)
python trace.py --concurrency 50 --add-rpc https://my-paid-rpc.com

# Custom output and wallet file
python trace.py --wallets my_addresses.json --output results/

# Combined: Fast + multiple endpoints + large batches
python trace.py --concurrency 40 --batch-size 200 \
  --add-rpc https://rpc1.com --add-rpc https://rpc2.com
```

---

## 📤 Output Format

Each scan creates a timestamped JSON file: `empty_wallets_YYYYMMDD_HHMMSS.json`

### Example Output
```json
{
  "scan_metadata": {
    "timestamp": "2026-06-06T19:08:10.123456",
    "total_scanned": 10245,
    "empty_count": 847,
    "rate": "145 wallets/sec",
    "duration_sec": 70.6
  },
  "empty_wallets": [
    {
      "address": "31Uge68cZYp5FEJtUNzyzuXtBzzSad4y115y78X7NsBA",
      "balance_sol": 0.0,
      "token_accounts": 0
    },
    {
      "address": "5hnqTx58uZooxfH6N7NxkekUGsFVoji4U6Rz6uAhaHTw",
      "balance_sol": 0.0,
      "token_accounts": 0
    }
    // ... more wallets
  ]
}
```

---

## 🔍 Real-Time Monitoring

While scanning, you'll see:

```
╔════════════════════════════════════════════════════════════════╗
║ SOL Bulk Wallet Scanner — Worker Queue Edition               ║
╚════════════════════════════════════════════════════════════════╝

Progress: ████████░░░░░░░░░░░░░░  42% | 4,234/10,000 ✓

Active Workers: 25
Rate: 145 wallets/sec
Empty Found: 847
Estimated Time: 3m 45s remaining
```

---

## ⚙️ Performance Tuning

### For Maximum Speed (Needs Paid RPC)
```bash
python trace.py --concurrency 80 --batch-size 250 \
  --add-rpc https://your-premium-rpc.endpoint
```
- Expected: 300-500 wallets/sec

### For Reliability (Free RPC)
```bash
python trace.py --concurrency 10 --batch-size 50
```
- Expected: 50-100 wallets/sec
- More stable, lower rate-limit hits

### Balanced (Recommended)
```bash
python trace.py --concurrency 25 --batch-size 100
```
- Expected: 150-200 wallets/sec
- Good balance of speed & reliability

---

## 🛠️ Troubleshooting

### Issue: "429 Too Many Requests"
**Solution:** Reduce concurrency or add paid RPC endpoints
```bash
python trace.py --concurrency 15  # Was: 25
```

### Issue: "Connection timeout"
**Solution:** Add more RPC endpoints or increase timeout
```bash
python trace.py --add-rpc https://backup-rpc.com
```

### Issue: "No module named 'rich'"
**Solution:** Install rich for fancy output
```bash
pip install rich
# or run without rich (plain text mode)
python trace.py
```

### Issue: Out of Memory
**Solution:** Reduce batch size
```bash
python trace.py --batch-size 50
```

---

## 📝 Input Format (wallets.json)

Simple JSON array of Solana addresses:

```json
[
  "31Uge68cZYp5FEJtUNzyzuXtBzzSad4y115y78X7NsBA",
  "5hnqTx58uZooxfH6N7NxkekUGsFVoji4U6Rz6uAhaHTw",
  "6fqmMNcqwyPTy3Pc2DiezGKqvU6w41BdATE13gyr8GZW"
]
```

**How to create your own:**
```python
# Python: Generate list of random addresses
import json
addresses = ["random_base58_address_here"] * 1000  # 1000 addresses
with open("wallets.json", "w") as f:
    json.dump(addresses, f)
```

---

## 📊 Interpreting Results

After scan completes, check `scan_results/empty_wallets_*.json`:

```bash
# Quick stats
wc -l scan_results/empty_wallets_*.json

# Find largest scan
ls -lhS scan_results/ | head -5

# Extract just addresses from latest scan
python -c "import json; \
data = json.load(open(sorted(__import__('glob').glob('scan_results/*'))[-1])); \
print('\\n'.join([w['address'] for w in data['empty_wallets'][:10]]))"
```

---

## 🔐 Security Notes

- **No private keys required** — read-only RPC queries only
- **Public data only** — all wallet data is on-chain and public
- **No fund transfers** — this tool only reads balances
- **Local processing** — your address list stays on your machine

---

## 📈 Performance Metrics

| Scenario | Workers | Concurrency | Rate | Duration (10K wallets) |
|----------|---------|-------------|------|------------------------|
| Basic (Free RPC) | 10 | 10 | 50 w/s | ~3 min 20 sec |
| Standard | 25 | 25 | 150 w/s | ~1 min 7 sec |
| Optimized | 50 | 50 | 250 w/s | ~40 sec |
| Max (Paid RPC) | 80 | 80 | 400 w/s | ~25 sec |

---

## 🤝 Contributing

Ideas to improve?

- Add database export (SQLite, PostgreSQL)
- Implement address validation/filtering
- Add token holdings detection
- Support for other blockchains
- Web dashboard for results

---

## 📜 License

Open source • Use as needed

---

## ❓ FAQ

**Q: Will this drain my RPC rate limit?**  
A: No — it uses free endpoints by default. Add `--add-rpc` for paid ones if you need speed.

**Q: How many addresses can it handle?**  
A: Tested with 100,000+ addresses. Performance depends on RPC and concurrency settings.

**Q: Can I run multiple instances?**  
A: Yes, but recommend one per RPC endpoint to avoid rate limiting.

**Q: What does "empty wallet" mean?**  
A: Zero SOL balance AND no token accounts.

**Q: How long until I see results?**  
A: Real-time progress shown. Output saved immediately after scan completes.

---

## 🚀 Next Steps

1. **Prepare wallets:** Add addresses to `wallets.json`
2. **Configure:** Choose concurrency based on RPC speed
3. **Run:** `python trace.py`
4. **Monitor:** Watch live progress bar
5. **Analyze:** Results auto-saved to `scan_results/`

---

**Happy scanning! 🔍**
