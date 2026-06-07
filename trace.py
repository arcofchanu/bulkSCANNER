#!/usr/bin/env python3
"""
◎ SOL Bulk Wallet Scanner — Worker Queue Edition
Scans 10,000+ Solana addresses. Worker-queue pattern: N workers,
blocking queue, sentinel exit — every batch guaranteed to complete.
"""

import json
import asyncio
import aiohttp
import argparse
import sys
import time
import shutil
from pathlib import Path
from datetime import datetime
from collections import deque
import random

try:
    from rich.console import Console
    from rich.table import Table
    from rich.panel import Panel
    from rich.live import Live
    from rich.text import Text
    from rich import box
    HAS_RICH = True
except ImportError:
    HAS_RICH = False

console = Console(force_terminal=True, highlight=False) if HAS_RICH else None

# ── RPC Pool — free endpoints, user can inject paid ones via --add-rpc ─────────
BASE_RPCS = [
    "https://api.mainnet-beta.solana.com",
    "https://solana-api.projectserum.com",
    "https://rpc.ankr.com/solana",
    "https://mainnet.rpcpool.com",
    "https://solana-mainnet.rpc.extrnode.com",
    "https://solana.public-rpc.com",
]

LAMPORTS_PER_SOL    = 1_000_000_000
DEFAULT_CONCURRENCY = 25
DEFAULT_BATCH_SIZE  = 100
RETRY_LIMIT         = 6
REQUEST_TIMEOUT     = 12           # per-request timeout (seconds)
COOLDOWN_BASE       = 5.0          # seconds after a 429
COOLDOWN_MAX        = 60.0
TOKEN_BUCKET_RATE   = 8.0          # default req/s (raise with paid RPC)
TOKEN_BUCKET_BURST  = 15

_DONE = object()                   # sentinel to stop workers


# ── Token Bucket ──────────────────────────────────────────────────────────────
class TokenBucket:
    def __init__(self, rate: float, burst: float):
        self.rate      = rate
        self.tokens    = burst
        self.max       = burst
        self.last_fill = time.monotonic()
        self._lock     = asyncio.Lock()

    async def acquire(self):
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.max, self.tokens + (now - self.last_fill) * self.rate)
            self.last_fill = now
            if self.tokens >= 1:
                self.tokens -= 1
                return
            wait = (1 - self.tokens) / self.rate
        await asyncio.sleep(wait)
        async with self._lock:
            self.tokens = max(0, self.tokens - 1)


# ── RPC Pool with per-endpoint health tracking ─────────────────────────────────
class RPCPool:
    def __init__(self, endpoints: list):
        self._eps            = list(dict.fromkeys(endpoints))
        self._failures       = {ep: 0   for ep in self._eps}
        self._cooldown_until = {ep: 0.0 for ep in self._eps}
        self._lock           = asyncio.Lock()

    async def pick(self) -> str:
        """Block until a healthy endpoint is available."""
        backoff = 0.05
        while True:
            async with self._lock:
                now       = time.monotonic()
                available = [ep for ep in self._eps if self._cooldown_until[ep] <= now]
                if available:
                    return min(available, key=lambda ep: self._failures[ep])
                earliest = min(self._cooldown_until[ep] for ep in self._eps)
                wait     = max(backoff, earliest - now)
            await asyncio.sleep(wait)
            backoff = min(backoff * 1.5, 2.0)

    async def report_429(self, ep: str):
        async with self._lock:
            self._failures[ep] += 1
            cd = min(COOLDOWN_BASE * (2 ** min(self._failures[ep] - 1, 5)), COOLDOWN_MAX)
            self._cooldown_until[ep] = time.monotonic() + cd + random.uniform(0, 1.5)

    async def report_error(self, ep: str, cooldown: float = 3.0):
        async with self._lock:
            self._failures[ep] += 1
            self._cooldown_until[ep] = time.monotonic() + cooldown

    async def report_success(self, ep: str):
        async with self._lock:
            if self._failures[ep] > 0:
                self._failures[ep] = max(0, self._failures[ep] - 1)

    def status(self):
        now = time.monotonic()
        return {ep: max(0.0, round(self._cooldown_until[ep] - now, 1)) for ep in self._eps}

    @property
    def count(self):
        return len(self._eps)


# ── Live State ────────────────────────────────────────────────────────────────
class ScanState:
    def __init__(self, total_batches, total_addrs):
        self.total_batches = total_batches
        self.total_addrs   = total_addrs
        self.done_batches  = 0
        self.done_addrs    = 0
        self.funded        = 0
        self.empty         = 0
        self.errors        = 0
        self.rpc_calls     = 0
        self.rate_limited  = 0
        self.requeued      = 0
        self.start_time    = time.time()
        self.last_funded   = []
        self.lock          = asyncio.Lock()

    @property
    def elapsed(self):  return time.time() - self.start_time
    @property
    def speed(self):
        e = self.elapsed; return self.done_addrs / e if e > 0 else 0
    @property
    def pct(self):      return self.done_batches / self.total_batches if self.total_batches else 0
    @property
    def eta(self):
        if self.done_batches == 0: return "–"
        rate = self.done_batches / max(self.elapsed, 0.01)
        secs = (self.total_batches - self.done_batches) / rate if rate > 0 else 0
        return f"{int(secs//60)}m {int(secs%60)}s" if secs >= 60 else f"{secs:.0f}s"
    @property
    def elapsed_fmt(self):
        s = self.elapsed
        return f"{int(s//60)}m {int(s%60)}s" if s >= 60 else f"{s:.1f}s"


# ── Single batch RPC fetch ─────────────────────────────────────────────────────
async def fetch_batch(session, addresses, pool, bucket, state,
                      min_balance, with_balance, zero_balance, errors):
    """
    Fetch one batch of addresses via getMultipleAccounts.
    Retries up to RETRY_LIMIT times. On 429 → exponential cooldown on that endpoint.
    On empty/bad response → short cooldown, rotate. Never silently drops.
    """
    for attempt in range(RETRY_LIMIT):
        ep = await pool.pick()
        await bucket.acquire()

        try:
            payload = {
                "jsonrpc": "2.0", "id": 1,
                "method": "getMultipleAccounts",
                "params": [addresses, {"encoding": "base64", "commitment": "confirmed"}],
            }
            async with session.post(
                ep, json=payload,
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            ) as resp:

                # ── Rate limited ──────────────────────────────────────────────
                if resp.status == 429:
                    async with state.lock:
                        state.rate_limited += 1
                        state.requeued     += 1
                    await pool.report_429(ep)
                    await asyncio.sleep(random.uniform(1.0, 2.5) * (attempt + 1))
                    continue

                # ── Server error ──────────────────────────────────────────────
                if resp.status >= 500:
                    await pool.report_error(ep, cooldown=min(2 ** attempt, 10))
                    await asyncio.sleep(min(2 ** attempt, 10))
                    continue

                # ── Read + parse body ─────────────────────────────────────────
                raw = await resp.text()
                if not raw or not raw.strip():
                    await pool.report_error(ep, cooldown=3.0)
                    await asyncio.sleep(min(2 ** attempt, 10))
                    continue

                try:
                    data = json.loads(raw)
                except json.JSONDecodeError:
                    # HTML error page or garbage — hard cooldown on this endpoint
                    await pool.report_error(ep, cooldown=10.0)
                    await asyncio.sleep(min(2 ** attempt, 10))
                    continue

                # ── RPC-level error ───────────────────────────────────────────
                if "error" in data:
                    err  = data["error"]
                    code = err.get("code", 0) if isinstance(err, dict) else 0
                    if code == 429 or code == -32005:
                        async with state.lock:
                            state.rate_limited += 1
                            state.requeued     += 1
                        await pool.report_429(ep)
                        await asyncio.sleep(random.uniform(2.0, 4.0) * (attempt + 1))
                        continue
                    await pool.report_error(ep, cooldown=2.0)
                    await asyncio.sleep(min(2 ** attempt, 10))
                    continue

                # ── Success ───────────────────────────────────────────────────
                await pool.report_success(ep)
                accounts = data.get("result", {}).get("value", [])

                async with state.lock:
                    for addr, acct in zip(addresses, accounts):
                        if acct and "lamports" in acct:
                            bal = acct["lamports"] / LAMPORTS_PER_SOL
                            if bal >= min_balance and bal > 0:
                                with_balance[addr] = bal
                                state.funded += 1
                                snippet = f"{addr[:8]}…{addr[-6:]}  {bal:.4f} ◎"
                                state.last_funded = ([snippet] + state.last_funded)[:5]
                            else:
                                zero_balance[addr] = bal
                                state.empty += 1
                        else:
                            zero_balance[addr] = 0.0
                            state.empty += 1
                    state.done_addrs   += len(addresses)
                    state.done_batches += 1
                    state.rpc_calls    += 1
                return  # ← success, exit retry loop

        except asyncio.TimeoutError:
            # Hard timeout — this endpoint is slow, punish it more
            await pool.report_error(ep, cooldown=min(5 * (attempt + 1), 30))

        except (aiohttp.ClientError, aiohttp.ServerDisconnectedError,
                aiohttp.ClientOSError, aiohttp.ClientConnectionError):
            await pool.report_error(ep, cooldown=min(3 * (attempt + 1), 15))

    # ── All retries exhausted — count as errors, never stall ─────────────────
    async with state.lock:
        errors.extend(addresses)
        state.errors       += len(addresses)
        state.done_addrs   += len(addresses)
        state.done_batches += 1


# ── Worker: blocking queue.get() + sentinel exit ──────────────────────────────
async def worker(queue, session, pool, bucket, state,
                 min_balance, with_balance, zero_balance, errors):
    """
    Pulls batches from queue with blocking await queue.get().
    Stops ONLY when it receives the _DONE sentinel.
    This is the fix for the 3000-wallet cutoff:
      - Old code: asyncio.gather(*[100 coros]) — event loop silently drops tasks
      - New code: N workers blocking on a queue — zero tasks ever get lost
    """
    while True:
        item = await queue.get()       # blocks until work is available
        if item is _DONE:
            queue.task_done()
            break
        try:
            await fetch_batch(session, item, pool, bucket, state,
                              min_balance, with_balance, zero_balance, errors)
        finally:
            queue.task_done()


# ── Dashboard ─────────────────────────────────────────────────────────────────
def build_dashboard(state: ScanState, pool: RPCPool) -> Panel:
    W = max(shutil.get_terminal_size((100, 24)).columns - 4, 60)

    # progress bar
    bar_w  = max(20, W - 32)
    filled = int(bar_w * state.pct)
    bar    = Text()
    bar.append(" ")
    bar.append("█" * filled,           style="bold green")
    bar.append("░" * (bar_w - filled), style="dim white")
    bar.append(f"  {state.pct*100:5.1f}%", style="bold white")
    bar.append(f"  {state.done_addrs:,}/{state.total_addrs:,}", style="dim white")

    # stats grid
    g = Table(box=None, show_header=False, padding=(0, 3), expand=True)
    g.add_column(style="dim white",  width=18)
    g.add_column(style="bold white", width=13)
    g.add_column(style="dim white",  width=18)
    g.add_column(style="bold white", width=13)

    rl_style = "bold red" if state.rate_limited > 0 else "dim white"
    g.add_row("✅  Funded",    Text(f"{state.funded:,}",    style="bold green"),
              "⬜  Empty",     Text(f"{state.empty:,}",     style="dim white"))
    g.add_row("❌  Errors",    Text(f"{state.errors:,}",    style="red" if state.errors else "dim white"),
              "🔁  RPC Calls", Text(f"{state.rpc_calls:,}", style="dim white"))
    g.add_row("⚡  Speed",     Text(f"{state.speed:.0f} w/s",  style="cyan"),
              "⏱   Elapsed",   Text(state.elapsed_fmt,         style="yellow"))
    g.add_row("📦  Batches",   Text(f"{state.done_batches}/{state.total_batches}", style="white"),
              "⏳  ETA",        Text(state.eta,                  style="magenta"))
    g.add_row("🚦  429s",      Text(f"{state.rate_limited}",    style=rl_style),
              "🔄  Requeued",  Text(f"{state.requeued}",         style="yellow" if state.requeued else "dim white"))

    # RPC health row
    if pool:
        statuses = pool.status()
        healthy  = sum(1 for v in statuses.values() if v == 0)
        cooling  = pool.count - healthy
        rpc_txt  = Text()
        rpc_txt.append(f"{healthy}/{pool.count} healthy", style="green" if healthy > 0 else "red")
        if cooling:
            rpc_txt.append(f"  {cooling} cooling", style="yellow")
        rpc_row = Table(box=None, show_header=False, padding=(0, 1))
        rpc_row.add_column(style="dim white", width=18)
        rpc_row.add_column()
        rpc_row.add_row("🌐  RPC Pool", rpc_txt)
    else:
        rpc_row = Text()

    # live funded feed
    if state.last_funded:
        feed_body = Text()
        for line in state.last_funded:
            feed_body.append("● ", style="bold green")
            feed_body.append(line + "\n", style="cyan")
    else:
        feed_body = Text("No funded wallets found yet…", style="dim")

    feed = Panel(feed_body,
                 title="[bold yellow]◎ Recent Funded Finds[/bold yellow]",
                 border_style="yellow", padding=(0, 1))

    outer = Table(box=None, show_header=False, padding=(0, 0), expand=True)
    outer.add_column()
    outer.add_row(bar)
    outer.add_row("")
    outer.add_row(g)
    outer.add_row(rpc_row)
    outer.add_row("")
    outer.add_row(feed)

    return Panel(outer,
                 title="[bold cyan]◎ SOL Scanner — Live[/bold cyan]",
                 border_style="cyan", padding=(0, 1))


# ── Main Scanner ──────────────────────────────────────────────────────────────
async def scan_all(addresses, concurrency, batch_size, min_balance,
                   extra_rpcs=None, token_rate=TOKEN_BUCKET_RATE):

    # If user passed --add-rpc, those go first (higher priority = fewer failures)
    user_rpcs = extra_rpcs or []
    rpcs      = list(dict.fromkeys(user_rpcs + BASE_RPCS))   # user RPCs at front
    pool      = RPCPool(rpcs)
    bucket    = TokenBucket(rate=token_rate, burst=TOKEN_BUCKET_BURST)

    # ── Build queue: all batches + one _DONE per worker ───────────────────────
    queue   = asyncio.Queue()
    batches = [addresses[i:i + batch_size] for i in range(0, len(addresses), batch_size)]
    for batch in batches:
        await queue.put(batch)
    for _ in range(concurrency):     # one sentinel per worker
        await queue.put(_DONE)

    total_batches = len(batches)
    state         = ScanState(total_batches, len(addresses))
    with_balance  = {}
    zero_balance  = {}
    errors        = []

    connector = aiohttp.TCPConnector(
        limit=concurrency * 4,
        limit_per_host=25,
        ttl_dns_cache=300,
        enable_cleanup_closed=True,
        force_close=False,
    )

    async def render_loop(live, done_event):
        while not done_event.is_set():
            live.update(build_dashboard(state, pool))
            await asyncio.sleep(0.1)
        live.update(build_dashboard(state, pool))   # final frame

    async with aiohttp.ClientSession(connector=connector) as session:
        done_event   = asyncio.Event()
        worker_tasks = [
            asyncio.create_task(
                worker(queue, session, pool, bucket, state,
                       min_balance, with_balance, zero_balance, errors)
            )
            for _ in range(concurrency)
        ]

        with Live(build_dashboard(state, pool), console=console,
                  refresh_per_second=10, transient=False) as live:
            render_task = asyncio.create_task(render_loop(live, done_event))
            await asyncio.gather(*worker_tasks)      # wait for all workers to drain queue
            done_event.set()
            await render_task

    return with_balance, zero_balance, errors, state.elapsed, {
        "rpc_calls":    state.rpc_calls,
        "rate_limited": state.rate_limited,
        "requeued":     state.requeued,
    }


# ── Save Results ──────────────────────────────────────────────────────────────
def save_results(with_balance, zero_balance, errors, output_dir):
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")

    funded_sorted = dict(sorted(with_balance.items(), key=lambda x: x[1], reverse=True))

    with open(out / f"funded_wallets_{ts}.json", "w") as f:
        json.dump([{"address": a, "balance_sol": round(b, 9)}
                   for a, b in funded_sorted.items()], f, indent=2)

    with open(out / f"empty_wallets_{ts}.json", "w") as f:
        json.dump(list(zero_balance.keys()), f, indent=2)

    if errors:
        with open(out / f"error_wallets_{ts}.json", "w") as f:
            json.dump(errors, f, indent=2)

    with open(out / f"funded_summary_{ts}.csv", "w") as f:
        f.write("address,balance_sol\n")
        for a, b in funded_sorted.items():
            f.write(f"{a},{round(b, 9)}\n")


# ── Banner ────────────────────────────────────────────────────────────────────
def print_banner():
    if not HAS_RICH:
        print("=" * 62)
        print("  ◎  SOL BULK WALLET SCANNER — Worker Queue Edition")
        print("=" * 62)
        return
    console.print(Panel.fit(
        Text.from_markup(
            "[bold cyan]◎  SOL BULK WALLET SCANNER[/bold cyan]\n"
            "[dim]Worker Queue  •  Sentinel Exit  •  Zero Dropped Batches[/dim]"
        ), border_style="cyan", box=box.DOUBLE_EDGE, padding=(1, 4)
    ))
    console.print()


# ── Summary ───────────────────────────────────────────────────────────────────
def print_summary(with_balance, zero_balance, errors, elapsed, stats, output_dir):
    total = len(with_balance) + len(zero_balance) + len(errors)
    rate  = total / elapsed if elapsed > 0 else 0

    if not HAS_RICH:
        print(f"\nFunded:{len(with_balance)}  Empty:{len(zero_balance)}  Errors:{len(errors)}")
        print(f"Speed:{rate:.0f}/s  Elapsed:{elapsed:.1f}s  Out:{output_dir}/")
        return

    t = Table(title="[bold green]◎ Scan Complete[/bold green]",
              box=box.ROUNDED, border_style="green",
              show_header=True, header_style="bold cyan", min_width=50)
    t.add_column("Metric",  style="dim white", width=22)
    t.add_column("Value",   style="bold white")
    t.add_row("Total Scanned",  f"[white]{total:,}[/white]")
    t.add_row("✅ Funded",      f"[bold green]{len(with_balance):,}[/bold green]")
    t.add_row("⬜ Empty",       f"[dim]{len(zero_balance):,}[/dim]")
    t.add_row("❌ Errors",      f"[red]{len(errors):,}[/red]")
    t.add_row("⏱  Elapsed",     f"[yellow]{elapsed:.1f}s[/yellow]")
    t.add_row("⚡ Speed",        f"[cyan]{rate:.0f} wallets/s[/cyan]")
    t.add_row("🔁 RPC Calls",    f"[dim]{stats['rpc_calls']:,}[/dim]")
    t.add_row("🚦 429s total",   f"[{'red' if stats['rate_limited'] else 'dim'}]{stats['rate_limited']}[/{'red' if stats['rate_limited'] else 'dim'}]")
    t.add_row("🔄 Requeued",     f"[dim]{stats['requeued']}[/dim]")
    t.add_row("📁 Output",      f"[blue]{output_dir}/[/blue]")
    console.print(t)

    if with_balance:
        console.print()
        top = Table(title="[bold yellow]◎ Top Funded Wallets[/bold yellow]",
                    box=box.SIMPLE_HEAVY, border_style="yellow", header_style="bold yellow")
        top.add_column("#",           style="dim",        width=4)
        top.add_column("Address",     style="cyan",       width=46)
        top.add_column("SOL Balance", style="bold green", justify="right")
        for i, (addr, bal) in enumerate(
            sorted(with_balance.items(), key=lambda x: x[1], reverse=True)[:10], 1
        ):
            top.add_row(str(i), addr, f"{bal:.6f} ◎")
        console.print(top)

    console.print()
    console.print(Panel.fit(
        "[bold green]✅ funded_wallets.json[/bold green]  ← funded wallets (sorted by balance)\n"
        "[dim]⬜ empty_wallets.json   ← zero-balance addresses\n"
        "📊 funded_summary.csv   ← CSV for spreadsheet import[/dim]\n"
        f"\n[dim]Saved to [bold blue]{output_dir}/[/bold blue][/dim]",
        title="[bold]Output Files[/bold]", border_style="green"
    ))


# ── CLI ───────────────────────────────────────────────────────────────────────
def main():
    p = argparse.ArgumentParser(
        prog="sol_scanner",
        description="◎ SOL bulk wallet scanner — worker queue, zero dropped batches",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
How to use with Helius (recommended for 10k+):
  python sol_scanner.py wallets.json \\
      --add-rpc https://mainnet.helius-rpc.com/?api-key=YOUR_KEY \\
      -c 40 -r 30

  Helius free tier: 300k credits/month, 1 credit per getMultipleAccounts call.
  10k wallets @ batch 100 = 100 calls = 100 credits. You have 3000 free runs/month.

Free RPC only (slower, may hit limits):
  python sol_scanner.py wallets.json -c 15 -r 6

Other examples:
  python sol_scanner.py wallets.json --min-balance 0.01 -o results/
  python sol_scanner.py wallets.json --dry-run
        """
    )
    p.add_argument("input",               help='JSON file: ["addr1","addr2",...]')
    p.add_argument("--min-balance", "-m", type=float, default=0.0,
                   help="Min SOL to flag as funded (default: 0 = any nonzero balance)")
    p.add_argument("--concurrency", "-c", type=int,   default=DEFAULT_CONCURRENCY,
                   help=f"Worker count (default: {DEFAULT_CONCURRENCY}). Raise to 40+ with a paid RPC.")
    p.add_argument("--batch-size",  "-b", type=int,   default=DEFAULT_BATCH_SIZE,
                   help=f"Addresses per RPC call, max 100 (default: {DEFAULT_BATCH_SIZE})")
    p.add_argument("--rate",        "-r", type=float, default=TOKEN_BUCKET_RATE,
                   help=f"Global req/s cap via token bucket (default: {TOKEN_BUCKET_RATE}). Helius: use 25-40.")
    p.add_argument("--output",      "-o", default="scan_results",
                   help="Output directory (default: scan_results/)")
    p.add_argument("--add-rpc",           action="append", default=[], metavar="URL",
                   help="Add RPC endpoint — goes to front of pool (higher priority). Repeatable.")
    p.add_argument("--list-rpcs",         action="store_true", help="Print RPC pool and exit")
    p.add_argument("--dry-run",           action="store_true", help="Show config only, no scan")
    args = p.parse_args()

    user_rpcs = args.add_rpc
    all_rpcs  = list(dict.fromkeys(user_rpcs + BASE_RPCS))

    print_banner()

    if args.list_rpcs:
        if HAS_RICH:
            console.print("[bold cyan]RPC Pool (in priority order):[/bold cyan]")
            for i, ep in enumerate(all_rpcs, 1):
                tag = " [bold green]← user RPC[/bold green]" if ep in user_rpcs else ""
                console.print(f"  [dim]{i:2}.[/dim] {ep}{tag}")
        else:
            for ep in all_rpcs:
                print(f"  {ep}")
        sys.exit(0)

    # ── Load + validate ───────────────────────────────────────────────────────
    path = Path(args.input)
    if not path.exists():
        (console.print(f"[bold red]✗ File not found: {args.input}[/bold red]")
         if HAS_RICH else print(f"ERROR: {args.input} not found"))
        sys.exit(1)

    try:
        with open(path) as f:
            raw = json.load(f)
    except json.JSONDecodeError as e:
        (console.print(f"[bold red]✗ Invalid JSON: {e}[/bold red]")
         if HAS_RICH else print(f"ERROR: {e}"))
        sys.exit(1)

    if not isinstance(raw, list):
        (console.print("[bold red]✗ JSON must be a flat list of address strings.[/bold red]")
         if HAS_RICH else print("ERROR: need a list"))
        sys.exit(1)

    seen, clean, skipped = set(), [], 0
    for a in raw:
        if not isinstance(a, str) or len(a) < 32 or a in seen:
            skipped += 1; continue
        seen.add(a); clean.append(a)
    addresses  = clean
    batch_size = min(args.batch_size, 100)
    n_batches  = -(-len(addresses) // batch_size)   # ceiling division

    if HAS_RICH:
        cfg = Table(box=None, show_header=False, padding=(0, 1))
        cfg.add_column(style="dim",        width=22)
        cfg.add_column(style="bold white")
        cfg.add_row("Wallets to scan",
                    f"{len(addresses):,}" + (f"  [dim]({skipped} dupes/invalid skipped)[/dim]" if skipped else ""))
        cfg.add_row("Batches",       f"{n_batches}  ({batch_size} addrs each, getMultipleAccounts)")
        cfg.add_row("Workers",       f"{args.concurrency}  (blocking queue, sentinel exit)")
        cfg.add_row("Rate cap",      f"{args.rate:.0f} req/s  (token bucket)")
        cfg.add_row("RPC pool",      f"{len(all_rpcs)} endpoints" +
                    (f"  [bold green]({len(user_rpcs)} user)[/bold green]" if user_rpcs else "  [dim](free only)[/dim]"))
        cfg.add_row("Min balance",   f"{args.min_balance} SOL")
        cfg.add_row("Output",        f"{args.output}/")
        console.print(Panel(cfg, title="[bold]Config[/bold]", border_style="dim blue"))
        console.print()
        if not user_rpcs:
            console.print(
                Panel(
                    "[yellow]No paid RPC provided.[/yellow] Free RPCs may be slow or throttled at scale.\n"
                    "[dim]Add Helius free tier: [bold]--add-rpc https://mainnet.helius-rpc.com/?api-key=KEY[/bold]\n"
                    "Then raise rate: [bold]-r 30 -c 40[/bold][/dim]",
                    border_style="yellow", padding=(0, 1)
                )
            )
            console.print()

    if args.dry_run:
        (console.print("[yellow]Dry run — no scan performed.[/yellow]")
         if HAS_RICH else print("Dry run."))
        sys.exit(0)

    try:
        with_balance, zero_balance, errors, elapsed, stats = asyncio.run(
            scan_all(addresses, args.concurrency, batch_size,
                     args.min_balance, args.add_rpc, args.rate)
        )
    except KeyboardInterrupt:
        (console.print("\n[yellow]Interrupted by user.[/yellow]")
         if HAS_RICH else print("\nInterrupted."))
        sys.exit(0)

    save_results(with_balance, zero_balance, errors, args.output)
    console.print() if HAS_RICH else print()
    print_summary(with_balance, zero_balance, errors, elapsed, stats, args.output)


if __name__ == "__main__":
    main()