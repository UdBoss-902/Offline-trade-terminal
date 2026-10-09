import time
import random
import json
from terminal_engine import TerminalEngine, init_db
from sync_daemon import SyncDaemon, MockRemoteServer
from binary_codec import encode_trade_batch

# Configurable Test Parameters
NUM_TRADES = 1500
BATCH_SIZE = 250
SKUS = ["SKU-AAPL", "SKU-TSLA", "SKU-NVDA", "SKU-MSFT", "SKU-AMZN"]

def run_simulation():
    print("=" * 60)
    print(f"STARTING SIMULATION: {NUM_TRADES} LOCAL OFFLINE TRADES")
    print("=" * 60)

    # 1. Reset database tables
    init_db()
    engine = TerminalEngine()
    server = MockRemoteServer()
    daemon = SyncDaemon(engine, server, batch_size=BATCH_SIZE)

    # 2. Phase 1: Simulate Offline Local Trade Execution
    print(f"\n[PHASE 1] Executing {NUM_TRADES} trades locally while offline...")
    start_offline = time.perf_counter()

    for _ in range(NUM_TRADES):
        sku = random.choice(SKUS)
        side = random.choice(["BUY", "SELL"])
        quantity = random.randint(1, 100)
        price_cents = random.randint(1000, 50000)
        
        engine.execute_trade(sku, side, quantity, price_cents)

    offline_duration = time.perf_counter() - start_offline
    offline_ops_per_sec = NUM_TRADES / offline_duration

    print(f"✓ Local Execution Completed in: {offline_duration:.4f} seconds")
    print(f"✓ Local Throughput:            {offline_ops_per_sec:.2f} trades/sec")
    print(f"✓ Outbox Queue State:          {engine.get_outbox_stats()}")

    # 3. Wire Size Comparison Metrics
    sample_batch = engine.fetch_pending_outbox_batch(limit=BATCH_SIZE)
    msgpack_size = len(encode_trade_batch(sample_batch))
    json_size = len(json.dumps(sample_batch).encode('utf-8'))
    compression_ratio = ((json_size - msgpack_size) / json_size) * 100

    print(f"\n[PAYLOAD OPTIMIZATION METRICS] ({BATCH_SIZE} trades per batch)")
    print(f"• Standard JSON String Size:  {json_size:,} bytes")
    print(f"• MessagePack Binary Size:   {msgpack_size:,} bytes")
    print(f"• Wire Bandwidth Savings:    {compression_ratio:.2f}% reduction")

    # 4. Phase 2: Simulate Network Restoration & Outbox Flush
    print(f"\n[PHASE 2] Network connection restored. Flushing outbox in binary batches of {BATCH_SIZE}...")
    
    sync_stats = daemon.flush_all()

    sync_throughput = sync_stats['total_synced'] / (sync_stats['duration_ms'] / 1000)

    print(f"✓ Total Trades Synced:        {sync_stats['total_synced']} / {NUM_TRADES}")
    print(f"✓ Total Binary Payload Sent:  {sync_stats['total_bytes_sent']:,} bytes")
    print(f"✓ Total Sync Duration:       {sync_stats['duration_ms']} ms")
    print(f"✓ Network Sync Speed:        {sync_throughput:.2f} trades/sec")
    print(f"✓ Final Local Outbox State:   {engine.get_outbox_stats()}")
    print(f"✓ Remote Server Received:     {len(server.received_trades)} trades verified")

    print("\n" + "=" * 60)
    print("SIMULATION COMPLETED SUCCESSFULLY")
    print("=" * 60)

if __name__ == "__main__":
    run_simulation()