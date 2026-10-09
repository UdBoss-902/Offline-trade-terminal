import time
from typing import List, Dict, Any, Tuple
from terminal_engine import TerminalEngine
from binary_codec import encode_trade_batch, decode_trade_batch


class MockRemoteServer:
    """
    Simulates a remote high-throughput trade engine endpoint.
    Receives binary payload frames, decodes them, and returns ACK with processed trade_ids.
    """
    def __init__(self):
        self.received_trades: List[Dict[str, Any]] = []

    def receive_binary_payload(self, payload_bytes: bytes) -> Dict[str, Any]:
        # 1. Decode binary wire payload back into structured trade records
        decoded_trades = decode_trade_batch(payload_bytes)
        
        # 2. Append to server-side trade store
        self.received_trades.extend(decoded_trades)
        
        # 3. Extract trade_ids for atomic ACK response
        acked_ids = [t['trade_id'] for t in decoded_trades]
        
        return {
            "status": "200_OK",
            "processed_count": len(acked_ids),
            "acked_trade_ids": acked_ids,
            "bytes_processed": len(payload_bytes)
        }


class SyncDaemon:
    def __init__(self, engine: TerminalEngine, server: MockRemoteServer, batch_size: int = 250):
        self.engine = engine
        self.server = server
        self.batch_size = batch_size

    def sync_once(self) -> Tuple[int, int]:
        """
        Executes a single sync cycle:
        1. Fetch up to `batch_size` pending records from SQLite outbox.
        2. Binary pack records into a compact wire frame.
        3. Send binary payload frame to remote endpoint.
        4. Mark trade IDs as 'SYNCED' upon receiving ACK.
        
        Returns: (processed_count, payload_byte_size)
        """
        pending_trades = self.engine.fetch_pending_outbox_batch(limit=self.batch_size)
        if not pending_trades:
            return 0, 0

        # Encode records into MessagePack binary payload
        binary_payload = encode_trade_batch(pending_trades)
        
        # Transmit binary payload frame to server
        response = self.server.receive_binary_payload(binary_payload)

        # Mark acknowledged trade IDs as synced in local SQLite outbox
        if response.get("status") == "200_OK":
            acked_ids = response.get("acked_trade_ids", [])
            self.engine.mark_outbox_synced(acked_ids)
            return len(acked_ids), len(binary_payload)

        return 0, 0

    def flush_all(self) -> Dict[str, Any]:
        """
        Continuously drains the outbox in chunked binary batches until no pending records remain.
        """
        total_synced = 0
        total_bytes_sent = 0
        start_time = time.perf_counter()

        while True:
            synced_count, payload_size = self.sync_once()
            if synced_count == 0:
                break
            total_synced += synced_count
            total_bytes_sent += payload_size

        duration_ms = (time.perf_counter() - start_time) * 1000
        return {
            "total_synced": total_synced,
            "total_bytes_sent": total_bytes_sent,
            "duration_ms": round(duration_ms, 2)
        }


if __name__ == "__main__":
    # Test sync daemon execution against local SQLite engine
    engine = TerminalEngine()
    server = MockRemoteServer()
    daemon = SyncDaemon(engine, server, batch_size=250)

    print("Initial Outbox State:", engine.get_outbox_stats())
    sync_result = daemon.flush_all()
    print("Sync Daemon Cycle Result:", sync_result)
    print("Post-Sync Outbox State:", engine.get_outbox_stats())
    print(f"Remote Server Received Total: {len(server.received_trades)} trades")