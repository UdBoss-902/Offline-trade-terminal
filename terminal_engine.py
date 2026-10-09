import sqlite3
import time
import uuid
from typing import List, Dict, Any, Optional

DB_PATH = "trade_terminal.db"

def init_db(db_path: str = DB_PATH, schema_path: str = "schema.sql") -> None:
    """
    Initializes the local SQLite database using schema.sql and enables WAL mode
    for concurrent, high-throughput local writes.
    """
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    
    with open(schema_path, "r") as f:
        conn.executescript(f.read())
        
    conn.commit()
    conn.close()


class TerminalEngine:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._enable_wal()

    def _get_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _enable_wal(self) -> None:
        with self._get_connection() as conn:
            conn.execute("PRAGMA journal_mode=WAL;")

    def execute_trade(
        self,
        sku: str,
        side: str,
        quantity: int,
        price_cents: int,
        trade_id: Optional[str] = None
    ) -> str:
        """
        Executes a trade locally in <1ms.
        Guarantees atomicity by inserting into 'trades' and 'outbox_queue' 
        in a single database transaction block.
        """
        if not trade_id:
            trade_id = str(uuid.uuid4())
            
        timestamp_ns = time.time_ns()

        conn = self._get_connection()
        try:
            with conn:  # Context manager starts transaction, commits on exit or rolls back on exception
                conn.execute(
                    """
                    INSERT INTO trades (trade_id, sku, side, quantity, price_cents, timestamp_ns)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (trade_id, sku, side, quantity, price_cents, timestamp_ns)
                )
                conn.execute(
                    """
                    INSERT INTO outbox_queue (trade_id, status)
                    VALUES (?, 'PENDING_SYNC')
                    """,
                    (trade_id,)
                )
            return trade_id
        finally:
            conn.close()

    def fetch_pending_outbox_batch(self, limit: int = 250) -> List[Dict[str, Any]]:
        """
        Fetches a batch of pending trades from the transactional outbox queue.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT t.trade_id, t.sku, t.side, t.quantity, t.price_cents, t.timestamp_ns
                FROM outbox_queue o
                JOIN trades t ON o.trade_id = t.trade_id
                WHERE o.status = 'PENDING_SYNC'
                ORDER BY o.id ASC
                LIMIT ?
                """,
                (limit,)
            )
            rows = cursor.fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def mark_outbox_synced(self, trade_ids: List[str]) -> None:
        """
        Updates outbox records to 'SYNCED' upon receiving server ACK.
        """
        if not trade_ids:
            return

        conn = self._get_connection()
        try:
            placeholders = ",".join(["?"] * len(trade_ids))
            with conn:
                conn.execute(
                    f"""
                    UPDATE outbox_queue
                    SET status = 'SYNCED', synced_at = CURRENT_TIMESTAMP
                    WHERE trade_id IN ({placeholders})
                    """,
                    trade_ids
                )
        finally:
            conn.close()

    def get_outbox_stats(self) -> Dict[str, int]:
        """
        Returns count of trade records grouped by status.
        """
        conn = self._get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT status, COUNT(*) as cnt FROM outbox_queue GROUP BY status"
            )
            rows = cursor.fetchall()
            stats = {row['status']: row['cnt'] for row in rows}
            return stats
        finally:
            conn.close()


if __name__ == "__main__":
    # Test local database initialization and trade write
    init_db()
    engine = TerminalEngine()

    print("Executing sample offline trades...")
    t1 = engine.execute_trade("SKU-AAPL", "BUY", 10, 18500)
    t2 = engine.execute_trade("SKU-TSLA", "SELL", 5, 24000)

    pending = engine.fetch_pending_outbox_batch(10)
    print(f"Recorded Trades in Outbox: {len(pending)}")
    print(f"Sample Pending Trade Payload: {pending[0]}")
    print(f"Outbox Queue Stats: {engine.get_outbox_stats()}")