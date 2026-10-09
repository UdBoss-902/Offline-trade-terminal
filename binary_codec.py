import msgpack
import uuid
from typing import List, Dict, Any

def encode_trade_batch(trades: List[Dict[str, Any]]) -> bytes:
    """
    Serializes a batch of trade dictionaries into a compact binary byte stream using MessagePack.
    Reduces wire size by ~60-70% compared to standard JSON stringification.
    """
    packed_records = []
    for t in trades:
        # Convert string UUID to raw 16-byte binary representation to save space
        trade_uuid_bytes = uuid.UUID(t['trade_id']).bytes
        
        packed_records.append([
            trade_uuid_bytes,
            t['sku'],
            1 if t['side'] == 'BUY' else 0,  # Pack enum as 1-byte int
            t['quantity'],
            t['price_cents'],
            t['timestamp_ns']
        ])
    
    # Pack array of tuples into msgpack binary frame
    return msgpack.packb(packed_records, use_bin_type=True)


def decode_trade_batch(payload: bytes) -> List[Dict[str, Any]]:
    """
    Deserializes a binary byte stream frame back into structured trade dictionaries.
    Used by remote ingest endpoints or local sync validators.
    """
    raw_records = msgpack.unpackb(payload, raw=False)
    unpacked_trades = []

    for record in raw_records:
        trade_uuid_bytes, sku, side_int, qty, price_cents, timestamp_ns = record
        
        # Convert 16-byte binary back to canonical hyphenated UUID string
        trade_id_str = str(uuid.UUID(bytes=trade_uuid_bytes))
        
        unpacked_trades.append({
            "trade_id": trade_id_str,
            "sku": sku,
            "side": "BUY" if side_int == 1 else "SELL",
            "quantity": qty,
            "price_cents": price_cents,
            "timestamp_ns": timestamp_ns
        })

    return unpacked_trades


if __name__ == "__main__":
    # Sanity check codec roundtrip
    test_trade = {
        "trade_id": str(uuid.uuid4()),
        "sku": "SKU-BTC-USDT",
        "side": "BUY",
        "quantity": 5,
        "price_cents": 6450000,
        "timestamp_ns": 1728000000000000000
    }

    binary_data = encode_trade_batch([test_trade])
    decoded = decode_trade_batch(binary_data)

    print(f"Original size (est JSON): ~180 bytes")
    print(f"Binary wire size:        {len(binary_data)} bytes")
    print(f"Codec Integrity Check:   {'PASSED' if decoded[0] == test_trade else 'FAILED'}")