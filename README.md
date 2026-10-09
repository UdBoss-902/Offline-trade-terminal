# Offline-First Trade Terminal Prototype

> **High-Throughput Offline Execution Engine with Transactional Outbox Pattern and MessagePack Binary Serialization.**

## 📌 Overview

High-frequency trading terminals and point-of-sale systems operating in intermittent network environments cannot afford execution latency or data loss during network dropouts.

**`Offline-trade-terminal`** is an architectural prototype designed to handle **1,000+ local order executions per second** completely offline. It guarantees local durability and zero state ambiguity using SQLite in Write-Ahead Logging (WAL) mode paired with the **Transactional Outbox Pattern**. Once network connectivity is restored, a background sync daemon serializes pending records into compact **MessagePack binary payload frames** (reducing wire size by ~60–70% compared to JSON) and transmits them to a remote ingestion engine with client-side UUID idempotency verification.

---

## 🏗️ System Architecture

```
                                  OFFLINE LOCAL TERMINAL
┌──────────────────────────────────────────────────────────────────────────────────┐
│                                                                                  │
│   [ Local Trade Input ]                                                          │
│             │                                                                    │
│             ▼                                                                    │
│   ┌──────────────────────────────────────────────────────────────────────────┐   │
│   │                    SQLITE WAL TRANSACTION BLOCK                          │   │
│   ├──────────────────────────────────────────────────────────────────────────┤   │
│   │  1. INSERT INTO trades (trade_id, sku, side, qty, price, timestamp_ns)   │   │
│   │  2. INSERT INTO outbox_queue (trade_id, status='PENDING_SYNC')           │   │
│   └──────────────────────────────────────────────────────────────────────────┘   │
│                                                                                  │
└────────────────────────────────────────┬─────────────────────────────────────────┘
                                         │
                             (Network Connectivity Event)
                                         │
                                         ▼
                               ┌───────────────────┐
                               │  Sync Daemon      │
                               │ (Batch Collector) │
                               └─────────┬─────────┘
                                         │
                                         ▼
                               ┌───────────────────┐
                               │ MessagePack       │
                               │ Binary Packing    │
                               └─────────┬─────────┘
                                         │
                                         ▼ (Compact Binary Byte Stream)
                               ┌───────────────────┐
                               │ Remote Ingestion  │
                               │   Trade Engine    │
                               └───────────────────┘
```

---

## ⚡ Key Architectural Features

1. **Transactional Outbox Pattern:** Guarantees atomic local writes. Every order execution commits to both the primary `trades` log and the `outbox_queue` in a single SQLite transaction context.
2. **SQLite Write-Ahead Logging (WAL):** Configured with `PRAGMA journal_mode=WAL;` and `PRAGMA synchronous=NORMAL;` to achieve sub-millisecond local execution speed (<1ms per operation).
3. **MessagePack Binary Wire Protocol:** Replaces traditional JSON stringification with binary packing (UUIDs as 16-byte raw buffers, side enums as 1-byte integers). Achieves ~65% wire size reduction and eliminates JSON parsing overhead.
4. **Idempotency via Client-Side UUIDv4:** Prevents duplicate trade processing during re-transmissions or network retries by indexing client-generated UUID keys.
5. **Chunked Background Sync Daemon:** Sweeps pending outbox records in configurable batch chunks (e.g., 250 orders per frame) to prevent client/server memory spikes.

---

## 🗄️ Database Schema (`schema.sql`)

* **`trades`**: Stores the local record of executed transactions (`trade_id`, `sku`, `side`, `quantity`, `price_cents`, `timestamp_ns`).
* **`outbox_queue`**: Maintains sync status tracking (`PENDING_SYNC`, `IN_FLIGHT`, `SYNCED`, `FAILED`) with explicit indexes for high-volume sweeps.

---

## 🚀 Quick Start Guide

### 1. Prerequisites
* Python 3.10+

### 2. Environment Setup
Clone the repository and activate the virtual environment:

```powershell
# Create & Activate Virtual Environment
python -m venv .venv
.\.venv\Scripts\Activate.ps1   # On Windows

# Install Dependencies
pip install -r requirements.txt
```

### 3. Run Benchmark & Stress Simulation

Execute the simulation runner to execute 1,500 trades offline, observe local write throughput, compare payload wire sizes, and perform a full binary sync sweep:

```powershell
python test_simulation.py
```

---

## 📊 Performance Benchmarks

Sample simulation run on 1,500 trade executions:

| Metric | Measured Benchmark |
| :--- | :--- |
| **Local Offline Throughput** | ~2,500+ trades / second |
| **Local Transaction Latency** | < 0.4 ms / trade |
| **JSON Payload Size (250 trades)** | ~48,500 bytes |
| **MessagePack Binary Size (250 trades)** | ~16,800 bytes |
| **Bandwidth Savings** | **~65.3% reduction** |
| **Sync Daemon Processing Speed** | ~10,000+ trades / second |

---

## 🛠️ Stack

* **Language:** Python 3
* **Local Database:** SQLite3 (WAL Mode)
* **Serialization Protocol:** MessagePack (`msgpack`)
* **Environment Configuration:** `python-dotenv`