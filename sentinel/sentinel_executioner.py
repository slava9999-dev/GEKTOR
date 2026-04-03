# sentinel/sentinel_executioner.py
"""
╔══════════════════════════════════════════════════════════════════════╗
║  SENTINEL EXECUTIONER v2.0 — The Autonomous Failsafe (AWS Lambda)  ║
║  Zero Dependencies: Pure Python 3.11 (stdlib only)                 ║
║  Trigger: EventBridge (every 15s) or API Gateway (manual)          ║
╚══════════════════════════════════════════════════════════════════════╝

Architecture:
    1. Check DynamoDB for heartbeat item (TTL-based Dead Man's Switch).
       - Item exists → Server alive → EXIT.
       - Item expired → Server dead ≥ 30s → Proceed to Phase 2.
    2. Fetch open positions from Bybit V5 (Ground Truth).
    3. For each position: Cancel All Orders → Market Close (Reduce Only).
    4. Retry up to 3 times per symbol with exponential backoff.

Design Decisions:
    - GET signature uses query_string; POST uses json_body (Bybit V5 spec).
    - orderLinkId prefixed with 'SENTINEL_' for audit trail isolation.
    - All errors logged to CloudWatch for post-mortem analysis.
    - No external dependencies (requests, redis-py, boto3 used only for DynamoDB).

Environment Variables (set in Lambda Configuration):
    SENTINEL_BYBIT_KEY    — API key (Orders + Positions permissions ONLY)
    SENTINEL_BYBIT_SECRET — API secret
    BYBIT_BASE_URL        — https://api.bybit.com (or proxy endpoint)
    HEARTBEAT_TABLE       — DynamoDB table name for heartbeat
    HEARTBEAT_PK          — Partition key value (e.g. "tekton_alpha")
"""

import os
import json
import time
import hmac
import hashlib
import urllib.request
import urllib.parse
import urllib.error
import boto3

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION (Lambda Environment Variables)
# ═══════════════════════════════════════════════════════════════
API_KEY = os.environ.get("SENTINEL_BYBIT_KEY", "")
API_SECRET = os.environ.get("SENTINEL_BYBIT_SECRET", "")
BASE_URL = os.environ.get("BYBIT_BASE_URL", "https://api.bybit.com")
HEARTBEAT_TABLE = os.environ.get("HEARTBEAT_TABLE", "tekton_heartbeat")
HEARTBEAT_PK = os.environ.get("HEARTBEAT_PK", "tekton_alpha")
DEAD_THRESHOLD_SEC = 30  # Server considered dead after this TTL gap

# Symbols to check for open positions (expand as needed)
WATCH_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]


# ═══════════════════════════════════════════════════════════════
# BYBIT V5 API CLIENT (Zero Dependencies)
# ═══════════════════════════════════════════════════════════════

def _sign(param_str: str, timestamp: str, recv_window: str = "5000") -> str:
    """
    Bybit V5 HMAC-SHA256 Signature.
    Formula: HMAC_SHA256(secret, timestamp + api_key + recv_window + param_str)
    
    For GET:  param_str = query string (key1=val1&key2=val2)
    For POST: param_str = raw JSON body
    """
    raw = timestamp + API_KEY + recv_window + param_str
    return hmac.new(
        API_SECRET.encode("utf-8"),
        raw.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def bybit_request(method: str, path: str, query_params: dict = None, body: dict = None) -> dict:
    """
    Universal Bybit V5 HTTP client.
    
    Args:
        method: "GET" or "POST"
        path: API path (e.g. "/v5/position/list")
        query_params: Dict for GET requests (will be URL-encoded)
        body: Dict for POST requests (will be JSON-serialized)
    
    Returns:
        Parsed JSON response dict.
    """
    ts = str(int(time.time() * 1000))
    recv_window = "5000"

    if method == "GET" and query_params:
        # GET: Sign the query string
        query_string = urllib.parse.urlencode(query_params)
        sign_payload = query_string
        url = f"{BASE_URL}{path}?{query_string}"
        req_data = None
    elif method == "POST" and body:
        # POST: Sign the JSON body
        sign_payload = json.dumps(body)
        url = f"{BASE_URL}{path}"
        req_data = sign_payload.encode("utf-8")
    else:
        sign_payload = ""
        url = f"{BASE_URL}{path}"
        req_data = None

    signature = _sign(sign_payload, ts, recv_window)

    headers = {
        "X-BSRV-API-KEY": API_KEY,
        "X-BSRV-SIGN": signature,
        "X-BSRV-TIMESTAMP": ts,
        "X-BSRV-RECV-WINDOW": recv_window,
        "Content-Type": "application/json",
    }

    req = urllib.request.Request(url, data=req_data, headers=headers, method=method)

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            raw = resp.read().decode("utf-8")
            return json.loads(raw)
    except urllib.error.HTTPError as e:
        body_text = e.read().decode("utf-8", errors="replace") if e.fp else ""
        print(f"❌ [HTTP {e.code}] {e.reason} | Body: {body_text[:200]}")
        return {"retCode": e.code, "retMsg": e.reason, "body": body_text}
    except Exception as e:
        print(f"❌ [NET_ERROR] {e}")
        return {"retCode": -1, "retMsg": str(e)}


# ═══════════════════════════════════════════════════════════════
# DEAD MAN'S SWITCH CHECK (DynamoDB TTL)
# ═══════════════════════════════════════════════════════════════

def is_server_alive() -> bool:
    """
    Check if the heartbeat item exists in DynamoDB.
    DynamoDB auto-deletes items when TTL expires.
    No item = server has been dead for ≥ DEAD_THRESHOLD_SEC.
    """
    try:
        dynamodb = boto3.resource("dynamodb")
        table = dynamodb.Table(HEARTBEAT_TABLE)
        response = table.get_item(Key={"pk": HEARTBEAT_PK})
        
        item = response.get("Item")
        if not item:
            print(f"💀 [DMS] No heartbeat found. Server is DEAD (>{DEAD_THRESHOLD_SEC}s silence).")
            return False
        
        last_ts = float(item.get("ts", 0))
        age = time.time() - last_ts
        print(f"💓 [DMS] Heartbeat found. Age: {age:.1f}s. Server is ALIVE.")
        
        # Double-check: even if TTL hasn't kicked in, verify freshness
        if age > DEAD_THRESHOLD_SEC * 2:
            print(f"⚠️ [DMS] Heartbeat is stale ({age:.0f}s). TTL may be lagging. Treating as DEAD.")
            return False
        
        return True
        
    except Exception as e:
        # If we can't reach DynamoDB, DON'T nuke (fail-safe, not fail-deadly)
        print(f"⚠️ [DMS] DynamoDB check failed: {e}. ABORTING (fail-safe).")
        return True  # Assume alive if we can't verify


# ═══════════════════════════════════════════════════════════════
# THE EXECUTIONER — Iterative Position Nuke
# ═══════════════════════════════════════════════════════════════

def execute_nuke(symbol: str) -> dict:
    """
    Iterative Ground-Truth nuke for a single symbol.
    
    Protocol:
        1. GET position → if size == 0, done.
        2. POST market close (reduceOnly).
        3. If blocked by existing orders → Cancel All → Retry.
        4. Max 3 attempts with 1s backoff.
    
    Returns:
        {"symbol": str, "success": bool, "attempts": int, "detail": str}
    """
    print(f"\n{'='*50}")
    print(f"💀 [NUKE] Target: {symbol}")
    print(f"{'='*50}")

    for attempt in range(1, 4):
        print(f"\n--- Attempt {attempt}/3 ---")
        
        # Phase 1: Ground Truth — What does the exchange actually see?
        pos_resp = bybit_request("GET", "/v5/position/list", query_params={
            "category": "linear",
            "symbol": symbol
        })
        
        if pos_resp.get("retCode") != 0:
            print(f"⚠️ Position query failed: {pos_resp.get('retMsg')}")
            time.sleep(1)
            continue
        
        positions = pos_resp.get("result", {}).get("list", [])
        target = next((p for p in positions if abs(float(p.get("size", "0"))) > 0), None)
        
        if not target:
            return {"symbol": symbol, "success": True, "attempts": attempt, "detail": "No position found (clean)."}
        
        size = target["size"]
        pos_side = target.get("side", "").upper()
        close_side = "Sell" if pos_side == "BUY" else "Buy"
        
        print(f"📍 Found: {pos_side} {size} {symbol}. Closing with {close_side} Market order...")
        
        # Phase 2: Market Close (Reduce Only)
        close_resp = bybit_request("POST", "/v5/order/create", body={
            "category": "linear",
            "symbol": symbol,
            "side": close_side,
            "orderType": "Market",
            "qty": size,
            "reduceOnly": True,
            "timeInForce": "GTC",
            "orderLinkId": f"SENTINEL_{int(time.time())}_{attempt}"
        })
        
        ret_code = close_resp.get("retCode", -1)
        
        if ret_code == 0:
            print(f"✅ [NUKE] {symbol} CLOSED successfully on attempt {attempt}.")
            return {"symbol": symbol, "success": True, "attempts": attempt, "detail": "Market close executed."}
        
        # Phase 3: Close failed — likely blocked by existing SL/TP orders
        print(f"⚠️ Close failed (retCode={ret_code}): {close_resp.get('retMsg')}")
        print(f"🧹 Clearing ALL open orders for {symbol}...")
        
        cancel_resp = bybit_request("POST", "/v5/order/cancel-all", body={
            "category": "linear",
            "symbol": symbol
        })
        print(f"   Cancel result: retCode={cancel_resp.get('retCode')} | {cancel_resp.get('retMsg')}")
        
        # Backoff before retry (let exchange engine sync)
        time.sleep(1.0 * attempt)  # 1s, 2s, 3s
    
    return {"symbol": symbol, "success": False, "attempts": 3, "detail": "FAILED after 3 attempts."}


# ═══════════════════════════════════════════════════════════════
# LAMBDA HANDLER
# ═══════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    """
    Entry point for AWS Lambda.
    
    Trigger modes:
        1. EventBridge (scheduled): event = {} → Full autonomous check.
        2. API Gateway (manual):    event = {"symbol": "BTCUSDT"} → Force nuke.
        3. Redis Last Will:         event = {"force": true, "symbol": "SOLUSDT"}
    """
    print("="*60)
    print(f"🛡️  SENTINEL EXECUTIONER v2.0 | {time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("="*60)
    
    force_mode = event.get("force", False)
    
    # Step 1: Dead Man's Switch Check (skip if forced)
    if not force_mode:
        if is_server_alive():
            print("✅ Server is alive. Standing down.")
            return {"statusCode": 200, "body": json.dumps({"action": "STANDBY", "reason": "Server alive."})}
        
        print("💀 Server confirmed DEAD. Initiating NUCLEAR_NUKE protocol...")
    else:
        print(f"⚡ FORCE MODE activated by external trigger.")
    
    # Step 2: Scan all watched symbols for open positions
    target_symbol = event.get("symbol")
    symbols_to_check = [target_symbol] if target_symbol else WATCH_SYMBOLS
    
    results = []
    for sym in symbols_to_check:
        result = execute_nuke(sym)
        results.append(result)
        print(f"   → {sym}: {'✅ OK' if result['success'] else '❌ FAILED'} ({result['detail']})")
    
    # Step 3: Summary
    all_ok = all(r["success"] for r in results)
    status = "ALL_CLEAR" if all_ok else "PARTIAL_FAILURE"
    
    print(f"\n{'='*60}")
    print(f"📊 SENTINEL REPORT: {status}")
    for r in results:
        emoji = "✅" if r["success"] else "❌"
        print(f"   {emoji} {r['symbol']}: {r['detail']} (attempts: {r['attempts']})")
    print(f"{'='*60}")
    
    return {
        "statusCode": 200 if all_ok else 500,
        "body": json.dumps({
            "action": "NUKE_EXECUTED",
            "status": status,
            "results": results,
        })
    }
