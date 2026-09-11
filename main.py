from __future__ import annotations

import json
import math
import os
import time
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from signalrcore.hub_connection_builder import HubConnectionBuilder
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

APP_VERSION = "2026.09.11-final-live-structure-v1"

PROJECTX_BASE = "https://api.topstepx.com/api"
PROJECTX_MARKET_HUB = "https://rtc.topstepx.com/hubs/market"

TIMEFRAMES = {
    "1m":  {"unit": 2, "unitNumber": 1,  "days": 7,    "limit": 5000},
    "5m":  {"unit": 2, "unitNumber": 5,  "days": 30,   "limit": 5000},
    "15m": {"unit": 2, "unitNumber": 15, "days": 60,   "limit": 5000},
    "30m": {"unit": 2, "unitNumber": 30, "days": 90,   "limit": 5000},
    "1h":  {"unit": 3, "unitNumber": 1,  "days": 365,  "limit": 5000},
    "4h":  {"unit": 3, "unitNumber": 4,  "days": 730,  "limit": 5000},
    "1d":  {"unit": 4, "unitNumber": 1,  "days": 1460, "limit": 3000},
    "1w":  {"unit": 5, "unitNumber": 1,  "days": 3650, "limit": 1000},
}

PRIMARY_WEIGHTS = {
    "BOS": 24.0,
    "LIQUIDITY_SWEEP": 22.0,
    "SMA": 22.0,
    "FVG": 20.0,
    "NEWS": 18.0,
}

SECONDARY_WEIGHTS = {
    "SWING_TREND": 3.5,
    "CHOCH": 3.0,
    "ORDER_BLOCK": 3.0,
    "DISPLACEMENT": 2.5,
    "VWAP": 2.0,
    "EMA": 2.0,
    "ADX": 1.75,
    "RSI": 1.25,
    "VOLUME": 1.5,
    "ORB": 1.25,
    "PRIOR_LEVELS": 1.25,
    "SESSION_POSITION": 1.0,
    "VOLATILITY": 1.0,
}

TOKEN_CACHE = {"token": None, "issued": 0.0}
CONTRACT_CACHE = {"ts": 0.0, "contracts": []}
BAR_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
BAR_CACHE_LOCK = threading.RLock()

BAR_CACHE_TTL = {
    "1m": 10,
    "5m": 15,
    "15m": 20,
    "30m": 30,
    "1h": 45,
    "4h": 60,
    "1d": 120,
    "1w": 300,
}

NEWS_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}
NEWS_LOCK = threading.RLock()

NEWS_LOOKBACK_HOURS = {
    "1m": 2,
    "5m": 3,
    "15m": 4,
    "30m": 8,
    "1h": 12,
    "4h": 24,
    "1d": 72,
    "1w": 168,
}

app = FastAPI(title="OP.exe Final Live Structure", version=APP_VERSION)


# ------------------------------------------------------------
# ProjectX / Topstep authentication + live contract discovery
# ------------------------------------------------------------

def get_env(name: str) -> str:
    return os.getenv(name, "").strip()


def px_headers(token: str) -> dict:
    return {
        "accept": "text/plain",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}",
    }


def authenticate() -> str:
    username = get_env("TOPSTEP_USERNAME")
    api_key = get_env("TOPSTEP_API_KEY")

    if not username or not api_key:
        raise RuntimeError(
            "Missing TOPSTEP_USERNAME or TOPSTEP_API_KEY in Render Environment."
        )

    token = TOKEN_CACHE["token"]
    if token and time.time() - TOKEN_CACHE["issued"] < 23 * 3600:
        return token

    r = requests.post(
        f"{PROJECTX_BASE}/Auth/loginKey",
        headers={"accept": "text/plain", "Content-Type": "application/json"},
        json={"userName": username, "apiKey": api_key},
        timeout=15,
    )
    r.raise_for_status()
    payload = r.json()

    if not payload.get("success") or not payload.get("token"):
        raise RuntimeError(payload.get("errorMessage") or "Topstep authentication failed.")

    TOKEN_CACHE["token"] = payload["token"]
    TOKEN_CACHE["issued"] = time.time()
    return payload["token"]


def available_contracts(token: str) -> List[dict]:
    # Cache available live contracts for 5 minutes.
    if CONTRACT_CACHE["contracts"] and time.time() - CONTRACT_CACHE["ts"] < 300:
        return CONTRACT_CACHE["contracts"]

    r = requests.post(
        f"{PROJECTX_BASE}/Contract/available",
        headers=px_headers(token),
        json={"live": True},
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()
    contracts = payload.get("contracts", [])
    if not payload.get("success", True) and not contracts:
        raise RuntimeError(payload.get("errorMessage") or "Could not list live contracts.")

    CONTRACT_CACHE["contracts"] = contracts
    CONTRACT_CACHE["ts"] = time.time()
    return contracts


def normalize_symbol(symbol: str) -> str:
    return (symbol or "").strip().upper().replace(" ", "")


def pick_contract(symbol: str, contracts: List[dict]) -> Optional[dict]:
    s = normalize_symbol(symbol)

    # 1) exact active name
    exact = [c for c in contracts if str(c.get("name", "")).upper() == s and c.get("activeContract")]
    if exact:
        return exact[0]

    # 2) exact name even if active flag absent/false
    exact_any = [c for c in contracts if str(c.get("name", "")).upper() == s]
    if exact_any:
        return exact_any[0]

    # 3) active contract beginning with user text (MNQ, MES, etc.)
    active_prefix = [
        c for c in contracts
        if c.get("activeContract") and str(c.get("name", "")).upper().startswith(s)
    ]
    if active_prefix:
        return active_prefix[0]

    # 4) symbolId / description matching fallback
    fuzzy = [
        c for c in contracts
        if c.get("activeContract") and (
            s in str(c.get("symbolId", "")).upper()
            or s in str(c.get("description", "")).upper()
        )
    ]
    if fuzzy:
        return fuzzy[0]

    return None



# ------------------------------------------------------------
# ProjectX realtime Market Hub (SignalR / WebSocket)
# ------------------------------------------------------------

class RealtimeMarketHub:
    """
    Keeps one authenticated SignalR/WebSocket connection to ProjectX's
    market hub and caches the newest quote/trade for each subscribed contract.

    This is read-only market data. It does not place, modify, or cancel orders.
    """

    def __init__(self):
        self.lock = threading.RLock()
        self.connection = None
        self.connected = False
        self.connecting = False
        self.subscribed = set()
        self.quotes: Dict[str, dict] = {}
        self.trades: Dict[str, dict] = {}
        self.last_error: Optional[str] = None
        self.last_open_time: Optional[float] = None

    def _token(self):
        return authenticate()

    @staticmethod
    def _unpack_event(args):
        """
        signalrcore versions can deliver callback arguments either as
        positional values or as one list. Normalize both shapes.
        """
        vals = list(args)
        if len(vals) == 1 and isinstance(vals[0], (list, tuple)):
            vals = list(vals[0])

        contract_id = None
        data = None

        if len(vals) >= 2:
            contract_id, data = vals[0], vals[1]
        elif len(vals) == 1 and isinstance(vals[0], dict):
            data = vals[0]
            contract_id = (
                data.get("contractId")
                or data.get("contractID")
                or data.get("id")
            )

        return str(contract_id) if contract_id is not None else None, data

    def _on_quote(self, *args):
        cid, data = self._unpack_event(args)
        if not isinstance(data, dict):
            return
        with self.lock:
            if cid:
                self.quotes[cid] = {
                    **data,
                    "_received_epoch": time.time(),
                }

    def _on_trade(self, *args):
        cid, data = self._unpack_event(args)
        if not isinstance(data, dict):
            return
        with self.lock:
            if cid:
                self.trades[cid] = {
                    **data,
                    "_received_epoch": time.time(),
                }

    def _on_open(self):
        with self.lock:
            self.connected = True
            self.connecting = False
            self.last_error = None
            self.last_open_time = time.time()
            current = list(self.subscribed)

        # Re-subscribe after reconnect/open.
        for cid in current:
            try:
                self.connection.send("SubscribeContractQuotes", [cid])
                self.connection.send("SubscribeContractTrades", [cid])
            except Exception as exc:
                with self.lock:
                    self.last_error = f"resubscribe: {exc}"

    def _on_close(self):
        with self.lock:
            self.connected = False
            self.connecting = False

    def _on_error(self, data):
        with self.lock:
            self.last_error = str(getattr(data, "error", data))

    def ensure_connected(self):
        with self.lock:
            if self.connected:
                return
            if self.connecting:
                return
            self.connecting = True

        try:
            conn = (
                HubConnectionBuilder()
                .with_url(
                    PROJECTX_MARKET_HUB,
                    options={
                        "access_token_factory": self._token,
                    },
                )
                .with_automatic_reconnect({
                    "type": "raw",
                    "keep_alive_interval": 10,
                    "reconnect_interval": 3,
                    "max_attempts": 100,
                })
                .build()
            )

            conn.on("GatewayQuote", self._on_quote)
            conn.on("GatewayTrade", self._on_trade)
            conn.on_open(self._on_open)
            conn.on_close(self._on_close)
            conn.on_error(self._on_error)

            with self.lock:
                self.connection = conn

            # signalrcore's start() performs the SignalR handshake and then
            # leaves its receive loop running in the background.
            conn.start()

            with self.lock:
                # Some signalrcore builds invoke on_open immediately;
                # this fallback prevents a false "connecting forever" state.
                if not self.connected:
                    self.connected = True
                    self.connecting = False
                    self.last_open_time = time.time()

        except Exception as exc:
            with self.lock:
                self.connected = False
                self.connecting = False
                self.last_error = str(exc)
            raise

    def subscribe(self, contract_id: str):
        if not contract_id:
            return

        self.ensure_connected()

        with self.lock:
            already = contract_id in self.subscribed
            self.subscribed.add(contract_id)
            conn = self.connection

        if not already and conn is not None:
            try:
                conn.send("SubscribeContractQuotes", [contract_id])
                conn.send("SubscribeContractTrades", [contract_id])
            except Exception as exc:
                with self.lock:
                    self.last_error = f"subscribe: {exc}"

    def snapshot(self, contract_id: str) -> dict:
        with self.lock:
            q = dict(self.quotes.get(contract_id, {}))
            t = dict(self.trades.get(contract_id, {}))
            connected = bool(self.connected)
            err = self.last_error

        newest_epoch = max(
            float(q.get("_received_epoch", 0) or 0),
            float(t.get("_received_epoch", 0) or 0),
        )
        age_ms = (time.time() - newest_epoch) * 1000 if newest_epoch else None

        return {
            "connected": connected,
            "quote": q or None,
            "trade": t or None,
            "age_ms": age_ms,
            "last_error": err,
        }


REALTIME = RealtimeMarketHub()


def _stream_price(snapshot: dict) -> Tuple[Optional[float], Optional[pd.Timestamp], str]:
    """
    Prefer a trade event because it is an actual execution.
    Fall back to quote.lastPrice if no newer trade is cached.
    """
    quote = snapshot.get("quote") or {}
    trade = snapshot.get("trade") or {}

    q_epoch = float(quote.get("_received_epoch", 0) or 0)
    t_epoch = float(trade.get("_received_epoch", 0) or 0)

    source = "none"
    data = None

    if t_epoch >= q_epoch and trade.get("price") is not None:
        data = trade
        source = "GatewayTrade"
        price = trade.get("price")
        ts = trade.get("timestamp")
    elif quote.get("lastPrice") is not None:
        data = quote
        source = "GatewayQuote"
        price = quote.get("lastPrice")
        ts = quote.get("timestamp") or quote.get("lastUpdated")
    else:
        return None, None, source

    try:
        price = float(price)
    except Exception:
        return None, None, source

    parsed = pd.to_datetime(ts, utc=True, errors="coerce") if ts else pd.NaT
    return price, (None if pd.isna(parsed) else parsed), source


def overlay_realtime_price(df: pd.DataFrame, contract_id: str, tf: str) -> Tuple[pd.DataFrame, dict]:
    """
    Overlay the newest WebSocket trade/quote on top of the REST partial bar.
    REST provides the historical candle context; SignalR closes the latency gap
    between REST requests.
    """
    try:
        REALTIME.subscribe(contract_id)
    except Exception:
        # The REST live partial-bar feed still works if WebSocket startup fails.
        pass

    snap = REALTIME.snapshot(contract_id)
    price, ts, event_source = _stream_price(snap)

    if price is None:
        return df, {
            "connected": snap.get("connected", False),
            "event_source": "REST partial bar",
            "age_ms": snap.get("age_ms"),
            "last_error": snap.get("last_error"),
        }

    x = df.copy()
    last_i = x.index[-1]

    # For most requests includePartialBar=True means the active candle already
    # exists. Update that candle using the tick/quote that arrived after REST.
    x.loc[last_i, "Close"] = price
    x.loc[last_i, "High"] = max(float(x.loc[last_i, "High"]), price)
    x.loc[last_i, "Low"] = min(float(x.loc[last_i, "Low"]), price)

    return x, {
        "connected": snap.get("connected", False),
        "event_source": event_source,
        "age_ms": snap.get("age_ms"),
        "last_error": snap.get("last_error"),
        "event_timestamp": ts.isoformat() if ts is not None else None,
    }


def retrieve_bars_cached(contract_id: str, tf: str, token: str) -> pd.DataFrame:
    key = (contract_id, tf)
    ttl = BAR_CACHE_TTL[tf]

    with BAR_CACHE_LOCK:
        item = BAR_CACHE.get(key)
        if item and time.time() - item["ts"] <= ttl:
            return item["df"].copy()

    df = retrieve_bars(contract_id, tf, token)

    with BAR_CACHE_LOCK:
        BAR_CACHE[key] = {
            "ts": time.time(),
            "df": df.copy(),
        }

    return df.copy()


def retrieve_bars(contract_id: str, tf: str, token: str) -> pd.DataFrame:
    cfg = TIMEFRAMES[tf]
    end = datetime.now(timezone.utc)
    start = end - timedelta(days=cfg["days"])

    r = requests.post(
        f"{PROJECTX_BASE}/History/retrieveBars",
        headers=px_headers(token),
        json={
            "contractId": contract_id,
            "live": True,
            "startTime": start.isoformat().replace("+00:00", "Z"),
            "endTime": end.isoformat().replace("+00:00", "Z"),
            "unit": cfg["unit"],
            "unitNumber": cfg["unitNumber"],
            "limit": cfg["limit"],
            "includePartialBar": True,
        },
        timeout=20,
    )
    r.raise_for_status()
    payload = r.json()

    bars = payload.get("bars", [])
    if not bars:
        raise RuntimeError(payload.get("errorMessage") or "No live bars returned.")

    df = pd.DataFrame(bars).rename(columns={
        "t": "Date",
        "o": "Open",
        "h": "High",
        "l": "Low",
        "c": "Close",
        "v": "Volume",
    })

    df["Date"] = pd.to_datetime(df["Date"], utc=True, errors="coerce")
    df = df.dropna(subset=["Date"]).set_index("Date").sort_index()

    for c in ["Open", "High", "Low", "Close", "Volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    return df.dropna(subset=["Open", "High", "Low", "Close"])



# ------------------------------------------------------------
# Fresh market news engine
# ------------------------------------------------------------

BULLISH_WORDS = {
    "beat": 1.4, "beats": 1.4, "surge": 1.5, "surges": 1.5, "rally": 1.4,
    "rallies": 1.4, "record high": 1.4, "upgrade": 1.2, "upgrades": 1.2,
    "strong": 0.8, "growth": 0.8, "easing": 1.0, "rate cut": 1.3,
    "rate cuts": 1.3, "stimulus": 1.0, "bullish": 1.3, "outperform": 1.2,
    "higher guidance": 1.4, "raises guidance": 1.4, "dovish": 1.1,
    "cooling inflation": 1.2, "soft landing": 1.2, "buyback": 0.8,
}

BEARISH_WORDS = {
    "miss": 1.4, "misses": 1.4, "plunge": 1.5, "plunges": 1.5, "selloff": 1.5,
    "sell-off": 1.5, "downgrade": 1.2, "downgrades": 1.2, "weak": 0.8,
    "recession": 1.3, "rate hike": 1.3, "rate hikes": 1.3, "hawkish": 1.1,
    "inflation accelerates": 1.2, "hot inflation": 1.2, "war": 0.8,
    "sanctions": 0.7, "default": 1.2, "cuts guidance": 1.4,
    "lower guidance": 1.4, "bearish": 1.3, "underperform": 1.2,
}

ASSET_NEWS_QUERIES = {
    "NQ": '(Nasdaq OR "S&P 500" OR Nvidia OR Apple OR Microsoft OR Fed OR inflation OR rates)',
    "MNQ": '(Nasdaq OR "S&P 500" OR Nvidia OR Apple OR Microsoft OR Fed OR inflation OR rates)',
    "ES": '("S&P 500" OR equities OR Fed OR inflation OR rates OR earnings)',
    "MES": '("S&P 500" OR equities OR Fed OR inflation OR rates OR earnings)',
    "RTY": '("Russell 2000" OR "small caps" OR Fed OR rates OR credit)',
    "M2K": '("Russell 2000" OR "small caps" OR Fed OR rates OR credit)',
    "YM": '("Dow Jones" OR industrials OR Fed OR rates OR earnings)',
    "MYM": '("Dow Jones" OR industrials OR Fed OR rates OR earnings)',
    "GC": '(gold OR bullion OR "US dollar" OR Fed OR inflation OR yields)',
    "MGC": '(gold OR bullion OR "US dollar" OR Fed OR inflation OR yields)',
    "CL": '(oil OR crude OR OPEC OR inventories OR geopolitics)',
    "MCL": '(oil OR crude OR OPEC OR inventories OR geopolitics)',
}

def news_query_for_symbol(symbol: str) -> str:
    s = normalize_symbol(symbol)
    for root, q in ASSET_NEWS_QUERIES.items():
        if s.startswith(root):
            return q
    return f'("{s}" OR markets OR Fed OR inflation OR rates)'

def headline_sentiment(text: str) -> float:
    t = (text or "").lower()
    score = 0.0
    for phrase, weight in BULLISH_WORDS.items():
        if phrase in t:
            score += weight
    for phrase, weight in BEARISH_WORDS.items():
        if phrase in t:
            score -= weight
    return max(-4.0, min(4.0, score))

def _article_epoch(article: dict) -> float:
    for k in ("datetime", "seendate", "publishedAt", "published"):
        v = article.get(k)
        if v is None:
            continue
        try:
            if isinstance(v, (int, float)):
                return float(v)
            dt = pd.to_datetime(v, utc=True, errors="coerce")
            if pd.notna(dt):
                return dt.timestamp()
        except Exception:
            pass
    return 0.0

def fetch_finnhub_news(symbol: str, hours: int) -> List[dict]:
    key = get_env("FINNHUB_API_KEY")
    if not key:
        return []

    try:
        r = requests.get(
            "https://finnhub.io/api/v1/news",
            params={"category": "general", "token": key},
            timeout=8,
        )
        r.raise_for_status()
        items = r.json()
        if not isinstance(items, list):
            return []

        cutoff = time.time() - hours * 3600
        out = []
        for x in items:
            epoch = _article_epoch(x)
            if epoch and epoch < cutoff:
                continue
            out.append({
                "headline": x.get("headline", ""),
                "summary": x.get("summary", ""),
                "source": x.get("source", "Finnhub"),
                "url": x.get("url", ""),
                "datetime": epoch,
                "provider": "Finnhub",
            })
        return out[:60]
    except Exception:
        return []

def fetch_gdelt_news(symbol: str, hours: int) -> List[dict]:
    try:
        params = {
            "query": news_query_for_symbol(symbol),
            "mode": "ArtList",
            "format": "json",
            "maxrecords": 60,
            "sort": "HybridRel",
            "timespan": f"{max(1, int(hours))}h",
        }
        r = requests.get(
            "https://api.gdeltproject.org/api/v2/doc/doc",
            params=params,
            timeout=10,
        )
        r.raise_for_status()
        payload = r.json()
        arts = payload.get("articles", [])
        out = []
        for x in arts:
            epoch = _article_epoch(x)
            out.append({
                "headline": x.get("title", ""),
                "summary": "",
                "source": x.get("domain", "GDELT"),
                "url": x.get("url", ""),
                "datetime": epoch,
                "provider": "GDELT",
            })
        return out[:60]
    except Exception:
        return []

def news_context(symbol: str, tf: str) -> dict:
    hours = NEWS_LOOKBACK_HOURS[tf]
    key = (normalize_symbol(symbol), tf)

    with NEWS_LOCK:
        cached = NEWS_CACHE.get(key)
        if cached and time.time() - cached["ts"] < 90:
            return cached["value"]

    items = fetch_finnhub_news(symbol, hours) + fetch_gdelt_news(symbol, hours)

    # De-duplicate by normalized headline.
    seen = set()
    unique = []
    for x in items:
        h = " ".join((x.get("headline") or "").lower().split())
        if not h or h in seen:
            continue
        seen.add(h)
        unique.append(x)

    now = time.time()
    weighted = 0.0
    total_w = 0.0
    scored_items = []

    for x in unique:
        age_h = max(0.0, (now - (x.get("datetime") or now)) / 3600)
        recency = math.exp(-age_h / max(hours / 2, 1))
        text = f"{x.get('headline','')} {x.get('summary','')}"
        s = headline_sentiment(text)
        weighted += s * recency
        total_w += recency
        scored_items.append({
            **x,
            "sentiment": s,
            "age_hours": age_h,
            "recency_weight": recency,
        })

    avg = weighted / total_w if total_w else 0.0

    # Require meaningful average sentiment; otherwise keep news neutral.
    direction = 1 if avg >= 0.35 else -1 if avg <= -0.35 else 0

    scored_items.sort(
        key=lambda x: (x.get("recency_weight", 0) * abs(x.get("sentiment", 0)), -x.get("age_hours", 9999)),
        reverse=True,
    )

    value = {
        "direction": direction,
        "score": avg,
        "headline_count": len(unique),
        "lookback_hours": hours,
        "top_headlines": scored_items[:8],
        "providers": sorted(set(x.get("provider", "") for x in unique if x.get("provider"))),
    }

    with NEWS_LOCK:
        NEWS_CACHE[key] = {"ts": time.time(), "value": value}

    return value



# ------------------------------------------------------------
# Indicators
# ------------------------------------------------------------

def true_range(df: pd.DataFrame) -> pd.Series:
    prev_close = df["Close"].shift(1)
    return pd.concat([
        df["High"] - df["Low"],
        (df["High"] - prev_close).abs(),
        (df["Low"] - prev_close).abs(),
    ], axis=1).max(axis=1)


def atr(df: pd.DataFrame, n: int = 14) -> pd.Series:
    return true_range(df).ewm(alpha=1/n, adjust=False, min_periods=n).mean()


def rsi(df: pd.DataFrame, n: int = 14) -> pd.Series:
    delta = df["Close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    ag = gain.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    al = loss.ewm(alpha=1/n, adjust=False, min_periods=n).mean()
    rs = ag / al.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def adx(df: pd.DataFrame, n: int = 14):
    up = df["High"].diff()
    down = -df["Low"].diff()

    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)

    a = atr(df, n)
    pdi = 100 * plus_dm.ewm(alpha=1/n, adjust=False).mean() / a
    mdi = 100 * minus_dm.ewm(alpha=1/n, adjust=False).mean() / a
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    ax = dx.ewm(alpha=1/n, adjust=False).mean()

    return ax, pdi, mdi


def intraday_vwap(df: pd.DataFrame) -> pd.Series:
    if not isinstance(df.index, pd.DatetimeIndex):
        return pd.Series(np.nan, index=df.index)

    typical = (df["High"] + df["Low"] + df["Close"]) / 3
    vol = df["Volume"].replace(0, np.nan)
    dates = pd.Series(df.index.date, index=df.index)
    return (typical * vol).groupby(dates).cumsum() / vol.groupby(dates).cumsum()


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    x = df.copy()
    x["SMA20"] = x["Close"].rolling(20).mean()
    x["SMA50"] = x["Close"].rolling(50).mean()
    x["EMA9"] = x["Close"].ewm(span=9, adjust=False).mean()
    x["EMA21"] = x["Close"].ewm(span=21, adjust=False).mean()
    x["ATR14"] = atr(x)
    x["RSI14"] = rsi(x)
    x["ADX14"], x["PDI"], x["MDI"] = adx(x)
    x["VOL20"] = x["Volume"].rolling(20).mean()
    x["VWAP"] = intraday_vwap(x)
    return x


# ------------------------------------------------------------
# Structure
# ------------------------------------------------------------

def confirmed_swings(df: pd.DataFrame, left: int = 2, right: int = 2):
    highs, lows = [], []
    h, l = df["High"].values, df["Low"].values

    for i in range(left, len(df) - right):
        wh = h[i-left:i+right+1]
        wl = l[i-left:i+right+1]
        if h[i] == np.max(wh) and np.sum(wh == h[i]) == 1:
            highs.append((i, float(h[i])))
        if l[i] == np.min(wl) and np.sum(wl == l[i]) == 1:
            lows.append((i, float(l[i])))

    return highs, lows


def latest_before(items, idx):
    vals = [x for x in items if x[0] < idx]
    return vals[-1] if vals else None


def detect_fvgs(df: pd.DataFrame, lookback: int = 150):
    gaps = []

    for i in range(max(2, len(df) - lookback), len(df)):
        high_2 = float(df["High"].iloc[i-2])
        low_2 = float(df["Low"].iloc[i-2])
        high = float(df["High"].iloc[i])
        low = float(df["Low"].iloc[i])

        if low > high_2:
            later = df["Low"].iloc[i+1:]
            filled = bool((later <= high_2).any()) if len(later) else False
            gaps.append({
                "dir": 1,
                "low": high_2,
                "high": low,
                "i": i,
                "filled": filled,
            })

        if high < low_2:
            later = df["High"].iloc[i+1:]
            filled = bool((later >= low_2).any()) if len(later) else False
            gaps.append({
                "dir": -1,
                "low": high,
                "high": low_2,
                "i": i,
                "filled": filled,
            })

    return gaps



def _to_eastern(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        return df.copy()
    x = df.copy()
    if x.index.tz is None:
        x.index = x.index.tz_localize("UTC")
    return x.tz_convert("America/New_York")


def structural_levels(df: pd.DataFrame) -> dict:
    """
    Objective price levels used both as confluence and as TP/SL candidates.
    """
    levels = {
        "session_high": None, "session_low": None,
        "prior_day_high": None, "prior_day_low": None,
        "prior_week_high": None, "prior_week_low": None,
        "orb_high": None, "orb_low": None,
    }
    if not isinstance(df.index, pd.DatetimeIndex) or len(df) == 0:
        return levels

    try:
        x = _to_eastern(df)
        now_day = x.index[-1].date()

        # Current trading-date session high/low from available bars.
        today = x[x.index.date == now_day]
        if len(today):
            levels["session_high"] = float(today["High"].max())
            levels["session_low"] = float(today["Low"].min())

            orb_mask = (
                (today.index.time >= pd.Timestamp("09:00").time())
                & (today.index.time < pd.Timestamp("09:30").time())
            )
            orb = today.loc[orb_mask]
            if len(orb):
                levels["orb_high"] = float(orb["High"].max())
                levels["orb_low"] = float(orb["Low"].min())

        daily = x.resample("1D").agg({"High": "max", "Low": "min", "Close": "last"}).dropna()
        # Remove today's partial daily aggregation when selecting "prior day".
        prior_days = daily[daily.index.date < now_day]
        if len(prior_days):
            levels["prior_day_high"] = float(prior_days["High"].iloc[-1])
            levels["prior_day_low"] = float(prior_days["Low"].iloc[-1])

        weekly = x.resample("W-FRI").agg({"High": "max", "Low": "min", "Close": "last"}).dropna()
        current_week_end = pd.Timestamp(x.index[-1]).to_period("W-FRI").end_time.date()
        prior_weeks = weekly[weekly.index.date < current_week_end]
        if len(prior_weeks):
            levels["prior_week_high"] = float(prior_weeks["High"].iloc[-1])
            levels["prior_week_low"] = float(prior_weeks["Low"].iloc[-1])
    except Exception:
        pass

    return levels


def swing_trend_direction(df: pd.DataFrame) -> int:
    highs, lows = confirmed_swings(df)
    if len(highs) < 2 or len(lows) < 2:
        return 0
    h1, h2 = highs[-2][1], highs[-1][1]
    l1, l2 = lows[-2][1], lows[-1][1]
    if h2 > h1 and l2 > l1:
        return 1
    if h2 < h1 and l2 < l1:
        return -1
    return 0


def objective_order_block(df: pd.DataFrame) -> dict:
    """
    Conservative order-block proxy:
    - Bullish: last bearish candle immediately before a bullish displacement
      candle that closes above a recent local high.
    - Bearish: inverse.
    This is intentionally objective rather than subjective chart drawing.
    """
    if len(df) < 25:
        return {"direction": 0, "low": None, "high": None, "index": None}

    atrs = atr(df)
    start = max(3, len(df) - 80)

    for i in range(len(df) - 1, start, -1):
        a = atrs.iloc[i]
        if pd.isna(a) or a <= 0:
            continue

        o = float(df["Open"].iloc[i])
        c = float(df["Close"].iloc[i])
        h = float(df["High"].iloc[i])
        l = float(df["Low"].iloc[i])
        body = abs(c - o)

        if body < 0.75 * float(a):
            continue

        # Bullish displacement through recent highs.
        prior_high = float(df["High"].iloc[max(0, i-8):i].max())
        if c > o and c > prior_high:
            for j in range(i - 1, max(start - 1, i - 7), -1):
                jo = float(df["Open"].iloc[j]); jc = float(df["Close"].iloc[j])
                if jc < jo:
                    return {
                        "direction": 1,
                        "low": float(df["Low"].iloc[j]),
                        "high": float(df["High"].iloc[j]),
                        "index": j,
                    }

        # Bearish displacement through recent lows.
        prior_low = float(df["Low"].iloc[max(0, i-8):i].min())
        if c < o and c < prior_low:
            for j in range(i - 1, max(start - 1, i - 7), -1):
                jo = float(df["Open"].iloc[j]); jc = float(df["Close"].iloc[j])
                if jc > jo:
                    return {
                        "direction": -1,
                        "low": float(df["Low"].iloc[j]),
                        "high": float(df["High"].iloc[j]),
                        "index": j,
                    }

    return {"direction": 0, "low": None, "high": None, "index": None}


def primary_evidence(df: pd.DataFrame, news: dict):
    highs, lows = confirmed_swings(df)
    idx = len(df) - 1
    row = df.iloc[-1]

    sh = latest_before(highs, idx)
    sl = latest_before(lows, idx)

    close = float(row["Close"])
    high = float(row["High"])
    low = float(row["Low"])

    # 1) Break of Structure — highest-weight chart signal.
    bos = 0
    bos_detail = "No confirmed active break of structure"
    if sh and close > sh[1]:
        bos = 1
        bos_detail = f"Confirmed close above swing high {sh[1]:.4f}"
    elif sl and close < sl[1]:
        bos = -1
        bos_detail = f"Confirmed close below swing low {sl[1]:.4f}"

    # 2) Liquidity sweep / failed auction at confirmed swing.
    sweep = 0
    sweep_detail = "No latest-bar confirmed liquidity sweep"
    sweep_extreme = None
    if sl and low < sl[1] and close > sl[1]:
        sweep = 1
        sweep_detail = f"Sell-side liquidity swept below {sl[1]:.4f}, then reclaimed"
        sweep_extreme = low
    elif sh and high > sh[1] and close < sh[1]:
        sweep = -1
        sweep_detail = f"Buy-side liquidity swept above {sh[1]:.4f}, then rejected"
        sweep_extreme = high

    # 3) Fair Value Gap — objective 3-candle imbalance.
    fvg = 0
    gap = None
    fvg_detail = "No relevant open recent FVG"
    gaps = [g for g in detect_fvgs(df) if not g["filled"]]

    if gaps:
        atrv = float(row["ATR14"]) if pd.notna(row["ATR14"]) else max(close * 0.002, 1e-9)
        gap = min(
            gaps,
            key=lambda g: abs(close - (g["low"] + g["high"]) / 2) / max(atrv, 1e-9)
            + 0.025 * (idx - g["i"])
        )
        if gap["dir"] == 1 and close >= gap["low"]:
            fvg = 1
            fvg_detail = f"Open bullish FVG {gap['low']:.4f}-{gap['high']:.4f}"
        elif gap["dir"] == -1 and close <= gap["high"]:
            fvg = -1
            fvg_detail = f"Open bearish FVG {gap['low']:.4f}-{gap['high']:.4f}"

    # 4) SMA20 / SMA50 trend structure — promoted to primary hierarchy.
    sma = 0
    sma_detail = "SMA trend unavailable"
    if pd.notna(row["SMA20"]) and pd.notna(row["SMA50"]):
        sma20 = float(row["SMA20"])
        sma50 = float(row["SMA50"])

        sma20_prev = float(df["SMA20"].iloc[-2]) if len(df) >= 2 and pd.notna(df["SMA20"].iloc[-2]) else sma20
        sma50_prev = float(df["SMA50"].iloc[-2]) if len(df) >= 2 and pd.notna(df["SMA50"].iloc[-2]) else sma50

        bullish_stack = close > sma20 > sma50
        bearish_stack = close < sma20 < sma50
        bullish_slope = sma20 >= sma20_prev and sma50 >= sma50_prev
        bearish_slope = sma20 <= sma20_prev and sma50 <= sma50_prev

        if bullish_stack and bullish_slope:
            sma = 1
        elif bearish_stack and bearish_slope:
            sma = -1

        sma_detail = (
            f"Close={close:.4f}, SMA20={sma20:.4f}, SMA50={sma50:.4f}; "
            f"SMA20 slope={'up' if sma20 > sma20_prev else 'down' if sma20 < sma20_prev else 'flat'}, "
            f"SMA50 slope={'up' if sma50 > sma50_prev else 'down' if sma50 < sma50_prev else 'flat'}"
        )

    # 5) Market news / macro context.
    nd = int(news.get("direction", 0))
    ns = float(news.get("score", 0.0))
    nc = int(news.get("headline_count", 0))
    news_detail = (
        f"News score {ns:+.2f} from {nc} recent headlines "
        f"over {news.get('lookback_hours', '?')}h"
    )

    return {
        "BOS": (bos, bos_detail),
        "LIQUIDITY_SWEEP": (sweep, sweep_detail),
        "SMA": (sma, sma_detail),
        "FVG": (fvg, fvg_detail),
        "NEWS": (nd, news_detail),
        "_sh": sh,
        "_sl": sl,
        "_gap": gap,
        "_sweep_extreme": sweep_extreme,
    }

def secondary_evidence(df: pd.DataFrame, tf: str):
    row = df.iloc[-1]
    out = {}

    highs, lows = confirmed_swings(df)
    levels = structural_levels(df)
    ob = objective_order_block(df)

    # Higher-high/higher-low vs lower-high/lower-low structure.
    out["SWING_TREND"] = swing_trend_direction(df)

    # CHoCH / market-structure shift.
    choch = 0
    if len(highs) >= 2 and len(lows) >= 2:
        lh, ph = highs[-1], highs[-2]
        ll, pl = lows[-1], lows[-2]
        prior_bull = lh[1] > ph[1] and ll[1] > pl[1]
        prior_bear = lh[1] < ph[1] and ll[1] < pl[1]
        if prior_bull and row["Close"] < ll[1]:
            choch = -1
        elif prior_bear and row["Close"] > lh[1]:
            choch = 1
    out["CHOCH"] = choch

    # Objective order-block proxy.
    out["ORDER_BLOCK"] = int(ob.get("direction", 0))

    # Displacement / unusually directional candle.
    atrv = float(row["ATR14"]) if pd.notna(row["ATR14"]) else 0
    body = abs(float(row["Close"] - row["Open"]))
    rng = max(float(row["High"] - row["Low"]), 1e-9)
    out["DISPLACEMENT"] = (
        1 if atrv > 0 and body >= 0.8 * atrv and body / rng >= 0.60 and row["Close"] > row["Open"]
        else -1 if atrv > 0 and body >= 0.8 * atrv and body / rng >= 0.60 and row["Close"] < row["Open"]
        else 0
    )

    # VWAP is intraday context.
    out["VWAP"] = (
        0 if tf in ("1d", "1w") or pd.isna(row["VWAP"])
        else 1 if row["Close"] > row["VWAP"]
        else -1 if row["Close"] < row["VWAP"]
        else 0
    )

    # Shorter EMA alignment.
    out["EMA"] = (
        1 if row["EMA9"] > row["EMA21"] and row["Close"] > row["EMA9"]
        else -1 if row["EMA9"] < row["EMA21"] and row["Close"] < row["EMA9"]
        else 0
    )

    # ADX only contributes when directional movement is meaningful.
    av = row["ADX14"]
    out["ADX"] = (
        0 if pd.isna(av)
        else 1 if av >= 20 and row["PDI"] > row["MDI"]
        else -1 if av >= 20 and row["MDI"] > row["PDI"]
        else 0
    )

    # RSI used as momentum confirmation, not as a reversal oracle.
    rv = row["RSI14"]
    out["RSI"] = (
        0 if pd.isna(rv)
        else 1 if 52 <= rv <= 72
        else -1 if 28 <= rv <= 48
        else 0
    )

    # Volume expansion.
    vm = row["VOL20"]
    out["VOLUME"] = (
        0 if pd.isna(vm) or vm <= 0 or row["Volume"] < 1.20 * vm
        else 1 if row["Close"] > row["Open"]
        else -1 if row["Close"] < row["Open"]
        else 0
    )

    # 09:00-09:30 ET ORB direction.
    orb = 0
    if tf not in ("1d", "1w"):
        oh = levels.get("orb_high")
        ol = levels.get("orb_low")
        if oh is not None and row["Close"] > oh:
            orb = 1
        elif ol is not None and row["Close"] < ol:
            orb = -1
    out["ORB"] = orb

    # Prior day/week breakout location.
    bull = bear = 0
    for k in ("prior_day_high", "prior_week_high"):
        v = levels.get(k)
        if v is not None and row["Close"] > v:
            bull += 1
    for k in ("prior_day_low", "prior_week_low"):
        v = levels.get(k)
        if v is not None and row["Close"] < v:
            bear += 1
    out["PRIOR_LEVELS"] = 1 if bull > bear else -1 if bear > bull else 0

    # Current session position relative to its midpoint.
    sh, sl = levels.get("session_high"), levels.get("session_low")
    if sh is not None and sl is not None and sh > sl:
        mid = (sh + sl) / 2
        out["SESSION_POSITION"] = 1 if row["Close"] > mid else -1 if row["Close"] < mid else 0
    else:
        out["SESSION_POSITION"] = 0

    # Elevated ATR regime with directional location.
    vol_dir = 0
    if len(df) >= 40 and pd.notna(row["ATR14"]):
        atr_med = df["ATR14"].tail(40).median()
        if pd.notna(atr_med) and atr_med > 0 and row["ATR14"] >= 1.15 * atr_med:
            ma20 = df["Close"].tail(20).mean()
            vol_dir = 1 if row["Close"] > ma20 else -1 if row["Close"] < ma20 else 0
    out["VOLATILITY"] = vol_dir

    return out

def score(primary, secondary):
    bull = 0.0
    bear = 0.0
    evidence = []

    for name, weight in PRIMARY_WEIGHTS.items():
        direction, detail = primary[name]
        if direction > 0:
            bull += weight
        elif direction < 0:
            bear += weight
        evidence.append({
            "type": "PRIMARY",
            "name": name,
            "direction": direction,
            "weight": weight,
            "detail": detail,
        })

    for name, weight in SECONDARY_WEIGHTS.items():
        direction = int(secondary.get(name, 0))
        if direction > 0:
            bull += weight
        elif direction < 0:
            bear += weight
        evidence.append({
            "type": "SECONDARY",
            "name": name,
            "direction": direction,
            "weight": weight,
            "detail": "",
        })

    active = bull + bear
    if active <= 0:
        return {
            "signal": "WAIT",
            "tier": "MIXED",
            "agreement": 0.0,
            "primary_agreement": 0.0,
            "bull": bull,
            "bear": bear,
            "evidence": evidence,
        }

    agreement = 100 * max(bull, bear) / active

    pb = sum(PRIMARY_WEIGHTS[k] for k in PRIMARY_WEIGHTS if primary[k][0] > 0)
    pr = sum(PRIMARY_WEIGHTS[k] for k in PRIMARY_WEIGHTS if primary[k][0] < 0)
    pa = pb + pr
    primary_agreement = 100 * max(pb, pr) / pa if pa else 0.0

    net = bull - bear

    signal = (
        "LONG" if net >= 14 and agreement >= 58 and primary_agreement >= 55
        else "SHORT" if net <= -14 and agreement >= 58 and primary_agreement >= 55
        else "WAIT"
    )

    if signal != "WAIT" and agreement >= 78 and primary_agreement >= 75 and abs(net) >= 34:
        tier = "STRONG"
    elif signal != "WAIT" and agreement >= 66 and abs(net) >= 22:
        tier = "MODERATE"
    elif signal != "WAIT":
        tier = "QUALIFYING"
    else:
        tier = "MIXED"

    return {
        "signal": signal,
        "tier": tier,
        "agreement": agreement,
        "primary_agreement": primary_agreement,
        "bull": bull,
        "bear": bear,
        "evidence": evidence,
    }


# ------------------------------------------------------------
# Trade map: entry, TP, SL, R:R, projected $ P&L
# ------------------------------------------------------------

def trade_plan(df: pd.DataFrame, signal: str, primary: dict) -> Optional[dict]:
    if signal not in ("LONG", "SHORT"):
        return None

    row = df.iloc[-1]
    entry = float(row["Close"])
    atrv = float(row["ATR14"]) if pd.notna(row["ATR14"]) and row["ATR14"] > 0 else max(entry * 0.002, 1e-6)

    highs, lows = confirmed_swings(df)
    recent_highs = [p for _, p in highs[-12:]]
    recent_lows = [p for _, p in lows[-12:]]
    gaps = [g for g in detect_fvgs(df, 220) if not g["filled"]]
    levels = structural_levels(df)
    ob = objective_order_block(df)

    targets = []
    stops = []

    def add_target(price, label):
        if price is None:
            return
        p = float(price)
        if signal == "LONG" and p > entry + 0.15 * atrv:
            targets.append((p, label))
        elif signal == "SHORT" and p < entry - 0.15 * atrv:
            targets.append((p, label))

    def add_stop(price, label):
        if price is None:
            return
        p = float(price)
        if signal == "LONG" and p < entry:
            stops.append((p, label))
        elif signal == "SHORT" and p > entry:
            stops.append((p, label))

    if signal == "LONG":
        for p in recent_highs:
            add_target(p, "Confirmed swing high")
        add_target(levels.get("session_high"), "Current session high")
        add_target(levels.get("prior_day_high"), "Prior-day high")
        add_target(levels.get("prior_week_high"), "Prior-week high")
        add_target(levels.get("orb_high"), "09:00-09:30 ORB high")

        for g in gaps:
            if g["dir"] < 0:
                add_target(g["low"], "Opposing bearish FVG")

        # A bearish order block above price can act as supply/target.
        if ob.get("direction") == -1:
            add_target(ob.get("low"), "Opposing bearish order block")

        for p in recent_lows:
            if entry - p >= 0.20 * atrv:
                add_stop(p - 0.10 * atrv, "Below confirmed swing low")
        add_stop(
            levels.get("prior_day_low") - 0.10 * atrv if levels.get("prior_day_low") is not None else None,
            "Below prior-day low"
        )
        add_stop(
            levels.get("orb_low") - 0.10 * atrv if levels.get("orb_low") is not None else None,
            "Below ORB low"
        )
        if ob.get("direction") == 1:
            add_stop(
                ob.get("low") - 0.10 * atrv if ob.get("low") is not None else None,
                "Below bullish order block"
            )
        if primary.get("_sweep_extreme") is not None:
            add_stop(float(primary["_sweep_extreme"]) - 0.10 * atrv, "Below liquidity sweep extreme")

        target = min(targets, key=lambda x: x[0]) if targets else (entry + 1.5 * atrv, "1.5 ATR extension")
        stop = max(stops, key=lambda x: x[0]) if stops else (entry - 1.0 * atrv, "1 ATR invalidation")

    else:
        for p in recent_lows:
            add_target(p, "Confirmed swing low")
        add_target(levels.get("session_low"), "Current session low")
        add_target(levels.get("prior_day_low"), "Prior-day low")
        add_target(levels.get("prior_week_low"), "Prior-week low")
        add_target(levels.get("orb_low"), "09:00-09:30 ORB low")

        for g in gaps:
            if g["dir"] > 0:
                add_target(g["high"], "Opposing bullish FVG")

        if ob.get("direction") == 1:
            add_target(ob.get("high"), "Opposing bullish order block")

        for p in recent_highs:
            if p - entry >= 0.20 * atrv:
                add_stop(p + 0.10 * atrv, "Above confirmed swing high")
        add_stop(
            levels.get("prior_day_high") + 0.10 * atrv if levels.get("prior_day_high") is not None else None,
            "Above prior-day high"
        )
        add_stop(
            levels.get("orb_high") + 0.10 * atrv if levels.get("orb_high") is not None else None,
            "Above ORB high"
        )
        if ob.get("direction") == -1:
            add_stop(
                ob.get("high") + 0.10 * atrv if ob.get("high") is not None else None,
                "Above bearish order block"
            )
        if primary.get("_sweep_extreme") is not None:
            add_stop(float(primary["_sweep_extreme"]) + 0.10 * atrv, "Above liquidity sweep extreme")

        target = max(targets, key=lambda x: x[0]) if targets else (entry - 1.5 * atrv, "1.5 ATR extension")
        stop = min(stops, key=lambda x: x[0]) if stops else (entry + 1.0 * atrv, "1 ATR invalidation")

    reward = abs(float(target[0]) - entry)
    risk = abs(entry - float(stop[0]))
    rr = reward / risk if risk > 0 else None

    return {
        "entry": entry,
        "take_profit": float(target[0]),
        "stop_loss": float(stop[0]),
        "rr": rr,
        "target_basis": target[1],
        "stop_basis": stop[1],
        "atr_target_distance": reward / atrv if atrv else None,
        "atr_stop_distance": risk / atrv if atrv else None,
        "levels": levels,
        "order_block": ob,
    }

def projected_pnl(contract: dict, a: float, b: float, contracts: int) -> Optional[float]:
    tick_size = float(contract.get("tickSize") or 0)
    tick_value = float(contract.get("tickValue") or 0)

    if tick_size <= 0 or tick_value <= 0:
        return None

    ticks = abs(b - a) / tick_size
    return ticks * tick_value * contracts


# ------------------------------------------------------------
# Analysis
# ------------------------------------------------------------

def analyze(symbol: str, tf: str, contracts_count: int = 1) -> dict:
    if tf not in TIMEFRAMES:
        raise ValueError("Unsupported timeframe")

    token = authenticate()
    contracts = available_contracts(token)
    contract = pick_contract(symbol, contracts)

    if not contract:
        return {
            "timeframe": tf,
            "signal": "WAIT",
            "tier": "NO CONTRACT",
            "error": f"No live Topstep contract matched '{symbol}'.",
        }

    # Historical context comes from Topstep's live REST bars, while the
    # newest price comes from the realtime SignalR/WebSocket market hub.
    df = retrieve_bars_cached(contract["id"], tf, token)
    df, realtime_meta = overlay_realtime_price(df, contract["id"], tf)

    if len(df) < 60:
        return {
            "timeframe": tf,
            "signal": "WAIT",
            "tier": "NO DATA",
            "error": f"Only {len(df)} live bars returned.",
            "contract": contract,
        }

    df = add_indicators(df)
    news = news_context(symbol, tf)
    primary = primary_evidence(df, news)
    secondary = secondary_evidence(df, tf)
    scored = score(primary, secondary)
    plan = trade_plan(df, scored["signal"], primary)

    out = {
        "timeframe": tf,
        "signal": scored["signal"],
        "tier": scored["tier"],
        "agreement": scored["agreement"],
        "primary_agreement": scored["primary_agreement"],
        "bull_points": scored["bull"],
        "bear_points": scored["bear"],
        "evidence": scored["evidence"],
        "news": news,
        "price": float(df["Close"].iloc[-1]),
        "provider": "TopstepX / ProjectX REALTIME",
        "realtime": realtime_meta,
        "contract": {
            "id": contract.get("id"),
            "name": contract.get("name"),
            "description": contract.get("description"),
            "tickSize": contract.get("tickSize"),
            "tickValue": contract.get("tickValue"),
            "activeContract": contract.get("activeContract"),
        },
        "trade": plan,
        "structure": {
            "levels": structural_levels(df),
            "order_block": objective_order_block(df),
            "swing_trend": swing_trend_direction(df),
            "sma20": None if pd.isna(df["SMA20"].iloc[-1]) else float(df["SMA20"].iloc[-1]),
            "sma50": None if pd.isna(df["SMA50"].iloc[-1]) else float(df["SMA50"].iloc[-1]),
            "vwap": None if pd.isna(df["VWAP"].iloc[-1]) else float(df["VWAP"].iloc[-1]),
            "rsi14": None if pd.isna(df["RSI14"].iloc[-1]) else float(df["RSI14"].iloc[-1]),
            "adx14": None if pd.isna(df["ADX14"].iloc[-1]) else float(df["ADX14"].iloc[-1]),
            "atr14": None if pd.isna(df["ATR14"].iloc[-1]) else float(df["ATR14"].iloc[-1]),
        },
    }

    if plan:
        out["projected_profit"] = projected_pnl(
            contract, plan["entry"], plan["take_profit"], contracts_count
        )
        out["projected_risk"] = projected_pnl(
            contract, plan["entry"], plan["stop_loss"], contracts_count
        )

    return out


# ------------------------------------------------------------
# API + UI
# ------------------------------------------------------------

@app.get("/health")
def health():
    try:
        authenticate()
        return {
            "ok": True,
            "version": APP_VERSION,
            "topstep_auth": True,
            "market_hub_connected": REALTIME.connected,
            "market_hub_error": REALTIME.last_error,
            "subscribed_contracts": len(REALTIME.subscribed),
        }
    except Exception as exc:
        return {
            "ok": False,
            "version": APP_VERSION,
            "topstep_auth": False,
            "market_hub_connected": REALTIME.connected,
            "market_hub_error": REALTIME.last_error,
            "error": str(exc),
        }


@app.get("/api/realtime")
def api_realtime(symbol: str):
    token = authenticate()
    contracts = available_contracts(token)
    contract = pick_contract(symbol, contracts)
    if not contract:
        raise HTTPException(status_code=404, detail="Contract not found")

    try:
        REALTIME.subscribe(contract["id"])
    except Exception:
        pass

    snap = REALTIME.snapshot(contract["id"])
    return {
        "symbol": symbol,
        "contract": {
            "id": contract.get("id"),
            "name": contract.get("name"),
            "description": contract.get("description"),
        },
        "realtime": snap,
    }


@app.get("/api/news")
def api_news(symbol: str, timeframe: str = "15m"):
    if timeframe not in TIMEFRAMES:
        raise HTTPException(status_code=400, detail="Unsupported timeframe")
    return news_context(symbol, timeframe)


@app.get("/api/contracts")
def api_contracts(q: str = ""):
    token = authenticate()
    contracts = available_contracts(token)
    if q:
        uq = q.upper()
        contracts = [
            c for c in contracts
            if uq in str(c.get("name", "")).upper()
            or uq in str(c.get("description", "")).upper()
            or uq in str(c.get("symbolId", "")).upper()
        ]
    return {"contracts": contracts[:100]}


@app.get("/api/analyze")
def api_analyze(symbol: str, timeframe: str, contracts: int = 1):
    try:
        return analyze(symbol, timeframe, max(1, int(contracts)))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/api/analyze-all")
def api_analyze_all(symbol: str, contracts: int = 1):
    results = []
    for tf in TIMEFRAMES:
        try:
            results.append(analyze(symbol, tf, max(1, int(contracts))))
        except Exception as exc:
            results.append({
                "timeframe": tf,
                "signal": "WAIT",
                "tier": "ERROR",
                "error": str(exc),
            })
    return {"symbol": normalize_symbol(symbol), "results": results}


PAGE = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>OP.exe — Live Structure Engine</title>
<style>
body{font-family:Inter,system-ui,-apple-system,sans-serif;background:#0e1117;color:#f1f5f9;margin:0}
.wrap{max-width:1450px;margin:auto;padding:24px}
h1{margin:0 0 4px}.muted{color:#94a3b8}
.controls{display:flex;gap:12px;flex-wrap:wrap;margin:20px 0}
input,button{font:inherit;border-radius:9px;border:1px solid #334155;padding:10px 12px;background:#111827;color:#fff}
button{cursor:pointer;background:#1d4ed8;border-color:#1d4ed8;font-weight:700}
.status{padding:11px 13px;border-radius:9px;margin:12px 0;background:#111827;border:1px solid #263244}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}
.card{background:#111827;border:1px solid #263244;border-radius:12px;padding:14px}
.long{color:#4ade80}.short{color:#fb7185}.wait{color:#fbbf24}
table{width:100%;border-collapse:collapse;margin-top:18px;font-size:14px}
th,td{padding:10px;border-bottom:1px solid #263244;text-align:left}
th{color:#94a3b8}
details{margin:12px 0;background:#111827;border:1px solid #263244;border-radius:10px;padding:12px}
.small{font-size:12px}
</style>
</head>
<body>
<div class="wrap">
<h1>OP.exe — Live Structure Engine</h1>
<div class="muted">Realtime TopstepX/ProjectX • STRONGEST 5: BOS + Liquidity Sweeps + SMA20/50 + FVG + Market News • Full secondary structure stack retained</div>
<div id="status" class="status">Checking Topstep connection…</div>
<div class="controls">
<input id="symbol" value="MNQ" placeholder="MNQ, MES, MGC…">
<input id="contracts" type="number" value="1" min="1" max="100">
<button onclick="run()">Analyze</button>
</div>
<div id="cards" class="grid"></div>
<div id="table"></div>
<div id="detail"></div>
</div>
<script>
function cls(sig){return sig==="LONG"?"long":sig==="SHORT"?"short":"wait"}
function n(v,d=2){return(v===null||v===undefined||Number.isNaN(Number(v)))?"—":Number(v).toFixed(d)}

async function health(){
 try{
  let r=await fetch("/health");
  let d=await r.json();
  if(!d.topstep_auth){
    document.getElementById("status").innerHTML="⚠️ Topstep connection failed: "+(d.error||"unknown error");
  }else if(d.market_hub_connected){
    document.getElementById("status").innerHTML="🟢 REALTIME: TopstepX / ProjectX WebSocket market stream connected.";
  }else{
    document.getElementById("status").innerHTML="🟡 Topstep authenticated. WebSocket starts when a contract is analyzed; REST live partial bars are active meanwhile.";
  }
 }catch(e){
  document.getElementById("status").innerHTML="⚠️ Could not check Topstep connection.";
 }
}

async function run(){
 let symbol=document.getElementById("symbol").value.trim();
 let contracts=document.getElementById("contracts").value;
 let cards="",rows="",det="";

 let res=await fetch(`/api/analyze-all?symbol=${encodeURIComponent(symbol)}&contracts=${contracts}`);
 let data=await res.json();

 for(let r of data.results){
  cards+=`<div class="card">
   <div class="muted">${r.timeframe}</div>
   <div class="${cls(r.signal)}" style="font-size:26px;font-weight:800">${r.signal}</div>
   <div><b>${r.signal==="LONG"?"LONG / BUY BIAS":r.signal==="SHORT"?"SHORT / SELL BIAS":"NO TRADE / WAIT"}</b></div>
   <div>${r.tier||""}</div>
   <div class="small muted">${r.contract?.name||""}</div>
  </div>`;

  let t=r.trade||{};
  rows+=`<tr>
   <td>${r.timeframe}</td>
   <td class="${cls(r.signal)}"><b>${r.signal}</b></td>
   <td>${r.tier||""}</td>
   <td>${n(r.price,4)}</td>
   <td>${r.realtime?.event_source||"REST"}</td>
   <td>${r.realtime?.age_ms==null?"—":n(r.realtime.age_ms,0)+" ms"}</td>
   <td>${r.news?.score==null?"—":n(r.news.score,2)}</td>
   <td>${r.news?.headline_count??"—"}</td>
   <td>${n(r.agreement,0)}%</td>
   <td>${n(r.primary_agreement,0)}%</td>
   <td>${n(t.entry,4)}</td>
   <td>${n(t.take_profit,4)}</td>
   <td>${n(t.stop_loss,4)}</td>
   <td>${n(t.rr,2)}</td>
   <td>${r.projected_profit==null?"—":"$"+n(r.projected_profit,2)}</td>
   <td>${r.projected_risk==null?"—":"$"+n(r.projected_risk,2)}</td>
  </tr>`;

  let ev=(r.evidence||[]).map(e=>`<tr>
    <td>${e.type}</td><td>${e.name}</td>
    <td>${e.direction>0?"BULLISH":e.direction<0?"BEARISH":"NEUTRAL"}</td>
    <td>${e.weight}</td><td>${e.detail||""}</td>
   </tr>`).join("");

  det+=`<details>
   <summary><b>${r.timeframe} • ${r.signal} • ${r.tier||""}</b></summary>
   ${r.error?`<p>${r.error}</p>`:""}
   ${r.contract?`<p>Contract: <b>${r.contract.name}</b> — ${r.contract.description||""}<br>Data: <b>TopstepX / ProjectX REALTIME</b><br>Newest event: <b>${r.realtime?.event_source||"REST partial bar"}</b> ${r.realtime?.age_ms==null?"":"• "+n(r.realtime.age_ms,0)+" ms old when analyzed"}</p>`:""}
   ${r.trade?`<p>
    <b>TRADE PLAN</b><br>
    Entry: <b>${n(t.entry,4)}</b><br>
    Take Profit: <b>${n(t.take_profit,4)}</b> — ${t.target_basis}<br>
    Stop Loss: <b>${n(t.stop_loss,4)}</b> — ${t.stop_basis}<br>
    Reward:Risk: <b>${n(t.rr,2)}:1</b><br>
    Projected profit (${document.getElementById("contracts").value} contract(s)): <b>${r.projected_profit==null?"—":"$"+n(r.projected_profit,2)}</b><br>
    Projected risk: <b>${r.projected_risk==null?"—":"$"+n(r.projected_risk,2)}</b>
   </p>`:""}
   ${r.news?.top_headlines?.length?`<p><b>Fresh news context (${r.news.lookback_hours}h):</b><br>${r.news.top_headlines.slice(0,5).map(h=>`${h.sentiment>0?"▲":h.sentiment<0?"▼":"•"} ${h.headline} <span class="muted">(${h.source})</span>`).join("<br>")}</p>`:""}
   ${r.structure?`<p class="small"><b>Structure snapshot:</b> SMA20 ${n(r.structure.sma20,4)} • SMA50 ${n(r.structure.sma50,4)} • VWAP ${n(r.structure.vwap,4)} • RSI ${n(r.structure.rsi14,1)} • ADX ${n(r.structure.adx14,1)} • ATR ${n(r.structure.atr14,4)}</p>`:""}
   <table><tr><th>Type</th><th>Confluence</th><th>Direction</th><th>Weight</th><th>Detail</th></tr>${ev}</table>
  </details>`;
 }

 document.getElementById("cards").innerHTML=cards;
 document.getElementById("table").innerHTML=`<table>
  <tr><th>TF</th><th>Signal</th><th>Strength</th><th>Price</th><th>Newest source</th><th>Stream age</th><th>News</th><th># headlines</th><th>Agreement</th><th>Primary 5</th><th>Entry</th><th>TP</th><th>SL</th><th>R:R</th><th>Proj. profit</th><th>Proj. risk</th></tr>${rows}
 </table>`;
 document.getElementById("detail").innerHTML=det;
}

health();
run();
setInterval(()=>{health();run()},3000);
</script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def home():
    return PAGE
