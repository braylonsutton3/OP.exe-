OP.exe — Live Structure Engine

Strongest five inputs

These are intentionally the highest-weight parts of the hierarchy:

Break of Structure (BOS) — 24

Liquidity sweeps — 22

SMA20 / SMA50 trend structure — 22

Fair Value Gaps (FVG) — 20

Fresh market news — 18

Secondary confluence stack

The app retains objective/testable supporting context:

HH/HL vs LH/LL swing trend

CHoCH

objective order-block proxy

displacement

intraday VWAP

EMA9 / EMA21

ADX / directional movement

RSI momentum

volume expansion

09:00-09:30 ET ORB

prior-day high/low

prior-week high/low

current-session high/low

session midpoint positioning

ATR / volatility regime

confirmed swing highs/lows

Timeframes

1m

5m

15m

30m

1h

4h

1d

1w

Each timeframe is analyzed independently.

Live market data

The futures path uses:

TopstepX / ProjectX Market Hub realtime SignalR/WebSocket

GatewayTrade / GatewayQuote updates

live ProjectX bars with live: true

includePartialBar: true

websocket price overlay on the current bar

Historical bar calls are cached to respect ProjectX rate limits.

News

GDELT is used with no key.

Optional Finnhub market news can be added with FINNHUB_API_KEY.

News is a recency-weighted heuristic, not a guarantee.

Output

For every timeframe:

LONG / SHORT / WAIT

strength tier

agreement

primary-five agreement

live price / newest event source / stream age

entry

stop loss

take profit

R

projected profit/risk from ProjectX tickSize/tickValue

target and stop structural basis

full evidence table

Structural TP/SL candidates

Take-profit candidates include:

confirmed swing high/low

opposing open FVG

prior-day high/low

prior-week high/low

session high/low

ORB boundary

opposing objective order block

ATR fallback

Stop candidates include:

confirmed swing invalidation

liquidity sweep extreme

ORB invalidation

prior-day invalidation

objective order-block invalidation

ATR fallback

Render environment

Required:

TOPSTEP_USERNAME

TOPSTEP_API_KEY

Optional:

FINNHUB_API_KEY

Never commit secrets to GitHub.

Render build command

pip install -r requirements.txt

Render start command

uvicorn main:app --host 0.0.0.0 --port $PORT

Safety/design

This build is market-data and decision-support only. It does not place, modify, or cancel trades.
No indicator or combination is guaranteed; use logged out-of-sample results to calibrate it.
