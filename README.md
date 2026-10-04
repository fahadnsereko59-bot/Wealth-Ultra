# 💎 WEALTH ULTRA v18.4 — WEALTH WEAPON AUTOPILOT

**FotMob + ESPN • Own Probability Engine • Internal Fair Odds • Wealth Weapon • 1X2 + BTTS + O/U 0.5–4.5 • High-Odds Bias • Persistent Learning**

WEALTH ULTRA v18 removes the external bookmaker/odds dependency and the open-world AI research layer. Football intelligence is deliberately reduced to two live sources:

1. **FotMob — primary quantitative source**
2. **ESPN — independent secondary validation/context source**

WEALTH ULTRA then calculates its own probabilities, fair odds and virtual internal-book prices.

## Core architecture

```text
FOTMOB PRIMARY
      +
ESPN INDEPENDENT VALIDATION
      ↓
DUAL-SOURCE EVIDENCE PACKET
      ↓
10 SPECIALISTS / ORACLE / FORM / SQUAD / BTTS / CRITIC
      ↓
CALIBRATED PROBABILITY ENGINE
      ↓
OUR FAIR ODDS = 1 / PROBABILITY
      ↓
INTERNAL VIRTUAL BOOK PRICE
      ↓
EXACTLY SIX 1X2 × BTTS COMBINATIONS
      ↓
CRITIC + SENTINEL + CRITICAL AUDIT
      ↓
ONE PICK + ALWAYS-PUBLISH PRE-MATCH MARKETS
      ↓
WEALTH WEAPON / SINGLE / DOUBLE / MULTI
      ↓
RESULT → AUTOPSY → CALIBRATION → LEARNING
```

## What was removed

- The Odds API
- Sportmonks
- OpenAI live web search
- Open-world web research
- External bookmaker-price dependency
- External EV/value claims

No API key is required for ESPN in this build. FotMob's current public/community clients use the `/api/data` routes for match and detail data; match detail exposes lineups, stats, shotmap/H2H and other match intelligence when available. citeturn0search2turn0search5 ESPN's soccer site API exposes scoreboards, team data, rosters, injuries, schedules, standings and match summaries. citeturn0search4

## Own odds / own bookmaker concept

WEALTH ULTRA maintains two distinct prices:

- **Fair odds:** `1 / calibrated_probability`
- **Internal book odds:** fair odds adjusted by `INTERNAL_BOOK_MARGIN` for a virtual bookmaker quote

These prices are **model-generated**, not bookmaker market prices. Therefore the system does not claim a real-world market edge or guaranteed profit.

Example:

```text
Model probability = 62.5%
Fair odds        = 1 / 0.625 = 1.60
5% internal book = 1.52
```

The virtual bankroll remains UGX **4,500,000** by default. All staking is virtual simulation only.

## Six-combination engine

Every match evaluates exactly:

1. H + BTTS Yes
2. A + BTTS Yes
3. H + BTTS No
4. A + BTTS No
5. X + BTTS Yes
6. X + BTTS No

Joint probability:

`P(1X2) × P(BTTS)`

If lineups are unavailable, the joint probability receives the existing `0.92` uncertainty multiplier.

The highest valid joint probability remains the six-combination ONE PICK. In addition, every not-started match receives a published market selection from 1X2, BTTS, and Over/Under 0.5–4.5. Evidence quality changes the grade and marks weak selections as provisional rather than suppressing betslip generation. 90+ remains a measured evidence grade, never a forced outcome.

## Data integrity rules

- No static fixture fallback.
- No random prediction fallback.
- No synthetic xG.
- No fabricated lineups.
- No fabricated scores.
- No fabricated ESPN confirmation.
- Missing/stale critical evidence lowers confidence and adds a provisional warning; it does not suppress pre-match market publication.
- FotMob and ESPN disagreements remain visible rather than silently overwritten.
- Historical predictions are immutable until the explicit erase-all-data function is used.

## Wealth Weapon

The Wealth Weapon ranks matches by evidence grade, probability, dual-source confirmation and risk. It can generate:

- Best Single
- Best Double
- Best Multi
- ULTRA Best

Virtual staking uses probability/grade risk sizing with hard portfolio limits. It is not Kelly-on-a-bookmaker-price because no external bookmaker price is used.

## Permanent memory

SQLite stores:

- prediction history
- betslip history
- settled results
- post-match autopsies
- specialist calibration
- manager decision ledger
- simulation runs
- virtual bankroll transactions

Railway should mount `/data` as a persistent volume and use:

```text
DATABASE_PATH=/data/football_ai.db
```

## Railway deployment

1. Replace the current project files with this v18 build.
2. Push to the GitHub branch connected to Railway.
3. Remove old variables such as `ODDS_API_KEY`, `OPENAI_API_KEY`, `OPEN_WORLD_*`, and Sportmonks variables.
4. Add the variables from `.env.example` that you want to override.
5. Keep:

```text
FOTMOB_BASE=https://www.fotmob.com/api/data
STARTING_BANKROLL=4500000
DATABASE_PATH=/data/football_ai.db
ESPN_REQUIRED=true
```

6. Redeploy Railway.

## Diagnostics

After deployment test:

```text
/api/health
/api/debug/self-test
/api/debug/provider?provider=fotmob
/api/debug/provider?provider=espn
/api/ai/requirements
/api/ai/source-brain
/api/data-health
```

Then open **Match Center** and verify that an analysed match shows:

- FotMob xG
- last-5 form
- H2H
- lineups
- ESPN verification
- 1X2 probabilities
- BTTS probabilities
- fair odds
- internal book odds
- exactly six combinations
- ONE PICK or NO BET

## Important source notes

FotMob's current public/community documentation describes `/api/data/matches` and `/api/data/matchDetails`, including match details, lineups, stats, shotmap, H2H and live polling behavior. citeturn0search2turn0search5

ESPN's soccer site API documentation describes `scoreboard`, `teams`, `teams/{id}/roster`, `teams/{id}/injuries`, `teams/{id}/schedule`, `standings`, `news` and `summary?event=` resources. citeturn0search4

These are public/unofficial API references, not an official guarantee of future endpoint stability. The application therefore treats provider failures as data-quality failures rather than inventing replacements.

## Disclaimer

WEALTH ULTRA is a football decision-support and virtual-simulation system. Internal fair odds are model outputs, not bookmaker offers. High probabilities and high grades are not guarantees of winning or profit.

ESPN_HEALTH_LEAGUES=eng.1,esp.1,ita.1,ger.1,fra.1,uefa.champions,usa.1

## v18.3 market + manager changes

- Always generates pre-match Singles, Doubles, Multis and ULTRA Best from not-started fixtures when provider data is available.
- Added 1X2, BTTS and Over/Under 0.5, 1.5, 2.5, 3.5 and 4.5 markets.
- Added configurable `HIGH_ODDS_BIAS`; it changes market selection ordering, never the underlying probability.
- Added deterministic critical market audit and `/api/ai/critical-test` for probability sums, O/U complements, six-combination coverage and market-board consistency.
- Settled prediction performance remains measured separately with Brier/log loss/autopsies.

## v18.2 stability fixes

This build includes a full local runtime pass over the FastAPI application and fixes the runtime faults found in v18.1:

- Initializes the shared HTTP client/cache objects safely before startup.
- Adds the missing board concurrency lock and team/detail caches.
- Fixes the `/api/data-integrity` recursive 500 error.
- Adds `/health` as a Railway-friendly health endpoint while retaining `/api/health`.
- Fixes `/api/slips/history` and `/api/slips/memory` route ordering so they no longer get captured by `/api/slips/{slip_id}`.
- Keeps provider failures fail-safe: unavailable online evidence is labelled provisional; the system never fabricates fixtures or source data.
- Frontend JavaScript syntax was checked successfully.
- Uvicorn was started locally and `/health`, `/api/data-integrity`, `/api/slips/history`, and `/api/matches/board` all returned HTTP 200.

The container used for validation has no outbound DNS access, so live FotMob/ESPN provider connectivity cannot be claimed as tested from this environment. Provider calls are nevertheless handled as explicit online failures and do not create synthetic fixtures or predictions.
