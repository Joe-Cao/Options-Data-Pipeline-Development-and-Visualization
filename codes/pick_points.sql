DROP TABLE IF EXISTS selected_options;

CREATE TABLE selected_options AS
WITH
-- 目标 T 网格
T_targets(t_target) AS (
  VALUES (0.02),(0.06),(0.10),(0.20),(0.30),(0.50),(1.00)
),
-- 短久期 k 网格
k_targets_short(k_target) AS (
  VALUES (-0.15),(-0.08),(-0.04),(0.00),(0.04),(0.08),(0.15)
),
-- 长久期 k 网格
k_targets_long(k_target) AS (
  VALUES (-0.20),(-0.12),(-0.06),(-0.03),(0.00),(0.03),(0.06),(0.12),(0.20)
),

-- 1) 标准化 + 计算 mid/spread/t/k
base AS (
  SELECT
    date(quote_date) AS quote_date,
    date(expiry)     AS expiry,
    CAST(strike AS REAL) AS strike,

    -- cp = C/P
    substr(upper(COALESCE(type,'')), 1, 1) AS cp,

    CAST(last_bid_price AS REAL) AS bid_raw,
    CAST(last_ask_price AS REAL) AS ask_raw,
    CAST(last AS REAL)           AS last_raw,

    CAST(open_interest AS REAL)  AS open_interest,
    CAST(total_volume  AS REAL)  AS total_volume,

    underlying_symbol,
    CAST(underlying_close AS REAL) AS spot_raw
  FROM raw_options
),

enriched AS (
  SELECT
    quote_date, expiry, strike, cp,
    -- 让 bid/ask<=0 的当作 NULL（和你 python 里 bid>0 & ask>0 的逻辑一致）
    CASE WHEN bid_raw >= 0 THEN bid_raw END AS bid,
    CASE WHEN ask_raw >= 0 THEN ask_raw END AS ask,
    last_raw AS last,

    open_interest, total_volume,
    underlying_symbol,

    spot_raw AS spot,

    -- t: 年化到期（/365）
    (julianday(expiry) - julianday(quote_date)) / 365.0 AS t,

    -- mid：优先 bid/ask，否则用 last
    CASE
      WHEN bid_raw > 0 AND ask_raw > 0 THEN 0.5*(bid_raw + ask_raw)
      ELSE last_raw
    END AS mid,

    -- spread_abs / spread_pct
    CASE
      WHEN ask_raw IS NULL OR bid_raw IS NULL THEN NULL
      ELSE max(ask_raw - bid_raw, 0.0)
    END AS spread_abs
  FROM base
),

enriched2 AS (
  SELECT
    *,
    CASE WHEN mid > 0 AND spread_abs IS NOT NULL THEN spread_abs / mid END AS spread_pct,

    -- k = ln(K/S)
    -- 依赖 SQLite 是否启用了数学函数 ln()
    ln( max(strike, 1e-12) / max(spot, 1e-12) ) AS k
  FROM enriched
),

-- 2) 对每个 quote_date + T_target，选最接近的 expiry（t_selected）
day_exps AS (
  SELECT DISTINCT quote_date, expiry, t
  FROM enriched2
  WHERE t IS NOT NULL
),

closest_exp AS (
  SELECT
    e.quote_date,
    tt.t_target,
    e.expiry,
    e.t AS t_selected,
    row_number() OVER (
      PARTITION BY e.quote_date, tt.t_target
      ORDER BY abs(e.t - tt.t_target) ASC
    ) AS rn
  FROM day_exps e
  CROSS JOIN T_targets tt
),
tenor_choice AS (
  SELECT quote_date, t_target, expiry, t_selected
  FROM closest_exp
  WHERE rn = 1
),

-- 3) 根据 t_selected 选择短/长 k 网格
grid AS (
  SELECT
    tc.quote_date, tc.t_target, tc.expiry, tc.t_selected,
    ks.k_target
  FROM tenor_choice tc
  JOIN k_targets_short ks
    ON tc.t_selected <= 0.08

  UNION ALL

  SELECT
    tc.quote_date, tc.t_target, tc.expiry, tc.t_selected,
    kl.k_target
  FROM tenor_choice tc
  JOIN k_targets_long kl
    ON tc.t_selected > 0.08
),

-- 4) 对每个 (date, T_target, k_target) 在对应 expiry 下选 1 条最优
candidates AS (
  SELECT
    g.quote_date,
    g.expiry,
    g.t_selected,
    g.t_target,
    g.k_target,

    e.strike,
    e.cp,
    e.mid,
    e.bid,
    e.ask,
    e.spread_abs,
    e.spread_pct,
    e.open_interest,
    e.total_volume,
    e.underlying_symbol,
    e.spot,
    e.k,

    -- prefer_cp：k_target<0 选P，>0选C，=0 不偏好
    CASE
      WHEN g.k_target < 0 THEN 'P'
      ELSE 'C'
    END AS prefer_cp,

    abs(e.k - g.k_target) AS k_distance,

    -- pref_flag：有偏好就优先该边；没有偏好则全都算“满足”
    CASE
      WHEN g.k_target = 0 THEN 1
      WHEN g.k_target < 0 AND e.cp = 'P' THEN 1
      WHEN g.k_target > 0 AND e.cp = 'C' THEN 1
      ELSE 0
    END AS pref_flag
  FROM grid g
  JOIN enriched2 e
    ON e.quote_date = g.quote_date
   AND e.expiry     = g.expiry
  WHERE
    e.mid IS NOT NULL AND e.mid > 0
    AND e.k  IS NOT NULL
),

picked AS (
  SELECT
    *,
    row_number() OVER (
      PARTITION BY quote_date, t_target, k_target
      ORDER BY
        pref_flag DESC,          -- 先满足偏好（如果当天某一边缺失，会自然 fallback）
        k_distance ASC,          -- 再最接近 k_target
        spread_pct ASC,          -- 再更小点差
        total_volume DESC        -- 再更大成交量
    ) AS rn
  FROM candidates
)

SELECT
  quote_date,
  expiry,
  t_selected,
  t_target,
  strike,
  cp,
  mid,
  bid,
  ask,
  spread_abs,
  spread_pct,
  open_interest,
  total_volume,
  underlying_symbol,
  spot,
  k,
  k_target,
  k_distance AS k_distance,
  prefer_cp,

  -- liq_weight = 1/spread_pct，clip 下限0.001，上限1000
  CASE
    WHEN spread_pct IS NULL THEN 1.0
    ELSE min(1000.0, 1.0 / max(spread_pct, 0.001))
  END AS liq_weight

FROM picked
WHERE rn = 1
ORDER BY quote_date, t_target, k_target;
