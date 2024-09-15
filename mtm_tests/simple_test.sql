

\set QUIET on
\pset footer off

SET mtm.output_format='%s -> %s';
SET client_min_messages = 'ERROR';

BEGIN;

\echo 'Сhecking a monotonic data:'

WITH letter_values AS (
  VALUES
    ('A'),
    ('B'),
    ('C'),
    ('Q') -- Add more letters if needed
    ),
num AS (
  SELECT
    lv.column1 as text,
    CASE WHEN random() < 0.8 THEN random() * 10000 ELSE NULL END AS val
  FROM
    generate_series(0, 1000) gs CROSS
    JOIN letter_values lv
)
select
  *,
  CASE WHEN main = target then 'Ok' ELSE 'False' END as check
from
  (
    select
      text,
      max(val::numeric):: text || ' -> ' || min(val::numeric):: text as main,
      max_to_min(val::numeric) as target
    from
      num
    group by
      text
  ) t;

\echo 'Checking data with NULL:'

SELECT
  CASE WHEN max(val::numeric) || ' -> ' || min(val::numeric) = max_to_min(val::numeric) THEN 'Ok' ELSE 'False' END as check
FROM
  (
    VALUES
      (NULL),
      (6),
      (7),
      (9),
      (10),
      (7),
      (NULL)
  ) t(val);

\echo 'Checking full NULL data:'

CREATE TEMPORARY TABLE test_null_mtm (
  text char(1),
  val numeric
);
INSERT INTO test_null_mtm
VALUES
  ('A', NULL),
  ('A', NULL),
  ('A', NULL);
INSERT INTO test_null_mtm
VALUES
  ('B', NULL),
  ('B', NULL),
  ('B', NULL);
SELECT
  text,
  max_to_min(val::numeric),
  CASE WHEN max_to_min(val::numeric) IS NULL THEN 'Ok' ELSE 'False' END AS check
FROM
  test_null_mtm
GROUP BY
  text;
COMMIT;

\quit
