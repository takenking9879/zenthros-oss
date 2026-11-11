# SPDX-License-Identifier: Apache-2.0

"""What the pipeline computes, said once for both engines.

The SQL is the same text on Polars and on Spark. The features after it are
written twice, each in its engine's own API (pipeline_polars.py,
pipeline_spark.py); SCHEMA is the table both must produce, column for
column and type for type.

A row of the result is one month in which one customer bought something:
what it bought that month, what it had bought before, and the target, what
it spent the month after (zero when it bought nothing). Months are counted
by the calendar: a customer that skipped a month has no "previous month",
whatever its previous row says.
"""

from profiles import START_YEAR

# A line's amount, after its discount.
AMOUNT = "l.quantity * l.unit_price * (1 - l.discount)"
# The categories whose share of the month's spend is a feature.
SHARE_CATEGORIES = ("electronics", "grocery", "fashion")

_SHARES = "".join(f",\n       SUM(CASE WHEN l.category = '{name}' THEN {AMOUNT} ELSE 0 END) AS spend_{name}"
                  for name in SHARE_CATEGORIES)

# customers, orders and order_lines are the registered tables. An inner join
# of the three: an order counts with its customer and its lines, completed.
MONTHLY_SQL = f"""
SELECT o.customer_id,
       c.segment,
       c.country,
       (EXTRACT(YEAR FROM o.order_date) - {START_YEAR}) * 12 + EXTRACT(MONTH FROM o.order_date) - 1 AS month_idx,
       COUNT(DISTINCT o.order_id) AS n_orders,
       COUNT(*) AS n_lines,
       SUM(l.quantity) AS units,
       SUM({AMOUNT}) AS spend{_SHARES}
FROM orders o
INNER JOIN customers c ON o.customer_id = c.customer_id
INNER JOIN order_lines l ON l.order_id = o.order_id
WHERE o.status = 'completed'
GROUP BY o.customer_id, c.segment, c.country, month_idx
"""

# monthly is the result of MONTHLY_SQL.
SUMMARY_SQL = """
SELECT segment,
       country,
       COUNT(DISTINCT customer_id) AS customers,
       SUM(n_orders) AS orders,
       SUM(spend) AS spend
FROM monthly
GROUP BY segment, country
ORDER BY segment, country
"""

KEYS = ("customer_id", "month_idx")
CATEGORICAL = ("segment", "country")
TARGET = "target_next_spend"

SCHEMA = {
    "customer_id": "int64",
    "month_idx": "int32",
    "segment": "string",
    "country": "string",
    # The month itself.
    "n_orders": "int64",
    "n_lines": "int64",
    "units": "int64",
    "spend": "float64",
    "avg_order_value": "float64",
    "avg_line_value": "float64",
    **{f"share_{name}": "float64" for name in SHARE_CATEGORIES},
    # The calendar month before, and the three months that end in this one.
    "prev_spend": "float64",
    "prev_orders": "int64",
    "spend_3m": "float64",
    "orders_3m": "int64",
    # Everything up to this month.
    "spend_cum": "float64",
    "orders_cum": "int64",
    # Months since the customer's previous purchase (-1: this is its first)
    # and since its first.
    "months_since_prev": "int32",
    "tenure_months": "int32",
    # The month of the year, as a point on a circle: December is next to January.
    "month_sin": "float64",
    "month_cos": "float64",
    TARGET: "float64",
}
OUTPUT_COLUMNS = list(SCHEMA)
NUMERIC = [name for name in SCHEMA if name not in (*KEYS, *CATEGORICAL, TARGET)]

SUMMARY_SCHEMA = {"segment": "string", "country": "string", "customers": "int64", "orders": "int64", "spend": "float64"}
SUMMARY_COLUMNS = list(SUMMARY_SCHEMA)
