# The equal-answer workload (every engine answers these identically on orders.csv)

Q1  OLAP aggregate (top products by revenue):
    SELECT product_id, SUM(amount_cents) AS s, COUNT(*) AS c
    FROM orders GROUP BY product_id ORDER BY s DESC, product_id ASC LIMIT 10;

Q2  Point lookup:
    SELECT amount_cents FROM orders WHERE order_id = 424242;

Q3  Filtered count (scan):
    SELECT COUNT(*) FROM orders WHERE amount_cents > 500000;

ANSWER_HASH = sha256( Q1_ten_rows "|" Q2_value "|" Q3_value )
A lane is counted "equal-answer" ONLY if its ANSWER_HASH matches the reference (sqlite). A mismatch
is reported as MISMATCH and NEVER counted as a win -- honest by construction.
