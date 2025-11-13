SELECT
  p.product_id,
  p.name,
  SUM(oi.quantity * oi.unit_price) AS revenue,
  AVG(r.rating) AS avg_rating
FROM order_items oi
JOIN products p ON p.product_id = oi.product_id
JOIN orders o ON o.order_id = oi.order_id
LEFT JOIN reviews r ON r.product_id = p.product_id
WHERE lower(o.status) IN ('completed','delivered')
GROUP BY p.product_id
ORDER BY revenue DESC
LIMIT 10;

SELECT
  strftime('%Y-%m', o.order_date) AS month,
  COUNT(o.order_id) AS total_orders,
  SUM(oi.quantity * oi.unit_price) AS revenue
FROM orders o
JOIN order_items oi ON oi.order_id = o.order_id
WHERE lower(o.status) IN ('completed','delivered')
GROUP BY month
ORDER BY month DESC;

SELECT
  c.customer_id,
  c.first_name || ' ' || c.last_name AS name,
  SUM(oi.quantity * oi.unit_price) AS total_spent
FROM customers c
JOIN orders o ON o.customer_id = c.customer_id
JOIN order_items oi ON oi.order_id = o.order_id
WHERE lower(o.status) IN ('completed','delivered')
GROUP BY c.customer_id
ORDER BY total_spent DESC
LIMIT 10;
