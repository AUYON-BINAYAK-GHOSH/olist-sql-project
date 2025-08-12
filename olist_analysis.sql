-- ====================================================================
-- OLIST DATA ANALYSIS PIPELINE (MySQL Version)
--
-- PART 1: DATA CLEANING & PREPARATION
-- Run these queries first to clean the raw data.
-- ====================================================================

-- CLEANING STEP 1: Handle Missing Product Category Names
-- There are NULL values in the product_category_name column. We will replace them
-- with 'unknown' to ensure they are included in our analysis.
UPDATE products
SET product_category_name = 'unknown'
WHERE product_category_name IS NULL;

-- CLEANING STEP 2: Filter Out Logically Inconsistent Orders
-- Some orders have delivery dates that are before the purchase date, which is impossible.
-- We will create a view of valid order IDs to use in our analysis.
CREATE OR REPLACE VIEW valid_orders AS
SELECT order_id
FROM orders
WHERE order_purchase_timestamp < order_delivered_customer_date
   OR order_delivered_customer_date IS NULL;

-- CLEANING STEP 3: Create a Clean Geolocation View
-- The geolocation table has many duplicate zip codes with slightly different lat/lng values.
-- We create a clean, aggregated view to get a single, average coordinate for each zip code.
CREATE OR REPLACE VIEW geolocation_cleaned AS
SELECT
    geolocation_zip_code_prefix,
    AVG(geolocation_lat) AS avg_lat,
    AVG(geolocation_lng) AS avg_lng
FROM geolocation
GROUP BY geolocation_zip_code_prefix;


-- ====================================================================
-- PART 2: 10 PORTFOLIO ANALYSIS PROJECTS
-- These queries run on the cleaned and prepared data.
-- ====================================================================

-- PROJECT 1: Customer Segmentation using RFM Analysis
-- Identifies customer segments based on Recency, Frequency, and Monetary value.
WITH rfm_base AS (
    SELECT
        c.customer_unique_id,
        MAX(o.order_purchase_timestamp) AS last_purchase_date,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(p.payment_value) AS monetary
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN order_payments p ON o.order_id = p.order_id
    -- Use only delivered orders from our valid_orders view
    WHERE o.order_status = 'delivered' AND o.order_id IN (SELECT order_id FROM valid_orders)
    GROUP BY c.customer_unique_id
),
rfm_scores AS (
    SELECT
        customer_unique_id,
        DATEDIFF('2018-10-17', last_purchase_date) AS recency_days,
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY last_purchase_date DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
    FROM rfm_base
),
rfm_segments AS (
    SELECT
        *,
        CONCAT(CAST(r_score AS CHAR), CAST(f_score AS CHAR), CAST(m_score AS CHAR)) AS rfm_score,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 AND f_score >= 1 THEN 'Recent Customers'
            WHEN r_score >= 3 AND f_score <= 2 THEN 'Promising'
            WHEN r_score <= 2 AND f_score >= 3 THEN 'At-Risk Customers'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Hibernating'
            ELSE 'Other'
        END AS customer_segment
    FROM rfm_scores
)
SELECT
    customer_segment,
    COUNT(customer_unique_id) AS number_of_customers,
    AVG(recency_days) AS avg_recency,
    AVG(frequency) AS avg_frequency,
    AVG(monetary) AS avg_monetary
FROM rfm_segments
GROUP BY customer_segment
ORDER BY number_of_customers DESC;


-- PROJECT 2: Delivery Performance and Agent Efficiency Analysis
-- Analyzes delivery delays and fulfillment timeline.
WITH delivery_times AS (
    SELECT
        o.order_id,
        s.seller_state,
        c.customer_state,
        DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS delivery_delay,
        DATEDIFF(o.order_delivered_carrier_date, o.order_approved_at) AS seller_processing_time,
        DATEDIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date) AS carrier_transit_time,
        CASE WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1 ELSE 0 END AS is_on_time
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN order_items oi ON o.order_id = oi.order_id
    JOIN sellers s ON oi.seller_id = s.seller_id
    WHERE o.order_status = 'delivered'
      AND o.order_id IN (SELECT order_id FROM valid_orders)
      AND o.order_delivered_customer_date IS NOT NULL
      AND o.order_estimated_delivery_date IS NOT NULL
      AND o.order_delivered_carrier_date IS NOT NULL
      AND o.order_approved_at IS NOT NULL
)
SELECT
    seller_state,
    customer_state,
    COUNT(order_id) AS number_of_orders,
    ROUND(AVG(delivery_delay), 2) AS avg_delivery_delay_days,
    ROUND(AVG(seller_processing_time), 2) AS avg_seller_processing_days,
    ROUND(AVG(carrier_transit_time), 2) AS avg_carrier_transit_days,
    ROUND(AVG(is_on_time) * 100, 2) AS on_time_delivery_rate
FROM delivery_times
GROUP BY seller_state, customer_state
HAVING COUNT(order_id) > 100
ORDER BY avg_delivery_delay_days DESC
LIMIT 10;


-- PROJECT 3: Product Catalog Optimization
-- Identifies the top 10 best and worst-performing product categories.
WITH category_performance AS (
    SELECT
        pct.product_category_name_english AS category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price) AS total_revenue,
        AVG(r.review_score) AS avg_review_score,
        SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT o.order_id) AS cancellation_rate
    FROM order_items oi
    JOIN products p ON oi.product_id = p.product_id
    JOIN orders o ON oi.order_id = o.order_id
    JOIN order_reviews r ON o.order_id = r.order_id
    JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
    WHERE o.order_id IN (SELECT order_id FROM valid_orders)
    GROUP BY pct.product_category_name_english
    HAVING COUNT(DISTINCT o.order_id) > 50
)
(SELECT category, total_orders, ROUND(total_revenue, 2) AS total_revenue, ROUND(avg_review_score, 2) AS avg_review_score, ROUND(cancellation_rate, 4) AS cancellation_rate, 'Best Performing' AS performance_type FROM category_performance ORDER BY total_revenue DESC, avg_review_score DESC LIMIT 10)
UNION ALL
(SELECT category, total_orders, ROUND(total_revenue, 2) AS total_revenue, ROUND(avg_review_score, 2) AS avg_review_score, ROUND(cancellation_rate, 4) AS cancellation_rate, 'Worst Performing' AS performance_type FROM category_performance ORDER BY total_revenue ASC, avg_review_score ASC LIMIT 10);


-- PROJECT 4: Fake Review & Spam Detection System
-- Identifies users who may be leaving fake positive reviews.
SELECT
    c.customer_unique_id,
    COUNT(r.review_id) AS num_five_star_reviews,
    AVG(LENGTH(r.review_comment_message)) AS avg_comment_length
FROM order_reviews r
JOIN orders o ON r.order_id = o.order_id
JOIN customers c ON o.customer_id = c.customer_id
WHERE r.review_score = 5
  AND r.review_comment_message IS NOT NULL
  AND o.order_id IN (SELECT order_id FROM valid_orders)
GROUP BY c.customer_unique_id
HAVING COUNT(r.review_id) > 2 AND AVG(LENGTH(r.review_comment_message)) < 20
ORDER BY num_five_star_reviews DESC, avg_comment_length ASC
LIMIT 10;


-- PROJECT 5: Campaign ROI Tracker (Simulated)
-- Compares sales performance during a hypothetical campaign period against a baseline.
WITH monthly_sales AS (
    SELECT
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS sales_month,
        SUM(p.payment_value) AS total_revenue
    FROM orders o
    JOIN order_payments p ON o.order_id = p.order_id
    WHERE o.order_status = 'delivered' AND o.order_id IN (SELECT order_id FROM valid_orders)
    GROUP BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')
),
campaign_analysis AS (
    SELECT
        sales_month,
        total_revenue,
        LAG(total_revenue, 1, 0) OVER (ORDER BY sales_month) AS previous_month_revenue
    FROM monthly_sales
)
SELECT
    sales_month,
    total_revenue,
    previous_month_revenue,
    (total_revenue - previous_month_revenue) AS revenue_lift,
    (total_revenue - previous_month_revenue) * 100.0 / previous_month_revenue AS percentage_lift
FROM campaign_analysis
WHERE sales_month = '2017-11';


-- PROJECT 6: Funnel Drop-off Breakdown (Simulated)
-- Simulates a user journey funnel analysis using order statuses.
SELECT '1. Created' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_id IN (SELECT order_id FROM valid_orders)
UNION ALL
SELECT '2. Approved' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_approved_at IS NOT NULL AND order_id IN (SELECT order_id FROM valid_orders)
UNION ALL
SELECT '3. Shipped' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_delivered_carrier_date IS NOT NULL AND order_id IN (SELECT order_id FROM valid_orders)
UNION ALL
SELECT '4. Delivered' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_status = 'delivered' AND order_id IN (SELECT order_id FROM valid_orders)
ORDER BY funnel_stage;


-- PROJECT 7: Dynamic Product Bundling Engine
-- Identifies pairs of products that are frequently purchased together.
WITH product_pairs AS (
    SELECT
        a.product_id AS product_a,
        b.product_id AS product_b,
        a.order_id
    FROM order_items a
    JOIN order_items b ON a.order_id = b.order_id AND a.product_id < b.product_id
    WHERE a.order_id IN (SELECT order_id FROM valid_orders)
)
SELECT
    pct_a.product_category_name_english AS category_a,
    pct_b.product_category_name_english AS category_b,
    COUNT(*) AS pair_frequency
FROM product_pairs pp
JOIN products p_a ON pp.product_a = p_a.product_id
JOIN products p_b ON pp.product_b = p_b.product_id
JOIN product_category_name_translation pct_a ON p_a.product_category_name = pct_a.product_category_name
JOIN product_category_name_translation pct_b ON p_b.product_category_name = pct_b.product_category_name
GROUP BY category_a, category_b
ORDER BY pair_frequency DESC
LIMIT 10;


-- PROJECT 8: Product Lifecycle Analysis
-- Tracks weekly sales for the top 5 product categories to identify trends.
WITH weekly_sales AS (
    SELECT
        pct.product_category_name_english AS category,
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%U') AS sales_week,
        SUM(oi.price) AS weekly_revenue
    FROM order_items oi
    JOIN orders o ON oi.order_id = o.order_id
    JOIN products p ON oi.product_id = p.product_id
    JOIN product_category_name_translation pct ON p.product_category_name = pct.product_category_name
    WHERE o.order_status = 'delivered' AND o.order_id IN (SELECT order_id FROM valid_orders)
      AND pct.product_category_name_english IN (
          SELECT category FROM (
              SELECT pct_sub.product_category_name_english AS category, SUM(oi_sub.price) AS total_revenue
              FROM order_items oi_sub
              JOIN products p_sub ON oi_sub.product_id = p_sub.product_id
              JOIN product_category_name_translation pct_sub ON p_sub.product_category_name = pct_sub.product_category_name
              GROUP BY category ORDER BY total_revenue DESC LIMIT 5
          ) AS top_categories
      )
    GROUP BY category, sales_week
)
SELECT
    sales_week,
    category,
    weekly_revenue,
    AVG(weekly_revenue) OVER (PARTITION BY category ORDER BY sales_week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS four_week_moving_avg
FROM weekly_sales
ORDER BY category, sales_week;


-- PROJECT 9: Intelligent Surge Pricing Engine (Geospatial Analysis)
-- Identifies "hotspot" zones by analyzing order density using the cleaned geolocation data.
SELECT
    c.customer_zip_code_prefix,
    COUNT(o.order_id) AS number_of_orders,
    AVG(p.payment_value) AS avg_order_value,
    g.avg_lat,
    g.avg_lng
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_payments p ON o.order_id = p.order_id
-- Use the cleaned geolocation view
JOIN geolocation_cleaned g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix
WHERE o.order_status = 'delivered' AND o.order_id IN (SELECT order_id FROM valid_orders)
GROUP BY c.customer_zip_code_prefix, g.avg_lat, g.avg_lng
HAVING COUNT(o.order_id) > 50
ORDER BY number_of_orders DESC
LIMIT 20;


-- PROJECT 10: Payment Method Analysis
-- Analyzes the usage and performance of different payment methods.
SELECT
    p.payment_type,
    COUNT(DISTINCT p.order_id) AS number_of_orders,
    SUM(p.payment_value) AS total_payment_value,
    AVG(p.payment_value) AS average_payment_value,
    AVG(CASE WHEN p.payment_type = 'credit_card' THEN p.payment_installments ELSE NULL END) AS average_installments,
    AVG(TIMESTAMPDIFF(HOUR, o.order_purchase_timestamp, o.order_approved_at)) AS avg_approval_time_hours
FROM order_payments p
JOIN orders o ON p.order_id = o.order_id
WHERE o.order_status NOT IN ('unavailable', 'canceled') AND o.order_id IN (SELECT order_id FROM valid_orders)
GROUP BY p.payment_type
ORDER BY number_of_orders DESC;