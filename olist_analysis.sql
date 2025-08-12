-- =================================================================================================
--
-- OLIST E-COMMERCE DATA ANALYSIS PIPELINE
-- Author: [Your Name]
-- Date: [Current Date]
-- Description: An end-to-end SQL script that first cleans the Olist dataset and then
--              conducts 10 distinct business intelligence analyses in a MySQL environment.
--
-- =================================================================================================


-- =================================================================================================
-- PART 1: DATA CLEANING & PREPARATION
-- Objective: To create a reliable and clean base for analysis by addressing common data quality issues.
-- =================================================================================================

-- CLEANING STEP 1: Impute Missing Product Category Names
-- Problem: Product records with NULL category names can be unintentionally excluded from analysis.
-- Solution: Replace NULLs with the string 'unknown' to ensure all products are accounted for.
UPDATE
    products
SET
    product_category_name = 'unknown'
WHERE
    product_category_name IS NULL;


-- CLEANING STEP 2: Create a View for Logically Valid Orders
-- Problem: Some orders have impossible timestamps (e.g., delivered before purchase).
-- Solution: Create a reusable VIEW containing only the IDs of orders with a valid timeline.
--           This ensures all downstream analysis is performed on credible data.
CREATE OR REPLACE VIEW valid_orders AS
SELECT
    order_id
FROM
    orders
WHERE
    order_purchase_timestamp < order_delivered_customer_date
    OR order_delivered_customer_date IS NULL; -- (Includes orders not yet delivered)


-- CLEANING STEP 3: Create an Aggregated View for Geolocation Data
-- Problem: The raw geolocation table contains multiple latitude/longitude entries for a single zip code.
-- Solution: Create a VIEW that provides a single, averaged coordinate for each zip code prefix,
--           preventing skewed results in geospatial analysis.
CREATE OR REPLACE VIEW geolocation_cleaned AS
SELECT
    geolocation_zip_code_prefix,
    AVG(geolocation_lat) AS avg_lat,
    AVG(geolocation_lng) AS avg_lng
FROM
    geolocation
GROUP BY
    geolocation_zip_code_prefix;


-- =================================================================================================
-- PART 2: PORTFOLIO ANALYSIS PROJECTS
-- Objective: To derive actionable business insights from the cleaned dataset.
-- =================================================================================================

-- PROJECT 1: Customer Segmentation using RFM Analysis
-- Goal: Segment customers based on their purchasing behavior (Recency, Frequency, Monetary value).
WITH rfm_base AS (
    -- Step 1: Calculate raw Recency, Frequency, and Monetary values for each customer.
    SELECT
        c.customer_unique_id,
        MAX(o.order_purchase_timestamp) AS last_purchase_date,
        COUNT(DISTINCT o.order_id) AS frequency,
        SUM(p.payment_value) AS monetary
    FROM
        orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
        JOIN order_payments AS p ON o.order_id = p.order_id
    WHERE
        o.order_status = 'delivered'
        AND o.order_id IN (SELECT order_id FROM valid_orders)
    GROUP BY
        c.customer_unique_id
),
rfm_scores AS (
    -- Step 2: Score each customer on a scale of 1-5 for R, F, and M.
    SELECT
        customer_unique_id,
        DATEDIFF('2018-10-17', last_purchase_date) AS recency_days, -- Using a fixed date for consistent recency calculation
        frequency,
        monetary,
        NTILE(5) OVER (ORDER BY last_purchase_date DESC) AS r_score,
        NTILE(5) OVER (ORDER BY frequency DESC) AS f_score,
        NTILE(5) OVER (ORDER BY monetary DESC) AS m_score
    FROM
        rfm_base
),
rfm_segments AS (
    -- Step 3: Combine scores and assign a descriptive segment name.
    SELECT
        *,
        CONCAT(CAST(r_score AS CHAR), CAST(f_score AS CHAR), CAST(m_score AS CHAR)) AS rfm_score_string,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
            WHEN r_score >= 4 THEN 'Recent Customers'
            WHEN f_score >= 4 THEN 'High Frequency Shoppers'
            WHEN m_score >= 4 THEN 'High Value Shoppers'
            WHEN r_score <= 2 AND f_score <= 2 THEN 'Hibernating'
            WHEN r_score <= 2 AND f_score >= 3 THEN 'At-Risk Customers'
            ELSE 'Standard'
        END AS customer_segment
    FROM
        rfm_scores
)
-- Final Step: Aggregate the results to see the size and characteristics of each segment.
SELECT
    customer_segment,
    COUNT(customer_unique_id) AS number_of_customers,
    ROUND(AVG(recency_days), 0) AS avg_recency_days,
    ROUND(AVG(frequency), 2) AS avg_frequency,
    ROUND(AVG(monetary), 2) AS avg_monetary_value
FROM
    rfm_segments
GROUP BY
    customer_segment
ORDER BY
    number_of_customers DESC;


-- PROJECT 2: Delivery Performance Analysis
-- Goal: Analyze order fulfillment times, focusing on delays and on-time delivery rates between states.
WITH delivery_times AS (
    SELECT
        o.order_id,
        s.seller_state,
        c.customer_state,
        DATEDIFF(o.order_delivered_customer_date, o.order_estimated_delivery_date) AS delivery_delay_days,
        DATEDIFF(o.order_delivered_carrier_date, o.order_approved_at) AS seller_processing_days,
        DATEDIFF(o.order_delivered_customer_date, o.order_delivered_carrier_date) AS carrier_transit_days,
        CASE
            WHEN o.order_delivered_customer_date <= o.order_estimated_delivery_date THEN 1
            ELSE 0
        END AS is_on_time
    FROM
        orders AS o
        JOIN customers AS c ON o.customer_id = c.customer_id
        JOIN order_items AS oi ON o.order_id = oi.order_id
        JOIN sellers AS s ON oi.seller_id = s.seller_id
    WHERE
        o.order_status = 'delivered'
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
    ROUND(AVG(delivery_delay_days), 2) AS avg_delivery_delay,
    ROUND(AVG(seller_processing_days), 2) AS avg_seller_processing_time,
    ROUND(AVG(carrier_transit_days), 2) AS avg_carrier_transit_time,
    ROUND(AVG(is_on_time) * 100, 2) AS on_time_delivery_rate_pct
FROM
    delivery_times
GROUP BY
    seller_state,
    customer_state
HAVING
    COUNT(order_id) > 100 -- Focus on routes with significant volume
ORDER BY
    avg_delivery_delay DESC
LIMIT 10;


-- PROJECT 3: Product Catalog Optimization
-- Goal: Identify the best and worst-performing product categories based on revenue, order volume, and reviews.
WITH category_performance AS (
    SELECT
        pct.product_category_name_english AS category,
        COUNT(DISTINCT o.order_id) AS total_orders,
        SUM(oi.price) AS total_revenue,
        AVG(r.review_score) AS avg_review_score,
        SUM(CASE WHEN o.order_status = 'canceled' THEN 1 ELSE 0 END) * 1.0 / COUNT(DISTINCT o.order_id) AS cancellation_rate
    FROM
        order_items AS oi
        JOIN products AS p ON oi.product_id = p.product_id
        JOIN orders AS o ON oi.order_id = o.order_id
        JOIN order_reviews AS r ON o.order_id = r.order_id
        JOIN product_category_name_translation AS pct ON p.product_category_name = pct.product_category_name
    WHERE
        o.order_id IN (SELECT order_id FROM valid_orders)
    GROUP BY
        pct.product_category_name_english
    HAVING
        COUNT(DISTINCT o.order_id) > 50 -- Analyze categories with a meaningful number of orders
)
-- Combine top and bottom performers into a single report
(SELECT category, total_orders, total_revenue, avg_review_score, cancellation_rate, 'Best Performing' AS performance_type FROM category_performance ORDER BY total_revenue DESC, avg_review_score DESC LIMIT 10)
UNION ALL
(SELECT category, total_orders, total_revenue, avg_review_score, cancellation_rate, 'Worst Performing' AS performance_type FROM category_performance ORDER BY total_revenue ASC, avg_review_score ASC LIMIT 10);


-- PROJECT 4: Potential Fake Review Detection
-- Goal: Identify users who may be leaving low-effort, potentially spammy 5-star reviews.
SELECT
    c.customer_unique_id,
    COUNT(r.review_id) AS num_five_star_reviews,
    AVG(LENGTH(r.review_comment_message)) AS avg_comment_length
FROM
    order_reviews AS r
    JOIN orders AS o ON r.order_id = o.order_id
    JOIN customers AS c ON o.customer_id = c.customer_id
WHERE
    r.review_score = 5
    AND r.review_comment_message IS NOT NULL
    AND o.order_id IN (SELECT order_id FROM valid_orders)
GROUP BY
    c.customer_unique_id
HAVING
    COUNT(r.review_id) > 2 -- Users who have left multiple 5-star reviews
    AND AVG(LENGTH(r.review_comment_message)) < 20 -- With very short average comment length
ORDER BY
    num_five_star_reviews DESC,
    avg_comment_length ASC
LIMIT 10;


-- PROJECT 5: Campaign ROI Tracker (Simulated)
-- Goal: Measure the revenue lift during a simulated campaign month (Nov 2017) compared to the prior month.
WITH monthly_sales AS (
    SELECT
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS sales_month,
        SUM(p.payment_value) AS total_revenue
    FROM
        orders AS o
        JOIN order_payments AS p ON o.order_id = p.order_id
    WHERE
        o.order_status = 'delivered'
        AND o.order_id IN (SELECT order_id FROM valid_orders)
    GROUP BY
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')
),
campaign_analysis AS (
    SELECT
        sales_month,
        total_revenue,
        LAG(total_revenue, 1, 0) OVER (ORDER BY sales_month) AS previous_month_revenue
    FROM
        monthly_sales
)
SELECT
    sales_month,
    total_revenue,
    previous_month_revenue,
    (total_revenue - previous_month_revenue) AS revenue_lift,
    (total_revenue - previous_month_revenue) * 100.0 / previous_month_revenue AS percentage_lift
FROM
    campaign_analysis
WHERE
    sales_month = '2017-11'; -- Focus on the campaign month


-- PROJECT 6: Sales Funnel Drop-off Analysis
-- Goal: Simulate a user journey funnel to identify where customers drop off in the purchasing process.
SELECT '1. Created Order' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_id IN (SELECT order_id FROM valid_orders)
UNION ALL
SELECT '2. Payment Approved' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_approved_at IS NOT NULL AND order_id IN (SELECT order_id FROM valid_orders)
UNION ALL
SELECT '3. Shipped to Carrier' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_delivered_carrier_date IS NOT NULL AND order_id IN (SELECT order_id FROM valid_orders)
UNION ALL
SELECT '4. Delivered to Customer' AS funnel_stage, COUNT(DISTINCT order_id) AS num_orders FROM orders WHERE order_status = 'delivered' AND order_id IN (SELECT order_id FROM valid_orders)
ORDER BY
    funnel_stage;


-- PROJECT 7: Market Basket Analysis (Product Bundling)
-- Goal: Identify pairs of product categories that are frequently purchased together in the same order.
WITH product_pairs AS (
    -- Self-join to create pairs of products within the same order
    SELECT
        a.product_id AS product_a,
        b.product_id AS product_b,
        a.order_id
    FROM
        order_items AS a
        JOIN order_items AS b ON a.order_id = b.order_id AND a.product_id < b.product_id -- Ensures unique pairs
    WHERE
        a.order_id IN (SELECT order_id FROM valid_orders)
)
SELECT
    pct_a.product_category_name_english AS category_a,
    pct_b.product_category_name_english AS category_b,
    COUNT(*) AS pair_frequency
FROM
    product_pairs AS pp
    JOIN products AS p_a ON pp.product_a = p_a.product_id
    JOIN products AS p_b ON pp.product_b = p_b.product_id
    JOIN product_category_name_translation AS pct_a ON p_a.product_category_name = pct_a.product_category_name
    JOIN product_category_name_translation AS pct_b ON p_b.product_category_name = pct_b.product_category_name
GROUP BY
    category_a,
    category_b
ORDER BY
    pair_frequency DESC
LIMIT 10;


-- PROJECT 8: Product Lifecycle Analysis
-- Goal: Track weekly sales for the top 5 product categories to identify growth, maturity, or decline phases.
WITH weekly_sales AS (
    SELECT
        pct.product_category_name_english AS category,
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%U') AS sales_week, -- Group by year and week number
        SUM(oi.price) AS weekly_revenue
    FROM
        order_items AS oi
        JOIN orders AS o ON oi.order_id = o.order_id
        JOIN products AS p ON oi.product_id = p.product_id
        JOIN product_category_name_translation AS pct ON p.product_category_name = pct.product_category_name
    WHERE
        o.order_status = 'delivered'
        AND o.order_id IN (SELECT order_id FROM valid_orders)
        AND pct.product_category_name_english IN (
            -- Subquery to dynamically find the top 5 categories by total revenue
            SELECT category FROM (
                SELECT
                    pct_sub.product_category_name_english AS category,
                    SUM(oi_sub.price) AS total_revenue
                FROM order_items AS oi_sub
                JOIN products AS p_sub ON oi_sub.product_id = p_sub.product_id
                JOIN product_category_name_translation AS pct_sub ON p_sub.product_category_name = pct_sub.product_category_name
                GROUP BY category
                ORDER BY total_revenue DESC
                LIMIT 5
            ) AS top_categories
        )
    GROUP BY
        category,
        sales_week
)
SELECT
    sales_week,
    category,
    weekly_revenue,
    -- Calculate a 4-week moving average to smooth out weekly fluctuations
    AVG(weekly_revenue) OVER (PARTITION BY category ORDER BY sales_week ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS four_week_moving_avg
FROM
    weekly_sales
ORDER BY
    category,
    sales_week;


-- PROJECT 9: Geospatial Order Analysis (Customer Hotspots)
-- Goal: Identify geographic "hotspots" by analyzing order density and value by zip code.
SELECT
    c.customer_zip_code_prefix,
    COUNT(o.order_id) AS number_of_orders,
    AVG(p.payment_value) AS avg_order_value,
    g.avg_lat,
    g.avg_lng
FROM
    orders AS o
    JOIN customers AS c ON o.customer_id = c.customer_id
    JOIN order_payments AS p ON o.order_id = p.order_id
    JOIN geolocation_cleaned AS g ON c.customer_zip_code_prefix = g.geolocation_zip_code_prefix -- Using our clean view
WHERE
    o.order_status = 'delivered'
    AND o.order_id IN (SELECT order_id FROM valid_orders)
GROUP BY
    c.customer_zip_code_prefix,
    g.avg_lat,
    g.avg_lng
HAVING
    COUNT(o.order_id) > 50 -- Focus on zip codes with high order volume
ORDER BY
    number_of_orders DESC
LIMIT 20;


-- PROJECT 10: Payment Method Analysis
-- Goal: Analyze the usage, value, and performance of different payment methods.
SELECT
    p.payment_type,
    COUNT(DISTINCT p.order_id) AS number_of_orders,
    SUM(p.payment_value) AS total_payment_value,
    AVG(p.payment_value) AS average_payment_value,
    AVG(p.payment_installments) AS average_installments,
    AVG(TIMESTAMPDIFF(HOUR, o.order_purchase_timestamp, o.order_approved_at)) AS avg_approval_time_hours
FROM
    order_payments AS p
    JOIN orders AS o ON p.order_id = o.order_id
WHERE
    o.order_status NOT IN ('unavailable', 'canceled')
    AND o.order_id IN (SELECT order_id FROM valid_orders)
GROUP BY
    p.payment_type
ORDER BY
    number_of_orders DESC;

