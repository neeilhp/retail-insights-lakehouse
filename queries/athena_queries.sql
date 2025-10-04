/* Query 1: Bulk Order Impact
Situation: Retail Company wants to understand how bulk orders affect revenue.
Task: Quantify total revenue from bulk orders.
Action: 
*/
SELECT
  bulk_order,
  COUNT(*) AS num_orders,
  SUM(amount) AS total_revenue
FROM retail_analytics_db.sales_transformed
GROUP BY bulk_order;
--Result: Bulk orders contributed ₹X across Y transactions — a key segment for upselling.

/*Query 2 - Dicount Effectiveness
Situation: Marketing ran a discount campaign 
Situation: Compare average spend with and without discounts
Action:
*/
SELECT
  CASE 
    WHEN discounted_amount < amount THEN 'Discounted' 
    ELSE 'Full Price' 
  END AS pricing_type,
  COUNT(*) AS num_sales,
  AVG(amount) AS avg_original,
  AVG(discounted_amount) AS avg_final
FROM retail_analytics_db.sales_transformed
GROUP BY 
  CASE 
    WHEN discounted_amount < amount THEN 'Discounted' 
    ELSE 'Full Price' 
  END;

--Result:Discounted purchases averaged ₹X less, but drove Y% more volume.

/*Query-3 Store Level Performance
Situation: Regional Manager wants Store-level insights
Task: Rank Stores by total revenue.
Action: 
*/
SELECT
  store_id,
  SUM(discounted_amount) AS total_revenue
FROM retail_analytics_db.sales_transformed
GROUP BY store_id
ORDER BY total_revenue DESC
LIMIT 10;
--Result: Top N stores drove ₹X in revenue — ideal for pilot campaigns.

/*Query-4 Rank Stores by revenue (window function)
Situation: Businees team wants to identigy top-performing stores across the chains.
Task: Ranking Stores with Total revenue.
Action:
*/

SELECT
  store_id,
  SUM(discounted_amount) AS total_revenue,
  RANK() OVER (ORDER BY SUM(discounted_amount) DESC) AS revenue_rank
FROM retail_analytics_db.sales_transformed
GROUP BY store_id;
--Result: Store 102 ranked #1 with ₹X in revenue.

/* Query-5 Comparing daily revenue to 7-day average (CTE and Window Function)
Situation: Business Manager want to detect spikes and dips in daily sales.
Task: Compare each day's revenue to its trailing 7-day average.
Action: 
*/

WITH daily_sales AS (
  SELECT
    TRY_CAST(date AS DATE) AS sale_date,
    SUM(discounted_amount) AS daily_revenue
  FROM retail_analytics_db.sales_transformed
  WHERE LENGTH(date) = 10
  GROUP BY TRY_CAST(date AS DATE)
)

SELECT
  sale_date,
  daily_revenue,
  ROUND(AVG(daily_revenue) OVER (
    ORDER BY sale_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ), 2) AS trailing_7_day_avg
FROM daily_sales
ORDER BY sale_date;
--Result: Revenue on Sept 25 was ₹X, 12% above the trailing 7-day average.

/*Query 6 - Loyalty Tier Contribution (Joins and Aggregation)
Situation: Business Development manager wants to quantify how loyalty tier drive business revenue.
Task: Join enriched data and group loyalty status.
Action:
*/
SELECT
  loyalty_status,
  COUNT(*) AS num_customers,
  SUM(amount) AS total_revenue,
  ROUND(AVG(amount), 2) AS avg_spend
FROM retail_analytics_db.sales_enriched
GROUP BY loyalty_status
ORDER BY total_revenue DESC;

--Result: Gold tier customers contributed ₹X — with an average spend of ₹Y.


/* Query- 7 Percentage Revenue by category (Window + Partition)
Situation: Business team wants to know which product category dominates the revenue.
Task:Calculating percent contributing within each category.
Action:
*/

WITH category_sales AS (
  SELECT
    category,
    product_name,
    SUM(amount) AS product_revenue
  FROM retail_analytics_db.sales_enriched
  GROUP BY category, product_name
)

SELECT
  category,
  product_name,
  product_revenue,
  ROUND(
    100.0 * product_revenue / SUM(product_revenue) OVER (PARTITION BY category),
    2
  ) AS percent_of_category
FROM category_sales
ORDER BY category, percent_of_category DESC;

--Result: Product A contributed 42% of revenue in Electronics — a clear leader.

/* Query 8 - Monthly Revenue Trend by Category (Date Functions and Aggregation)
Situation: Business team wants to identify seasonal trends in product categories.
Task: Analyze monthly revenue trends by category.
Action:
*/  
SELECT
  category,
  DATE_FORMAT(TRY_CAST(date AS DATE), '%Y-%m') AS sale_month,
  SUM(discounted_amount) AS monthly_revenue
FROM retail_analytics_db.sales_enriched
WHERE LENGTH(date) = 10 -- Ensures valid date format
GROUP BY category, DATE_FORMAT(TRY_CAST(date AS DATE), '%Y-%m')             
ORDER BY category, sale_month;

--Result: Apparel saw a 30% revenue spike in November, likely due to festive sales


 