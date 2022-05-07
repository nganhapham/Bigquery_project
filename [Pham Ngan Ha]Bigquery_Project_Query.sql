-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
SELECT  
    format_date("%m", parse_date("%Y%m%d",date)) as month,
    sum(totals.visits) as visits, 
    sum(totals.pageviews) as pageviews,
    sum(totals.transactions) as transactions, 
    round(sum(totals.totalTransactionRevenue/1000000.0),2) as revenue    
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE  _table_suffix between '20170101' and '20170331'
GROUP BY  month
ORDER BY  month;

-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT 
trafficSource.source, SUM(totals.visits) AS total_visit,
SUM(totals.bounces) AS total_no_of_bounce,
SUM(totals.bounces)/SUM(totals.visits)*100.0 AS bounce_rate
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY  trafficSource.source
ORDER BY total_visit DESC;

-- Query 3: Revenue by traffic source by week, by month in June 2017
SELECT 
    'Month' as Type_of_date,
    format_date("%Y%m", parse_date("%Y%m%d",date)) as time,
    trafficSource.source,
    SUM(totals.totalTransactionRevenue)/1000000 as Revenue,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY  trafficSource.source, time
UNION ALL
SELECT
    'Week' as Type_of_date,
    format_date("%Y%W", parse_date("%Y%m%d",date)) as time,
    trafficSource.source,
SUM(totals.totalTransactionRevenue)/1000000 as Revenue,
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201706*`
GROUP BY  trafficSource.source, time
ORDER BY Revenue desc;

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH purchase AS
    (SELECT 
        FORMAT_DATE("%Y%m", parse_date("%Y%m%d",date)) as Month,
        sum(totals.pageviews)/count(distinct fullvisitorID) as avg_pageview_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE  _table_suffix between '20170601' and '20170731'
    AND totals.transactions >= 1
    GROUP BY month),
non_purchase AS
    (SELECT 
        FORMAT_DATE("%Y%m", parse_date("%Y%m%d",date)) as Month,
        sum(totals.pageviews)/count(distinct fullvisitorID) as avg_pageview_non_purchase
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
    WHERE  _table_suffix between '20170601' and '20170731'
    AND totals.transactions IS NULL
    GROUP BY month)
SELECT 
    purchase.Month,
    purchase.avg_pageview_purchase,
    non_purchase.avg_pageview_non_purchase
FROM purchase
INNER JOIN non_purchase 
USING(Month)
GROUP BY purchase.Month, purchase.avg_pageview_purchase, non_purchase.avg_pageview_non_purchase
ORDER BY purchase.Month;

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL

SELECT 
    FORMAT_DATE("%Y%m", parse_date("%Y%m%d",date)) as Month,
    sum(totals.transactions)/count(distinct fullvisitorID) as avg_pageview_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >= 1
GROUP BY month;

-- Query 06: Average amount of money spent per session
#standardSQL
SELECT 
    FORMAT_DATE("%Y%m", parse_date("%Y%m%d",date)) as Month,
    sum(totals.totalTransactionRevenue)/count(fullvisitorID) as avg_revenue_by_user_per_visit
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions IS NOT NULL
GROUP BY month;

-- Query 07: Products purchased by customers who purchased product A (Classic Ecommerce)
#standardSQL

SELECT product.v2ProductName as other_purchased_products,
sum(product.productQuantity) as Quantity
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST(hits) AS hits,
UNNEST(hits.product) as product
WHERE fullVisitorID IN (
    SELECT
    fullVisitorID
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
    UNNEST(hits) AS hits,
    UNNEST (hits.product) as product
    WHERE product.v2ProductName = "YouTube Men's Vintage Henley" 
    AND totals.transactions>=1
    AND product.productRevenue is not null
    GROUP BY fullVisitorId
    )
AND product.productRevenue is not null
AND product.v2ProductName <> "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY Quantity DESC
--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

WITH view AS
    (SELECT 
        format_date("%Y%m", parse_date("%Y%m%d", date )) as month,
        count(hits.eCommerceAction.action_type) as num_product_view
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
    WHERE  _table_suffix between '20170101' and '20170331'
    AND hits.eCommerceAction.action_type = '2'
    GROUP BY month),
cart AS
    (SELECT 
        format_date("%Y%m", parse_date("%Y%m%d", date )) as month,
        count(hits.eCommerceAction.action_type) as num_addtocart
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits
    WHERE  _table_suffix between '20170101' and '20170331'
    AND hits.eCommerceAction.action_type = '3'
    GROUP BY month),
purchase AS
    (SELECT 
        format_date("%Y%m", parse_date("%Y%m%d", date )) as month,
        count(product.productsku) as num_purchase,
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
    UNNEST(hits) AS hits,
    UNNEST(hits.product) AS product
    WHERE  _table_suffix between '20170101' and '20170331'
    AND hits.eCommerceAction.action_type = '6'
    GROUP BY month)
SELECT 
    v.month,
    v.num_product_view,
    c.num_addtocart,
    p.num_purchase,
    round(((c.num_addtocart/v.num_product_view)*100.0),2) as add_to_cart_rate,
    round(((p.num_purchase/v.num_product_view)*100.0),2) as purchase_rate
FROM view as v
INNER JOIN cart c using(month)
INNER JOIN purchase p using(month)
GROUP BY month, num_product_view, num_addtocart, num_purchase
ORDER BY  month;

