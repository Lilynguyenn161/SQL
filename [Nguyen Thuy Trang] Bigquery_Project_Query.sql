-- Big project for SQL

-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL

select
    format_date("%Y%m", parse_date("%Y%m%d", date)) as month, 
    SUM(totals.visits) AS visits,
    SUM(totals.pageviews) as pageviews,
    SUM(totals.transactions) as transactions,
    SUM(totals.totalTransactionRevenue)/10000000 as revenue 
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix between '20170101' and '20170331'
group by month   --group by 1
order by month;  --order by 1


-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
select 
    trafficSource.source as source,
    sum(totals.visits) as total_visits
    SUM(totals.bounces) as total_no_of_bounces,
    SUM(totals.bounces)*100/COUNT(DISTINCT fullVisitorId) as bounce_rate 
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`
Where _table_suffix LIKE '201707%'
group by source
order by total_visits desc;

-- Query 3: Revenue by traffic source by week, by month in June 2017

with month_data as(
SELECT
  "Month" as time_type,
  format_date("%Y%m", parse_date("%Y%m%d", date)) as month,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
),

week_data as(
SELECT
  "Week" as time_type,
  format_date("%Y%W", parse_date("%Y%m%d", date)) as date,
  trafficSource.source AS source,
  SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
_TABLE_SUFFIX BETWEEN '20170601' AND '20170631'
GROUP BY 1,2,3
order by revenue DESC
)

select * from month_data
union all
select * from week_data


--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL

with purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      (sum(totals.pageviews)/count(distinct fullvisitorid)) as avg_pageviews_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions>=1
  group by month
),

non_purchaser_data as(
  select
      format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
      sum(totals.pageviews)/count(distinct fullvisitorid) as avg_pageviews_non_purchase,
  from `bigquery-public-data.google_analytics_sample.ga_sessions_2017*`
  where _table_suffix between '0601' and '0731'
  and totals.transactions is null
  group by month
)

select
    pd.*,
    avg_pageviews_non_purchase
from purchaser_data pd
left join non_purchaser_data using(month)
order by pd.month

-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
select 
    format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
    SUM(totals.transactions)/COUNT(DISTINCT fullVisitorId) as Avg_total_transactions_per_user
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`  
where _table_suffix LIKE '201707%'  
and totals.transactions >= 1 
group by month;

-- Query 06: Average amount of money spent per session
#standardSQL

select 
     format_date("%Y%m",parse_date("%Y%m%d",date)) as month,
     SUM(totals.totalTransactionRevenue)/COUNT(DISTINCT fullVisitorId) as avg_revenue_by_user_per_visit
    FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
where totals.transactions IS NOT NULL 
and _table_suffix LIKE '201707%' 
group by month;

-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL
select 
  DISTINCT product.v2ProductName as other_purchased_products,
  SUM(product.productQuantity) as quantity
from
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where product.productRevenue IS NOT NULL 
  and product.v2ProductName !="YouTube Men's Vintage Henley"
  and fullVisitorId IN 
(select DISTINCT fullVisitorId
from `bigquery-public-data.google_analytics_sample.ga_sessions_*`, 
  UNNEST (hits) hits,
  UNNEST (hits.product) product
where product.v2ProductName ="YouTube Men's Vintage Henley")
and _table_suffix LIKE '201707%'
group by other_purchased_products
order by quantity DESC;

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL

with product_data as(
select
    format_date('%Y%m', parse_date('%Y%m%d',date)) as month,
    count(CASE WHEN eCommerceAction.action_type = '2' THEN product.v2ProductName END) as num_product_view,
    count(CASE WHEN eCommerceAction.action_type = '3' THEN product.v2ProductName END) as num_add_to_cart,
    count(CASE WHEN eCommerceAction.action_type = '6' THEN product.v2ProductName END) as num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
,UNNEST(hits) as hits
,UNNEST (hits.product) as product
where _table_suffix between '20170101' and '20170331'
and eCommerceAction.action_type in ('2','3','6')
group by month
order by month
)

select
    *,
    round(num_add_to_cart/num_product_view * 100, 2) as add_to_cart_rate,
    round(num_purchase/num_product_view * 100, 2) as purchase_rate
from product_data

