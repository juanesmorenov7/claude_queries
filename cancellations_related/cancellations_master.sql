------
--- Nota para claude - Esta query trae las tablas base para calcular el cancel rate y la forma de cancelarlo para que claude pueda modular.
-- la puedes solo ejecutar para el país que se te pida.

with ORDERS as (


SELECT O.*, ops.last_rt_transport_media_type, ops.last_rt_auto_accept, G.GMV, G.GMV_USD, G.DISCOUNTS, G.RAPPICREDITS, G.COUNT_TO_GMV, G.TIP, OPS.STORE_ZONE_DISTANCE  FROM RP_SILVER_DB_PROD.DES_PROD.ORDERS_CO O LEFT JOIN RP_SILVER_DB_PROD.DES_PROD.OPS_ORDERS_CO OPS ON OPS.COUNTRY = O.COUNTRY AND OPS.ORDER_ID = O.ORDER_ID LEFT JOIN rp_silver_db_prod.des_prod.orders_gmv_co G ON G.COUNTRY = O.COUNTRY AND G.ORDER_ID = O.ORDER_ID  
WHERE      DATE_TRUNC('DAY', COALESCE(O.CLOSED_AT,O.CANCELLED_AT,  O.CREATED_AT))::DATE >= CURRENT_DATE-10    AND DATE_TRUNC('DAY', COALESCE(O.CLOSED_AT,O.CANCELLED_AT,  O.CREATED_AT))::DATE < CURRENT_DATE  and o.state not in ('canceled_by_fraud','canceled_for_payment_error', 'canceled_by_early_regret') AND O.VERTICAL_GROUP IN ('CPGS','ECOMMERCE')  AND O.VERTICAL_SUB_GROUP NOT IN ('CARGO', 'TURBO')

)

, DIMENSIONS AS (
select * from RP_GOLD_DB_PROD.DES_PROD.ORDER_DIMENSIONS_CO WHERE ORDER_ID IN (SELECT DISTINCT ORDER_ID FROM ORDERS WHERE COUNTRY = 'CO')


)

, CANCELLATIONS AS (

SELECT C.*, V.STAGE_OF_THE_ORDER, V.RT_MINUTES_IN_STORE, V.CANCELATION_REASON, V.IS_CARGO  FROM RP_SILVER_DB_PROD.DES_PROD.CANCELLATIONS_CO C LEFT JOIN RP_SILVER_DB_PROD.DES_PROD.CANCELLATIONS_VARIABLES_CO V ON C.COUNTRY = V.COUNTRY AND C.ORDER_ID = V.ORDER_ID WHERE C.ORDER_ID IN (SELECT DISTINCT ORDER_ID FROM ORDERS WHERE COUNTRY = 'CO') 
)

, ORDER_TIMES AS (

SELECT * FROM RP_SILVER_DB_PROD.DES_PROD.order_times_co WHERE 1=1 AND ORDER_ID IN (SELECT DISTINCT ORDER_ID FROM ORDERS WHERE COUNTRY = 'CO')

)

, products_value as (
SELECT country, order_id, round(sum(total_price)) as total_value_products  FROM rp_silver_db_prod.des_prod.order_product_details_co  where order_id in (select distinct order_id from orders where country = 'CO')  group by all 

)
, final as (

select
    O.COUNTRY
    , O.ORDER_ID
    , date_trunc('DAY', COALESCE(O.CLOSED_AT,O.CANCELLED_AT,  O.CREATED_AT))::date AS  DAY 
    , date_part('hour', COALESCE(O.CLOSED_AT,O.CANCELLED_AT,  O.CREATED_AT)) as hour  
    , date_trunc('hour', C.CANCELLED_AT)::time as hour_canceled
    , O.CREATED_AT
    , O.PLACED_AT
    , O.VERTICAL_GROUP
    , O.VERTICAL_SUB_GROUP
    , O.VERTICAL
    , COALESCE(DS.BRAND_GROUP, DS.BRAND) AS BRAND_GROUP
    , COALESCE(DS.BRAND_GROUP_ID, DS.BRAND_ID ) AS BRAND_GROUP_ID
    , COALESCE(ST.PHYSICAL_STORE_ID, DS.physical_store_id) AS PHYSICAL_STORE_ID
    , ds.cp_store_id
    , O.STORE_ID
    , DS.STORE_NAME
    , DS.STORE_TYPE
    , O.CITY_ADDRESS_ID
    , DS.CITY
    , DIM.STORE_ZONE_NAME
    , DS.MICROZONE
    , DS.MICROZONE_ID
    , ST.STORE_LAT AS LAT
    , ST.STORE_LNG AS LNG
    , dim.store_zone_size as polygon_size 
    , DIM.USER_ZONE_NAME
    , DIM.USER_MICROZONE_NAME
    , DIM.payment_method
    , DIM.tiP
    , DIM.DELIVERY_FEE_USD
    , SERVICE_FEE
    , DIM.SKU_COUNT
    , DIM.ITEM_COUNT
    , o.last_rt_auto_accept as AUTO_ACCEPTANCE
    , O.STOREKEEPER_ID
    , o.last_rt_transport_media_type as VEHICLE_TYPE
    , O.IS_CANCELLED
    , O.IS_FINISHED
    , C.CANCELATION_REASON
    , c.LEVEL_3
    , c.effective_repurchase
    , c.CANCELLED_AT
    , case when c.Category is null then 'Finished' else c.category end as category
    , case when c.subcategory is null then 'Finished' else c.subcategory end as subcategory
    , B.DEFECTS_LEVEL_1
    , B.DEFECTS_LEVEL_2
    , B.DEFECTS_LEVEL_3
    , B.KUSTOMER_CONVERSATION_ID    
    , c.CANCELATION_PERFORMED_BY
    , C.STAGE_OF_THE_ORDER
    , o.application_user_id
    , case when O.state ilike '%cancel%' then 'canceled' else 'finished' end as state_type
    , CASE WHEN O.STATE NOT ILIKE '%CANCEL%' THEN datediff ('minutes', coalesce (o.placed_at, o.created_at), coalesce(o.closed_at, c.CANCELLED_AT, o.cancelled_at)) ELSE NULL END as tiempo_total
    , case 
            when O.is_finished = False and timediff('seconds', coalesce(o.placed_at, o.created_at), coalesce(o.cancelled_at, o.placed_at, o.created_at)) between 0 and 300 then 'A) 0-5 mins'
            when O.is_finished = False and timediff('seconds', coalesce(o.placed_at, o.created_at), coalesce(o.cancelled_at, o.placed_at, o.created_at)) between 301 and 600 then 'B) 5-10 mins'
            when O.is_finished = False and timediff('seconds', coalesce(o.placed_at, o.created_at), coalesce(o.cancelled_at, o.placed_at, o.created_at)) between 601 and 900 then 'C) 10-15 mins'
            when O.is_finished = False and timediff('seconds', coalesce(o.placed_at, o.created_at), coalesce(o.cancelled_at, o.placed_at, o.created_at)) between 901 and 1200 then 'D) 15-20 mins'
            when O.is_finished = False and timediff('seconds', coalesce(o.placed_at, o.created_at), coalesce(o.cancelled_at, o.placed_at, o.created_at)) > 1201 then 'E) 20+ mins'
            else null
            end as time_of_cancellation
    , o.gmv_usd
    , o.gmv
    , f.PICKED_BY
    , PRD.total_value_products as valor_de_products
    , case when b.cancellations_category iS NOT NULL THEN 1 ELSE 1 end  as ORDERS_count2
    , case when O.order_id is not null then 1 else 1 end as order_count
    , case when C.Category not in ('finished') then 1 else 0 end as cancel_count
    , iff(o.PLACED_AT is null, 'NOW','PROG') AS FLOW
    , iff(b.is_delay = 'TRUE', 1,NULL) AS DELAY
    , IFF(B.IS_CANCELLED = 'TRUE',1,NULL) AS CANCELED
    , IFF(B.IS_DEFECT = 'TRUE',1,NULL) AS DEFECT
    , IFF(B.IS_BAD_ORDER = 'TRUE',1,NULL) AS IS_BAD_ORDER
    , DIM.IS_BATCHING_BUNDLING
    , DIM.POSSIBLE_FRAUD_WITH_TAKE
    , DIM.GROSS_CONTRIBUTION_MARGIN
    , DIM.RT_PAY
    , DIM.RAPPICREDITS_USD
    , DIM.PROMO_PERCENTAGE
    , C.RT_MINUTES_IN_STORE
    , C.IS_CARGO
    , B.MINUTES_LATE
    ,case B.is_delay
            when minutes_late >= 0 and minutes_late <= 5  then 'A) 10 + 0-5 min'
            when minutes_late > 5  and minutes_late <= 10 then 'B) 10 + 5-10 min'
            when minutes_late > 10 and minutes_late <= 20 then 'C) 10 + 10-20 min'
            when minutes_late > 20 and minutes_late <= 30 then 'D) 10 + 20-30 min'
            when minutes_late > 30 and minutes_late <= 40 then 'E) 10 + 30-40 min'
            when minutes_late > 40 and minutes_late <= 50 then 'F) 10 + 40-50 min'
            when minutes_late > 50 and minutes_late <= 60 then 'G) 10 + 50-60 min'
            when minutes_late > 60 then 'H) 10 + 60+ min'
            else 'Not Applicable'
            end as lateness_bucket
    , dim.delivery_fee_usd as shipping
    , dim.product_price_usd
    , O.STORE_ZONE_DISTANCE
from ORDERS O
        LEFT JOIN DIMENSIONS DIM ON DIM.COUNTRY = O.COUNTRY AND DIM.ORDER_ID = O.ORDER_ID
        LEFT JOIN ORDER_TIMES ot on ot.order_id = o.order_id AND ot.country = o.country  
        LEFT JOIN CANCELLATIONS c on c.country = o.country and c.order_id = o.order_id
        LEFT JOIN products_value PRD ON PRD.COUNTRY = O.COUNTRY AND PRD.ORDER_ID = O.ORDER_ID
        LEFT JOIN RP_SILVER_DB_PROD.DES_PROD.BAD_ORDERS AS B ON B.COUNTRY = O.COUNTRY AND B.ORDER_ID = O.ORDER_ID
        LEFT JOIN  FIVETRAN.GLOBAL_FINANCES.GLOBAL_ORDER_DETAILS F ON o.ORDER_ID = F.ORDER_ID AND o.COUNTRY = F.COUNTRY          
        LEFT JOIN RP_SILVER_DB_PROD.CPGS_LOCAL_ANALYTICS.TBL_DIM_STORES DS ON DS.STORE_ID = O.STORE_ID AND DS.COUNTRY = O.COUNTRY
        LEFT JOIN RP_SILVER_DB_PROD.DES_PROD.STORES ST ON ST.COUNTRY = O.COUNTRY AND ST.STORE_ID = O.STORE_ID   
    
   
where 1=1
    -- AND DATE_TRUNC('DAY', COALESCE(O.CLOSED_AT,O.CANCELLED_AT,  O.CREATED_AT))::DATE >= CURRENT_DATE-90
    -- AND DATE_TRUNC('DAY', COALESCE(O.CLOSED_AT,O.CANCELLED_AT,  O.CREATED_AT))::DATE < CURRENT_DATE

    AND DIM.IS_MARKETPLACE = FALSE
    AND DIM.IS_OPS = TRUE
    AND DIM.SYNTHETIC = FALSE
)

select 
    day
    , count(distinct f.order_id) as total_orders
    , count(distinct iff(canceled = 1 and effective_repurchase = false, f.order_id, null)) as total_cancellations
    , count(distinct iff(canceled = 1 and effective_repurchase = false and category in ('RT'), f.order_id, null)) as total_cancellations_rt
    , count(distinct iff(canceled = 1 and effective_repurchase = false and category in ('Partner'), f.order_id, null)) as total_cancellations_partner
    , count(distinct iff(canceled = 1 and effective_repurchase = false and category in ('UX','User'), f.order_id, null)) as total_cancellations_ux_user
    , count(distinct iff(canceled = 1 and effective_repurchase = false and subcategory in ('Stockout'), f.order_id, null)) as total_cancellations_stockout

    , div0(total_cancellations, total_orders) as total_cancellations_rate
    , div0(total_cancellations_rt, total_orders) as total_cancellations_rt_rate
    , div0(total_cancellations_partner, total_orders) as total_cancellations_partner_rate
    , div0(total_cancellations_ux_user, total_orders) as total_cancellations_ux_user_rate
    , div0(total_cancellations_stockout, total_orders) as total_cancellations_stockout_rate

    from final f
    group by all
    ;
