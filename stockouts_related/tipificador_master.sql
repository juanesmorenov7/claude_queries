------
-------------
--- En esta Query puedes revisar los agotados y poder hacer diferentes modulaciones por los registros de la tabla.
------------
------------------------------- 
--- Esta tabla guarda todas las ordenes (order_id) y los productos que tuvo en esa orden junto a data por cada producto de la orden (product_id), tiene una  primary key: Order_id_product



with base as (
SELECT t.country, t.week,  coalesce(t.product_name,p.name) as product_name, t.product_store_id, t.cp_physical_id, t.brand, coalesce(t.cat3_name,cc.name) as cat3_name ,t.day,
       t.diff_order_created, t.final_state, t.total_price, t.sku_count, t.TOP, t.sold_gmv, t.create_stock,                t.cat2_name, t.cat1_name, 
        i.name as cp_name, t.typification, t.product_id, t.order_id, t.order_id_product, t.retail_id
    
FROM RP_SILVER_DB_PROD.CPGS_LOCAL_ANALYTICS.TBL_TIPIFICADOR_CO t
    left join  fivetran.CO_AMYSQL_CPGS_CLG_IM_CPGS_CLG_INVENTORY_MANAGER.STORE i ON i.id = t.cp_physical_id
    left join fivetran.co_PGLR_MS_CPGS_CLG_PM_PUBLIC.retailer_product pp ON pp.id = t.product_id
left join fivetran.co_PGLR_MS_CPGS_CLG_PM_PUBLIC.PRODUCT p on p.id = pp.product_id 
left join fivetran.co_PGLR_MS_CPGS_CLG_PM_PUBLIC.category cc on p.category_id = cc.id 
    
WHERE 1=1
    and vertical_sub_group not in ('turbo')
    and day >= current_date-15
    -- and brand in ('carulla')
    -- and t.cp_brand_id = 6120

--
--and vertical_group in ('ecommerce')---ND vertical_sub_group in ('ecommerce','Ecommerce')---'express','super','liquor')('express','super','liquor')
-- and vertical_group in ('cpgs')
-- and brand in ('makro super')
-- and vertical_sub_group in ('pets')

)

select 
-- week,
-- year(day) as year_,
day,
brand,

count(distinct iff(final_state = 'cso', order_id, null)) as cso, --- ordenes canceladas por stockout -- aquí hace un recuento distinto
count(distinct iff(final_state = 'cso' and sku_count=1, order_id, null)) as cso_1sku, --> ordenes canceladas por stockouut donde solo había 1 producto (1 sku)
count(distinct iff(final_state in ('stock','substituted','stockout') or (sku_count=1 and final_state in ('cso')), order_id_product, null)) as total_products, --> Es el total de productos ordenados, no se tiene en cuenta las cancelaciones (finale state = canceled) y solo se tiene en cuenta las cancelaciones por stockout(cso) de 1 sku
count(distinct iff(final_state in ('substituted','stockout') or (sku_count=1 and final_state in ('cso')), order_id_product, null)) as total_so, --> Es el total de productos marcados en stockout o sustituidos o que tuvieron una cancelación por stockout de 1 sku
count(distinct iff(final_state in ('stockout') or (sku_count=1 and final_state in ('cso')), order_id_product, null)) as total_so_ffr, --> Es el total de productos marcados en stockout o que tuvieron una cancelación por stockout de 1 sku (no incluye los que se lograron sustituir)
count(distinct iff(sku_count=1 and final_state in ('cso', 'canceled'), order_id_product, null)) as total_canceled, --> Es el total de productos cancelados donde solo había 1 producto (1 sku)
round(coalesce(sum( sold_gmv),0)) as gmv_sold, ---> Lo que se vendió efectivamente
round(100*coalesce(div0(gmv_sold, sum(gmv_sold) over (partition by day)),0),5) as share_gmv, --> el share de ventas sobre la dimensión
100*round(1-div0(total_so,total_products),3) as fr,--> este es el found rate 
100*round(1-div0(total_so_ffr,total_products),3) as ffr --> este es el fullfillment rate es decir la tasa de productos encontrados o sustituidos

-- FROM RP_SILVER_DB_PROD.CPGS_LOCAL_ANALYTICS.TBL_TIPIFICADOR_CO t
from base
where 1=1
and day >= current_date-7
group by all
order by 1
;
