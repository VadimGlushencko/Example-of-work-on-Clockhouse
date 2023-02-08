WITH 
orders_final AS (
	SELECT TRIM(store_name) as store_name
		 , DATE(created_at) as date
		 , CONCAT(utm_source, ' / ', utm_medium) as source_medium
		 , COUNT(*) as orders
		 , SUM(CASE WHEN shipment_created_at IS NOT NULL THEN 1 ELSE 0 END) as orders_shipped
		 , SUM(GREATEST(subtotal + shipping_amount - discount_amount - giftcard_amount, 0)) as grand_total
		 , SUM(CASE 
		 			WHEN shipment_created_at IS NOT NULL 
		 			THEN GREATEST(subtotal + shipping_amount - discount_amount - giftcard_amount, 0) 
		 			ELSE 0 END) as grand_total_shipped
		 , 0 as sessions
		 , 0 as users
		 , 0 as new_users
		 , 0 as pageviews
		 , 0 as session_duration
	FROM aristos.orders o FINAL
	WHERE date >= '2020-01-01'
		  AND status NOT IN ('canceled', 'closed')
	GROUP BY store_name
		   , date
		   , source_medium	
),
orders_with_short_store_name as (
	SELECT o.*
		 , s.store_group_nm as short_store_name
	FROM orders_final o
	LEFT JOIN replacement.store s 
	USING store_name 
),
sessions as (
	SELECT date
		 , source_medium
		 , 0 as orders
		 , 0 as orders_shipped
		 , 0 as grand_total
		 , 0 as grand_total_shipped
		 , sessions
		 , users
		 , new_users
		 , pageviews
		 , session_duration
		 , sm.store_name as short_store_name
	FROM google_analytics.source_medium sm FINAL 
),
orders_with_sessions as (
			  SELECT * FROM orders_with_short_store_name
	UNION ALL SELECT NULL as store_name, * FROM sessions
),
final_table as (	
	SELECT * 
		 , splitByString(' / ', source_medium)[1] as utm_source
		 , splitByString(' / ', source_medium)[2] as utm_medium
		 , CASE 
				WHEN source_medium LIKE 'yandex / cpc' THEN 'Яндекс.Директ'
				WHEN source_medium LIKE 'google / cpc' THEN 'Google Ads'
				WHEN source_medium LIKE 'yamarket / teaser_cpc' THEN 'Яндекс.Маркет'
				WHEN source_medium LIKE 'flocktory%' THEN 'Flocktory'
				WHEN source_medium LIKE '% / offsite' THEN 'Официальный сайт'
				WHEN source_medium LIKE '(direct) / (none)' THEN 'Прямые заходы'
				WHEN source_medium LIKE 'advcake / cpa' THEN 'Advcake'
				WHEN source_medium LIKE 'addigital_network / cpc' THEN 'Addigital'
				WHEN source_medium LIKE 'mytarget / cpc' THEN 'myTarget'
				WHEN source_medium LIKE 'sendpulse / push' THEN 'Push / SendPulse'
				WHEN source_medium LIKE 'share_basket / link' THEN 'Share Basket'
				WHEN source_medium LIKE 'nadavi / teaser_cpc' THEN 'Nadavi'
				WHEN source_medium LIKE 'sendpulse / webpush' THEN 'SendPulse'
				WHEN source_medium LIKE 'vk / (not set)' THEN 'Вконтакте'
				WHEN source_medium LIKE 'google / organic' THEN 'Органика Google'
				WHEN source_medium LIKE 'yandex / organic' THEN 'Органика Яндекс'
				WHEN source_medium LIKE 'yandex.% / organic' THEN 'Органика Яндекс'
				WHEN source_medium LIKE 'yandex.% / referral' THEN 'Органика Яндекс'
				WHEN source_medium LIKE 'turbo_turbo / (not set)' THEN 'Турбо'
				WHEN source_medium LIKE '% / cpm' THEN 'CPM'
				WHEN source_medium LIKE '% / email' THEN 'Email'
				WHEN source_medium LIKE '% / cpc' THEN 'CPC'
				WHEN source_medium LIKE '% / cpa' THEN 'CPA'
				WHEN source_medium LIKE '% / cpm' THEN 'CPM'			
				WHEN source_medium LIKE '% / referral' THEN 'Referral'
				WHEN source_medium LIKE '% / organic' THEN 'Органика'
				WHEN source_medium LIKE '% / teaser_cpc' THEN 'Teaser_cpc'
				WHEN source_medium LIKE '% / (not set)' THEN 'Not set'
				WHEN source_medium LIKE 'yandex_products / teaser_organic' THEN 'Yandex.Products'
				ELSE 'Остальное' 
				END as source
	FROM orders_with_sessions 
	LEFT JOIN dictionary.datepart d 
	USING date
),
orders as (
    SELECT project
    	 , status
		 , s.store_group_nm as store_name
		 , last_increment_id
		 , DATE(created_at) as created_at
		 , DATE(shipment_created_at) as shipment_created_at
		 , DATE(rma_created_at) as rma_created_at
		 , subtotal + shipping_amount - discount_amount - giftcard_amount as grand_total  
	     -- Добавляем источники
		 , CONCAT(utm_source, ' / ', utm_medium) as source_medium
		 , CASE WHEN LOWER(coupon_code) LIKE '%gfc%' THEN 'Get4Click'
		 		WHEN LOWER(coupon_code) LIKE '%g4c%' THEN 'Get4Click'
		 		WHEN LOWER(coupon_code) LIKE '%get4%' THEN 'Get4Click'
		 		WHEN LOWER(coupon_code) LIKE '%g4с%' THEN 'Get4Click'
		 		WHEN LOWER(coupon_code) LIKE '%gfс%' THEN 'Get4Click'
				WHEN source_medium LIKE '%addigital%' THEN 'Addigital'			 		
		 	    WHEN source_medium LIKE 'google / cpc' THEN 'Google Ads'
				WHEN source_medium LIKE 'yandex / cpc' THEN 'Яндекс.Директ'
				WHEN source_medium LIKE 'yamarket / teaser_cpc' THEN 'Яндекс.Маркет'
				WHEN source_medium LIKE 'advcake / cpa' THEN 'Advcake'
				WHEN source_medium LIKE '% / cpa' THEN 'CPA'
				WHEN source_medium LIKE '% / organic' THEN 'Органика'
				WHEN source_medium LIKE 'gr / email' THEN 'Рассылка'
				WHEN source_medium LIKE 'mytarget / cpc' THEN 'Таргетированная'
				WHEN source_medium LIKE 'flocktory / %' THEN 'Флоктори'
				WHEN source_medium LIKE 'cl-base / email' THEN 'Email'
				ELSE 'Остальное' END as source
    FROM aristos.orders o FINAL
    LEFT JOIN replacement.store s
	ON TRIM(o.store_name) = s.store_name
	WHERE     store_name != 'Other'
		  AND project != 'luxe'
),
advcake_commission AS (
	SELECT store_name
		 , REPLACE(
		 	LEFT(
		 		toString(
    		 	  CASE WHEN LEFT(toString(c.created_at), 7) = LEFT(toString(today()), 7) 
    		 	       THEN c.created_at
    		 		   ELSE c.updated_at 
    		 		   END as date) -- По дате создания (c.created_at) или обновления (c.updated_at)
    		 	, 7)
    		, '-'
    		, ''
    		) as yearmonth
		 , SUM(c.final_commission) as commission
	FROM advcake.commission c FINAL
	GROUP BY store_name
		   , yearmonth
),
advcake_cost as (
	SELECT c.store_name
		 , d.date
		 , 0 as clicks
		 , CASE WHEN LEFT(toString(d.date), 7) = LEFT(toString(today()), 7)
		 		THEN c.commission / greatest((DAY(today()) - 1), 1)
		 		ELSE c.commission / d.days_in_month 
		 		END as cost
	FROM advcake_commission as c   
	LEFT JOIN dictionary.datepart d
	USING yearmonth    	 	
	WHERE d.date < today()
),  
get4click_cost as (
	SELECT store_name
		 , created_at as date
		 , 0 as clicks
		 , SUM(CASE WHEN status IN ('canceled', 'closed') THEN 0
		 			WHEN store_name = 'Purina' THEN grand_total  * 6 / 100
 					ELSE grand_total * 4 / 100
 					END) as cost
 	FROM orders
 	WHERE source = 'Get4Click'
 	GROUP BY store_name
 		   , date
),
addigital_cost as (
	SELECT store_name
		 , date
		 , sessions as clicks
		 , sessions * 5 as cost -- 5 рублей за клик (сеанс)
	FROM google_analytics.source_medium sm 
	WHERE source_medium like '%addigital%'
),
yandex_direct as (
	SELECT store_name
		 , date
		 , clicks
		 , cost
	FROM yandex_direct.traffic_by_day FINAL
),
google_ads as (
	SELECT store_name
		 , date
		 , clicks
		 , cost
	FROM google_ads.traffic_by_day FINAL
),
yandex_market as (
	SELECT store_name
		 , date
		 , clicks
		 , cost
	FROM yandex_market.traffic_by_day FINAL
),
-- Загружаем расходы
costs AS (
			  SELECT 'Яндекс.Директ' 	as source, * FROM yandex_direct
    UNION ALL SELECT 'Google Ads'    	as source, * FROM google_ads
    UNION ALL SELECT 'Яндекс.Маркет'    as source, * FROM yandex_market
    UNION ALL SELECT 'Advcake'			as source, * FROM advcake_cost
    UNION ALL SELECT 'Get4Click'		as source, * FROM get4click_cost
	UNION ALL SELECT 'Addigital'		as source, * FROM addigital_cost
),
final_table AS (
SELECT f.*
	 , CASE WHEN f.store_name IS NULL THEN c.clicks ELSE 0 END as clicks
	 , CASE WHEN f.store_name IS NULL THEN c.cost ELSE 0 END as cost
	 , case short_store_name 
			WHEN 'Axor' THEN 'Axor Retail Store View'
			WHEN 'Black+Decker' THEN 'BD Store View'
			WHEN 'Cartier' THEN 'Cartier Store View'
			WHEN 'Castrol Marketplace' THEN 'Marketplace Castrol Store View'
			WHEN 'Castrol' THEN 'Castrol Store View'
			WHEN 'Dewalt' THEN 'Dewalt Store View'
			WHEN 'Emsa' THEN 'Emsa Store View'
			WHEN 'Geberit Marketplace' THEN 'Marketplace Geberit Store View'
			WHEN 'Geberit' THEN 'Geberit Store View'
			WHEN 'Grohe Marketplace' THEN 'Marketplace Grohe Store View'
			WHEN 'Grohe' THEN 'Grohe Store View'
			WHEN 'Hansgrohe Marketplace' THEN 'Marketplace Hansgrohe Store View'
			WHEN 'Hansgrohe' THEN 'Hansgrohe Retail Store View'
			WHEN 'Ideal Standard Marketplace' THEN 'Marketplace Ideal Standard Store View'
			WHEN 'Ideal Standard' THEN 'Ideal Standard Store View'
			WHEN 'IRG' THEN 'IRG Store View'
			WHEN 'Krups' THEN 'Krups PM Store View' ---!!! Krups Ariflex Store View
			WHEN 'Lenox' THEN 'Lenox Store View'
			WHEN 'Logic' THEN 'Logic Store View'
			WHEN 'Mavi' THEN 'Mavi Store View'
			WHEN 'Montblanc' THEN 'Montblanc Store View'
			WHEN 'Moulinex' THEN 'Moulinex Store View'
			WHEN 'Philips Marketplace' THEN 'Marketplace Store'
			WHEN 'Philips' THEN 'Beta Store'
			WHEN 'Ploom' THEN 'Ploom New Store View'
			WHEN 'Purina Marketplace' THEN 'Marketplace Purina Store View'
			WHEN 'Purina' THEN 'Purina Store View'
			WHEN 'Rehau' THEN 'Rehau Ariflex Store View'
			WHEN 'Roca Marketplace' THEN 'Marketplace Roca Store View'
			WHEN 'Roca' THEN 'Roca Store View'
			WHEN 'Rowenta' THEN 'Rowenta Store View'
			WHEN 'SBD Marketplace' THEN 'Marketplace SBD Store View'
			WHEN 'SBD Masterclub' THEN 'Masterclub Store View'
			WHEN 'Schneider Electric Marketplace' THEN 'Marketplace Schneider Store View'
			WHEN 'Schneider Electric' THEN 'Schneider Store View'
			WHEN 'Showroom' THEN 'Showroom Store View'
			WHEN 'Speedtest' THEN 'Speedtest Store View'
			WHEN 'Stanley' THEN 'Stanley Store View'
			WHEN 'Tefal' THEN 'Tefal Store View'
			WHEN 'Tikkurila' THEN 'Tikkurila Store View'
			WHEN 'VCA' THEN 'Van Cleef & Arpels RU Store View'
			WHEN 'Villeroy & Boch' THEN 'Villeroy & Boch Store View'
			WHEN 'Villeroy Boch Marketplace' THEN 'Villeroy & Boch Marketplace Store View'
			WHEN 'Villeroy Boch' THEN 'Villeroy & Boch Store View'
			WHEN 'Weber Marketplace' THEN 'Weber Marketplace Store View'
			WHEN 'Weber' THEN 'Weber Store View'
			else 'Остальное'
			END as store_name_filter
FROM final_table f
LEFT JOIN costs c
ON  f.short_store_name = c.store_name
AND LOWER(f.source) = LOWER(c.source)
AND f.date = c.date
)
SELECT f.*
	 , s.project
FROM final_table f
LEFT JOIN aristos.store s
ON f.store_name_filter = s.name