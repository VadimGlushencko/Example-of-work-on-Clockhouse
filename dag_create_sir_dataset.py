from datetime import timedelta, datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'admin',
    'start_date': datetime(2022, 3, 22, 0, 0),	
    'depends_on_past': False,     
    'description':'Импорт из Google Таблиц и загрузка в Clickhouse',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}    

with DAG(
    dag_id='create_sir_dataset', 
    default_args=default_args, 
    catchup=False, 
    start_date=datetime(2022, 3, 11, 0, 0),
    schedule_interval='0 */3 * * *' # каждые 3 часа
) as dag:

    def create_sir_dataset():
        from pandahouse import read_clickhouse
        
        db = 'datasets'                                                        # База данных
        table = 'sir'                                                          # Таблица
        update_field = 'updated_at'                                            # Поле с датой для обновление
        unique_fields = ['project', 
                         'store_name', 
                         'status', 
                         'last_order_id', 
                         'order_item_id', 
                         'shipment_item_id'
                        ]       # Уникальные поля
        
        # Данные для подключения к Clickhouse
        connection = dict(host = 'http://*****:8123',
                          user = '***',
                          password = '*****',
                          database = db)
        
        sql_query = """   
		WITH
		    shipment_and_return_items as (
		    	SELECT project
		    	     , order_item_id
		    	     , shipment_item_id
		    	     , type
		    	     , qty
		    	     , created_at
		    	FROM aristos.shipment_items si 
		    	WHERE type = 0  	
		    	UNION ALL
		    	SELECT project
		    	     , order_item_id
		    	     , id as shipment_item_id
		    	     , 1 as type
		    	     , qty
		    	     , created_at
		    	FROM aristos.return_items si 
		    ),
		    
		    -- Статистика по отгруженным товарам
		    items AS (
		        SELECT oi.project
		             , oi.order_item_id
		             , CASE WHEN si.shipment_item_id = 0 THEN ''
		                    WHEN si.shipment_item_id IS NULL THEN ''  
		                    ELSE toString(si.shipment_item_id) 
		                    END as shipment_item_id
		             , oi.attribute_set
		             , oi.business_group
		             , oi.is_gift
		             , oi.item_rule_ids
		             , oi.manufacturer
		             , oi.name
		             , oi.order_id
		             , oi.parent_group_product_sku
		             , oi.price
		             , oi.product_id
		             , oi.qty_returned
		             , oi.qty_shipped
		             , oi.retail_price
		             , oi.related_order_increment_id
		             , oi.sku
		             , oi.special_price
		
		             , CASE WHEN shipment_item_id IS NULL THEN 'w/o shipment'
		                    WHEN si.type = 0 THEN 'shipment' 
		                    WHEN si.type = 1 THEN 'rma'
		                    ELSE 'other'
		                    END AS shipment_type -- тип отгрузки
		
		             , CASE WHEN shipment_type = 'w/o shipment' THEN qty_ordered
		                    ELSE si.qty
		                    END as qty
		
		             , si.qty  * CASE WHEN shipment_type = 'shipment' THEN  1 
		                              WHEN shipment_type = 'rma' THEN -1
		                              ELSE 0
		                              END AS shipped_qty -- кол-во отгруженных товаров	
		             , oi.cost * CASE WHEN shipment_type = 'shipment' THEN  1 
		                              WHEN shipment_type = 'rma' THEN -1
		                              ELSE 1 
		                              END AS cost -- cost
		
		             , CASE WHEN shipment_type = 'shipment' AND qty_shipped = 0 THEN 0
		                    WHEN shipment_type = 'rma' AND qty_returned = 0 THEN 0
		                    WHEN shipment_type = 'shipment' THEN oi.row_total * shipped_qty / GREATEST(qty_shipped, 1)
		                    WHEN shipment_type = 'rma' THEN returned_amount * shipped_qty / GREATEST(qty_returned, 1)
		                    ELSE oi.row_total 
		                    END AS row_total -- стоимость товаров
		
		             , CASE WHEN qty_shipped = 0 THEN 0
		                    WHEN shipment_type = 'shipment' THEN oi.row_total_with_discount * shipped_qty / GREATEST(qty_shipped, 1)
		                    WHEN shipment_type = 'rma' THEN returned_amount * shipped_qty / GREATEST(qty_shipped, 1)
		                    ELSE oi.row_total_with_discount
		                    END AS row_total_with_discount -- стоимость товаров со скидкой
		
		             , DATE(TIMESTAMP_ADD(oi.created_at, INTERVAL 3 HOUR)) AS order_created_at	
		             , CASE WHEN shipment_item_id IS NULL THEN NULL 
		                    ELSE DATE(TIMESTAMP_ADD(si.created_at, INTERVAL 3 HOUR))
		                    END AS shipment_created_at
		
		        FROM aristos.order_items AS oi FINAL
		        LEFT JOIN shipment_and_return_items AS si FINAL
		        USING project
		            , order_item_id
		        WHERE oi.is_last_edition = 1
		     ),
		
		     items_with_created_dateparts as (
		        SELECT i.*
		             , d.week_from_to as order_week_from_to
		             , d.yearweek as order_yearweek
		             , d.yearmonth as order_yearmonth
		             , d.yearquarter as order_yearquarter
		             , d.month_year as order_month_year
		             , d.year as order_year
		        FROM items i
		        LEFT JOIN dictionary.datepart d
		        ON i.order_created_at = d.date
		     ),
		
		     items_with_shipment_dateparts as (
		        SELECT i.*
		             , d.week_from_to as shipment_week_from_to
		             , d.yearweek as shipment_yearweek
		             , d.yearmonth as shipment_yearmonth
		             , d.yearquarter as shipment_yearquarter
		             , d.month_year as shipment_month_year
		             , d.year as shipment_year
		        FROM items_with_created_dateparts i
		        LEFT JOIN dictionary.datepart d
		        ON i.shipment_created_at = d.date
		     ),
		     
		     items_with_orders as (
				 SELECT
				      i.*
				    , o.city
				    , o.complete_date
				    , o.country_id
				    , o.coupon_code
				    , o.customer_group
				    , o.customer_id
				    , o.ext_type
				    , o.last_order_id
				    , o.partner_id
				    , o.payment_method
				    , o.region
				    , o.rule_ids
				    , o.shipping_description
				    , o.shipping_method
				    , o.shipping_type
				    , o.staff_id
				    , o.status
				    , o.store_id
				    , o.tags_customer
				    , o.tags_order
				    , o.utm_campaign
				    , o.utm_content
				    , o.utm_medium
				    , o.utm_source
				    , o.utm_term
				    , i.row_total_with_discount AS grand_total
				    , i.cost * i.qty AS total_cost
				    , CASE WHEN shipment_type = 'shipment' AND qty_shipped = 0 THEN 0
				           WHEN shipment_type = 'rma' AND qty_returned = 0 THEN 0 
				           ELSE grand_total - total_cost
				           END AS margin_rub -- скидка
				    , total_cost AS real_cost
				    , total_cost AS fifo_cost
				    , o.payment_method AS invoice_method
				    , TRIM(o.store_name) as store_name -- поле для выдачи доступов
				    , o.delivery_date
				FROM items_with_shipment_dateparts as i
				LEFT JOIN aristos.orders AS o FINAL
				ON		i.project = o.project 
				    AND	i.order_id = o.entity_id
				WHERE entity_id IS NOT NULL    
		     )
		SELECT i.*
		     , d.week_from_to as delivery_week_from_to
		     , d.yearweek as delivery_yearweek
		     , d.yearmonth as delivery_yearmonth
		     , d.yearquarter as delivery_yearquarter
		     , d.month_year as delivery_month_year
		     , d.year as delivery_year
		     , now() as updated_at
		FROM items_with_orders i
		LEFT JOIN dictionary.datepart d
		ON i.delivery_date = d.date                  
        """
        
        fields =  {
            'project':'String',
            'order_item_id':'String',
            'shipment_item_id':'String',
            'attribute_set':'Nullable(String)',
            'business_group':'Nullable(String)',
            'is_gift':'String',
            'item_rule_ids':'Nullable(String)',
            'manufacturer':'String',
            'name':'String',
            'order_id':'String',
            'parent_group_product_sku':'Nullable(String)',
            'price':'Decimal(12,2)',
            'product_id':'String',
            'qty_returned':'UInt16',
            'qty_shipped':'UInt16',
            'retail_price':'Decimal(18,2)',
            'related_order_increment_id':'String',
            'sku':'String',
            'special_price':'Decimal(18,2)',
            'shipment_type':'String',
            'qty':'UInt16',
            'shipped_qty':'UInt16',
            'cost':'Decimal(18,2)',
            'row_total':'Decimal(18,2)',
            'row_total_with_discount':'Decimal(18,2)',
            'order_created_at':'Date',            
            'shipment_created_at':'Nullable(Date)',
            'order_week_from_to':'String',
            'order_yearweek':'String',
            'order_yearmonth':'String',
            'order_yearquarter':'String',
            'order_month_year':'String',
            'order_year':'String',
            'shipment_week_from_to':'Nullable(String)',
            'shipment_yearweek':'Nullable(String)',
            'shipment_yearmonth':'Nullable(String)',
            'shipment_yearquarter':'Nullable(String)',
            'shipment_month_year':'Nullable(String)',
            'shipment_year':'Nullable(String)',
            'city':'Nullable(String)',
            'complete_date':'Nullable(String)',
            'country_id':'Nullable(String)',
            'coupon_code':'Nullable(String)',
            'customer_group':'Nullable(String)',
            'customer_id':'String',
            'ext_type':'String',
            'last_order_id':'String',
            'partner_id':'Nullable(String)',
            'payment_method':'Nullable(String)',
            'region':'Nullable(String)',
            'rule_ids':'Nullable(String)',
            'shipping_description':'Nullable(String)',
            'shipping_method':'Nullable(String)',
            'shipping_type':'Nullable(String)',
            'staff_id':'Nullable(String)',
            'status':'String',
            'store_id':'Nullable(String)',
            'tags_customer':'Nullable(String)',
            'tags_order':'Nullable(String)',
            'utm_campaign':'Nullable(String)',
            'utm_content':'Nullable(String)',
            'utm_medium':'Nullable(String)',
            'utm_source':'Nullable(String)',
            'utm_term':'Nullable(String)',
            'grand_total':'Decimal(18,2)',
            'total_cost':'Decimal(18,2)',
            'margin_rub':'Decimal(18,2)',
            'real_cost':'Decimal(18,2)',
            'fifo_cost':'Decimal(18,2)',
            'invoice_method':'Nullable(String)',
            'store_name':'String',
            'delivery_date':'Nullable(Date)',
            'delivery_week_from_to':'Nullable(String)',
            'delivery_yearweek':'Nullable(String)',
            'delivery_yearmonth':'Nullable(String)',
            'delivery_yearquarter':'Nullable(String)',
            'delivery_month_year':'Nullable(String)',
            'delivery_year':'Nullable(String)',
            'updated_at':'Date'
        }
        
        def create_table(fields):
            """
            Проверяем на наличие готовой таблицы и создаем новую при необходимости.
            Сперва формируем запрос - для этого берем поля из fields, а также уникальные поля и поле с датой обновления
        
            :param fields: Преобразованные под Clickhouse поля из загружаемого датафрейма
            """
            unique_fields_str = ', '.join(map(str, unique_fields))   
            string = ''
            for i, field in enumerate(fields):
                string += '`{}` {}'.format(field, fields[field])
                if i < len(fields) - 1: string += ', ' 
            query = 'CREATE TABLE IF NOT EXISTS {0}.{1} ({2}) ENGINE = ReplacingMergeTree({3}, ({4}), 8192)'                    .format(db, table, string, update_field, unique_fields_str)
        
            try: read_clickhouse(query, connection = connection)
            except KeyError: print('Таблица создана')
        
        def truncate_table():
            query = 'TRUNCATE TABLE IF EXISTS {0}.{1} '.format(db, table)
            try: read_clickhouse(query, connection = connection)
            except KeyError: print('Таблица очищена')
        
        def insert_to_table(fields):
            """
            Добавляем данные в таблицу
        
            :param fields: Преобразованные под Clickhouse поля из загружаемого датафрейма
            """
            # INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
            string = ''
            for i, field in enumerate(fields):
                string += '{}'.format(field)
                if i < len(fields) - 1: string += ', ' 
            query = 'INSERT INTO {0}.{1} '.format(db, table) + sql_query
        
            try: read_clickhouse(query, connection = connection)
            except KeyError: print('Данные загружены')
        
        create_table(fields)
        truncate_table()
        insert_to_table(fields)
	
    t1 = PythonOperator(
        task_id='create_sir_dataset',
        python_callable = create_sir_dataset
    )

    t1

