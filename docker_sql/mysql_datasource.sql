-- Full-load reference table
-- 01.
DROP TABLE IF EXISTS olist_geolocation_dataset;
CREATE TABLE olist_geolocation_dataset (
    geolocation_zip_code_prefix int4,
    geolocation_lat float4,
    geolocation_lng float4,
    geolocation_city varchar(100),
    geolocation_state varchar(2)
);
INSERT INTO olist_geolocation_dataset (geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state) VALUES
	 (1037,-23.5456,-46.6393,'sao paulo','SP'),
	 (1046,-23.5461,-46.6448,'sao paulo','SP'),
	 (1046,-23.5461,-46.643,'sao paulo','SP'),
	 (1041,-23.5444,-46.6395,'sao paulo','SP'),
	 (1035,-23.5416,-46.6416,'sao paulo','SP'),
	 (1012,-23.5478,-46.6354,'são paulo','SP'),
	 (1047,-23.5463,-46.6412,'sao paulo','SP'),
	 (1013,-23.5469,-46.6343,'sao paulo','SP'),
	 (1029,-23.5438,-46.6343,'sao paulo','SP'),
	 (1011,-23.5476,-46.636,'sao paulo','SP');


-- Incremental by date time
-- 02.
DROP TABLE IF EXISTS olist_customers_dataset;
CREATE TABLE olist_customers_dataset (
    customer_id varchar(32),
    customer_unique_id varchar(32),
    customer_zip_code_prefix int4,
    customer_city varchar(100),
    customer_state varchar(2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (customer_id)
);
INSERT INTO olist_customers_dataset (customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state,created_at,updated_at) VALUES
	 ('00012a2ce6f8dcda20d059ce98491703','248ffe10d632bebe4f7267f1f44844c9',6273,'osasco','SP','2022-09-25 08:23:26','2022-09-10 00:00:00'),
	 ('000161a058600d5901f007fab4c27140','b0015e09bb4b6e47c52844fab5fb6638',35550,'itapecerica','MG','2022-09-25 08:23:25','2022-09-10 00:00:00'),
	 ('0001fd6190edaaf884bcaf3d49edf079','94b11d37cd61cb2994a194d11f89682b',29830,'nova venecia','ES','2022-09-25 08:23:26','2022-09-10 00:00:00'),
	 ('0002414f95344307404f0ace7a26f1d5','4893ad4ea28b2c5b3ddf4e82e79db9e6',39664,'mendonca','MG','2022-09-25 08:23:27','2022-09-10 00:00:00'),
	 ('000379cdec625522490c315e70c7a9fb','0b83f73b19c2019e182fd552c048a22c',4841,'sao paulo','SP','2022-09-25 08:23:26','2022-09-10 00:00:00'),
	 ('0004164d20a9e969af783496f3408652','104bdb7e6a6cdceaa88c3ea5fa6b2b93',13272,'valinhos','SP','2022-09-25 08:23:25','2022-09-10 00:00:00'),
	 ('000419c5494106c306a97b5635748086','14843983d4a159080f6afe4b7f346e7c',24220,'niteroi','RJ','2022-09-25 08:23:26','2022-09-10 00:00:00'),
	 ('00046a560d407e99b969756e0b10f282','0b5295fc9819d831f68eb0e9a3e13ab7',20540,'rio de janeiro','RJ','2022-09-25 08:23:27','2022-09-10 00:00:00'),
	 ('00050bf6e01e69d5c0fd612f1bcfb69c','e3cf594a99e810f58af53ed4820f25e5',98700,'ijui','RS','2022-09-25 08:23:26','2022-09-10 00:00:00'),
	 ('000598caf2ef4117407665ac33275130','7e0516b486e92ed3f3afdd6d1276cfbd',35540,'oliveira','MG','2022-09-25 08:23:25','2022-09-10 00:00:00');


-- Incremental by watermarks
-- 06.
DROP TABLE IF EXISTS olist_orders_dataset;
CREATE TABLE olist_orders_dataset (
    order_id varchar(32),
    customer_id varchar(32),
    order_status varchar(16),
    order_purchase_timestamp varchar(32),
    order_approved_at varchar(32),
    order_delivered_carrier_date varchar(32),
    order_delivered_customer_date varchar(32),
    order_estimated_delivery_date varchar(32),
    PRIMARY KEY(order_id)
);
INSERT INTO olist_orders_dataset (order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,order_estimated_delivery_date) VALUES
	 ('00010242fe8c5a6d1ba2dd792cb16214','3ce436f183e68e07877b285a838db11a','delivered','2017-09-13 08:59:02','2017-09-13 09:45:35','2017-09-19 18:34:16','2022-09-20 23:43:48','2017-09-29 00:00:00'),
	 ('00018f77f2f0320c557190d7a144bdd3','f6dd3ec061db4e3987629fe6b26e5cce','delivered','2017-04-26 10:53:06','2017-04-26 11:05:13','2017-05-04 14:35:00','2022-05-12 16:04:24','2017-05-15 00:00:00'),
	 ('000229ec398224ef6ca0657da4fc703e','6489ae5e4333f3693df5ad4372dab6d3','delivered','2018-01-14 14:33:31','2018-01-14 14:48:30','2018-01-16 12:36:48','2022-01-22 13:19:16','2018-02-05 00:00:00'),
	 ('00024acbcdf0a6daa1e931b038114c75','d4eb9395c8c0431ee92fce09860c5a06','delivered','2018-08-08 10:00:35','2018-08-08 10:10:18','2018-08-10 13:28:00','2022-08-14 13:32:39','2018-08-20 00:00:00'),
	 ('00042b26cf59d7ce69dfabb4e55b4fd9','58dbd0b2d70206bf40e62cd34e84d795','delivered','2017-02-04 13:57:51','2017-02-04 14:10:13','2017-02-16 09:46:09','2022-03-01 16:42:31','2017-03-17 00:00:00'),
	 ('00048cc3ae777c65dbb7d2a0634bc1ea','816cbea969fe5b689b39cfc97a506742','delivered','2017-05-15 21:42:34','2017-05-17 03:55:27','2017-05-17 11:05:55','2022-05-22 13:44:35','2017-06-06 00:00:00'),
	 ('00054e8431b9d7675808bcb819fb4a32','32e2e6ab09e778d99bf2e0ecd4898718','delivered','2017-12-10 11:53:48','2017-12-10 12:10:31','2017-12-12 01:07:48','2022-12-18 22:03:38','2018-01-04 00:00:00'),
	 ('000576fe39319847cbb9d288c5617fa6','9ed5e522dd9dd85b4af4a077526d8117','delivered','2018-07-04 12:08:27','2018-07-05 16:35:48','2018-07-05 12:15:00','2022-07-09 14:04:07','2018-07-25 00:00:00'),
	 ('0005a1a1728c9d785b8e2b08b904576c','16150771dfd4776261284213b89c304e','delivered','2018-03-19 18:40:33','2018-03-20 18:35:21','2018-03-28 00:37:42','2022-03-29 18:17:31','2018-03-29 00:00:00'),
	 ('0005f50442cb953dcd1d21e1fb923495','351d3cb2cee3c7fd0af6616c82df21d3','delivered','2018-07-02 13:59:39','2018-07-02 14:10:56','2018-07-03 14:25:00','2022-07-04 17:28:31','2018-07-23 00:00:00');

DROP TABLE IF EXISTS olist_products_dataset;
CREATE TABLE olist_products_dataset (
    product_id varchar(32),
    product_category_name varchar(64),
    product_name_lenght int4,
    product_description_lenght int4,
    product_photos_qty int4,
    product_weight_g int4,
    product_length_cm int4,
    product_height_cm int4,
    product_width_cm int4,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (product_id)
);

CREATE TABLE olist_sellers_dataset (
    seller_id varchar(32),
    seller_zip_code_prefix int4,
    seller_city nvarchar(1000),
    seller_state varchar(2),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (seller_id)
);

DROP TABLE IF EXISTS olist_order_items_dataset;
CREATE TABLE olist_order_items_dataset (
    order_id varchar(32),
    order_item_id int4,
    product_id varchar(32),
    seller_id varchar(32),
    shipping_limit_date varchar(32),
    price float4,
    freight_value float4,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (order_id, order_item_id, product_id, seller_id),
    FOREIGN KEY (order_id) REFERENCES olist_orders_dataset(order_id),
    FOREIGN KEY (product_id) REFERENCES olist_products_dataset(product_id),
    FOREIGN KEY (seller_id) REFERENCES olist_sellers_dataset(seller_id)
);