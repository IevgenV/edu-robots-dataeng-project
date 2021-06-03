--
-- Create table `clients`
--

DROP TABLE public.clients;
CREATE TABLE public.clients (
    client_id integer NOT NULL,
    fullname character varying(63),
    location_area_id integer,
    PRIMARY KEY (client_id)
);

--
-- Create table `location_areas`
--

DROP TABLE public.location_areas;
CREATE TABLE public.location_areas (
    area_id integer NOT NULL,
    area character varying(64),
    PRIMARY KEY (area_id)
);

--
-- Create table `orders`
--

DROP TABLE public.orders;
CREATE TABLE public.orders (
    product_id integer,
    client_id integer,
    order_date date,
    store_id integer,
    quantity integer,
    PRIMARY KEY (product_id, client_id, order_date, store_id)
);

--
-- Create table 'products'
--

DROP TABLE public.products;
CREATE TABLE public.products (
    product_id integer NOT NULL,
    product_name character varying(255),
    aisle character varying(127),
    department character varying(127),
    PRIMARY KEY (product_id)
);

--
-- Create table `stores`
--

DROP TABLE public.stores;
CREATE TABLE public.stores (
    store_id integer NOT NULL,
    location_area_id smallint,
    store_type character varying(64),
    PRIMARY KEY (store_id)
);

--
-- Create table `dates`
--

DROP TABLE public.dates;
CREATE TABLE public.dates (
    order_date date NOT NULL,
    date_week integer NOT NULL,
    date_month integer NOT NULL,
    date_year integer NOT NULL,
    date_weekday integer NOT NULL,
    PRIMARY KEY (order_date)
);

--
-- Create table `dates`
--

DROP TABLE public.out_of_stock;
CREATE TABLE public.out_of_stock (
    product_id int NOT NULL,
    order_date date NOT NULL,
    store_id integer,
    PRIMARY KEY (product_id, order_date)
);