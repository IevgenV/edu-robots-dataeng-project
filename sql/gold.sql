-- Table: city

--
-- Create table `clients`
--

CREATE TABLE public.clients (
    client_id integer NOT NULL,
    fullname character varying(63),
    location_area_id integer
);

--
-- Create table `location_areas`
--

CREATE TABLE public.location_areas (
    area_id integer NOT NULL,
    area character varying(64)
);

--
-- Create table `orders`
--

CREATE TABLE public.orders (
    product_id integer,
    client_id integer,
    order_date date,
    store_id integer,
    quantity integer,
);

--
-- Create table 'products'
--

CREATE TABLE public.products (
    product_id integer NOT NULL,
    product_name character varying(255),
    aisle character varying(127)
    department character varying(127)
);

--
-- Create table `stores`
--

CREATE TABLE public.stores (
    store_id integer NOT NULL,
    location_area_id smallint,
    store_type varying(64)
);

--
-- Create table `dates`
--

CREATE TABLE public.dates (
    order_date date NOT NULL,
    date_week integer NOT NULL,
    date_month integer NOT NULL,
    date_year integer NOT NULL,
    date_weekday integer NOT NULL
);

CREATE TABLE public.out_of_stock (
    product_id int NOT NULL,
    order_date date NOT NULL,
    store_id integer,
);