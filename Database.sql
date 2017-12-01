------------
-- TABLES --
------------
--images
CREATE TABLE public.images (
	"ID" varchar NOT NULL DEFAULT nextval('user_id_seq'::regclass),
	manifest varchar NOT NULL,
	file_name varchar NOT NULL,
	checksum uuid NULL,
	ordernumber int8 NOT NULL,
	ordercreated timestamp NOT NULL,
	orderexpiration timestamp NOT NULL,
	status varchar NOT NULL,
	file_size int8 NOT NULL,
	noaaid int8 NOT NULL,
	CONSTRAINT images_pk PRIMARY KEY (ordernumber,file_name)
)
WITH (
	OIDS=FALSE
) ;

--orders
CREATE TABLE public.orders (
	user_id int2 NOT NULL DEFAULT nextval('user_id_seq'::regclass),
	ordernumber int8 NOT NULL,
	status bpchar(12) NOT NULL,
	server varchar NOT NULL,
	notice varchar NULL,
	manifest varchar NULL,
	directory varchar NULL,
	manifesttotal int4 NULL,
	CONSTRAINT ordernumberunique UNIQUE (ordernumber)
)
WITH (
	OIDS=FALSE
) ;

-----------
-- VIEWS --
-----------
-- deleteorder
CREATE OR REPLACE VIEW public.deleteorder AS
 SELECT orders.ordernumber,
    orders.notice,
    orders.status,
    orders.directory
   FROM orders;

--downloadimages
CREATE OR REPLACE VIEW public.downloadimages AS
 SELECT i.ordernumber,
    i.file_name AS name,
    o.directory AS destination,
    o.server,
    i.checksum
   FROM images i
     LEFT JOIN orders o ON i.ordernumber = o.ordernumber
  WHERE i.status::text <> 'FINISHED'::text
  ORDER BY o.ordernumber, i.orderexpiration;

--getmanifest
CREATE OR REPLACE VIEW public.getmanifest AS
 SELECT orders.ordernumber,
    orders.server,
    orders.directory
   FROM orders
  WHERE orders.status = 'NEW'::bpchar AND orders.directory IS NOT NULL;

--imagesummary
CREATE OR REPLACE VIEW public.imagesummary AS
 SELECT i."ID",
    i.noaaid AS "NOAA ID",
    i.file_name AS "Filename",
    i.status AS "Status",
    round(i.file_size::numeric / power(1024::double precision, 2::double precision)::numeric, 2) AS "MB"
   FROM images i;

--overview
CREATE OR REPLACE VIEW public.overview AS
 SELECT o.ordernumber AS "Ordernumber",
    o.status AS "Status",
    count(i.ordernumber) FILTER (WHERE i.status::text = 'NEW'::text) AS "New",
    count(i.ordernumber) FILTER (WHERE i.status::text = 'FINISHED'::text) AS "Downloaded",
    count(i.ordernumber) FILTER (WHERE i.status::text = 'ERROR'::text) AS "Error",
    count(i.ordernumber) AS "Total",
    round((sum(i.file_size)::double precision / power(1024::double precision, 3::double precision))::numeric, 2) AS "GB",
    o.directory AS "Destination"
   FROM orders o
     LEFT JOIN images i ON i.ordernumber = o.ordernumber
  GROUP BY o.ordernumber, o.status, o.directory
  ORDER BY (count(i.ordernumber));

--processmanifest
CREATE OR REPLACE VIEW public.processmanifest AS
 SELECT orders.ordernumber,
    orders.directory,
    orders.manifest
   FROM orders
  WHERE orders.status = 'MANIFEST'::bpchar;

----------------
-- PROCEDURES --
----------------
--ordercomplete
CREATE OR REPLACE FUNCTION public.ordercomplete(n bigint)
 RETURNS boolean
 LANGUAGE plpgsql
AS $function$
BEGIN
if (select manifesttotal from orders where ordernumber = $1) = (select count(I.ordernumber)::integer
 from images as I
 where ordernumber = $1 and I.status = 'FINISHED'
 group by ordernumber) 
THEN
   return true;
ELSE 
   RETURN false;
END IF;
end;
$function$;


