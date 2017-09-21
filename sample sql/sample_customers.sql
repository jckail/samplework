--create table samplefinaldata as
select c.id as "Customer - id",
       case when c.gender = ''
         then null
       else c.gender end as "Customer - Gender",
       date_part('year',current_date::date) - date_part('year' ,c.birth_date::date) as "Customer - Age", --could also just get (current date - c.birth_date) / 365
       case when c.education != ''
         then c.education
       else null end as "Customer - Education",
       c.state as "Customer - State",
       ret.id as "Retailer - id",
       ret.retailer_type as "Retailer - Type",
       r.id as "Receipt - id",

       case when r.total_price is null  or  r.total_price > 1000
         then sum(  coalesce(case when ri.price < 100 then ri.price else null end,imputed_retailer_upc.avg_price,imputed_brand_category.avg_price,imputed_brand.avg_price, imputed_category.avg_price,imputed_retailer.avg_price,imputed_demographic.avg_price, imputed_gender.avg_price)) over (PARTITION BY r.id )
              when r.total_price is not null  and r.total_price::numeric < 1000 then r.total_price::numeric
       else null
       end as "Receipt - Total Price" ,

       case when r.total_price is null  or (r.total_price is null  and r.total_price::numeric > 1000)
         then TRUE
        when r.total_price is not null  then false
       end as "Receipt - Total Price Imputed FLAG",

       (r.created_at::TIMESTAMP AT TIME ZONE 'UTC') AT TIME ZONE 'MST' as "Receipt - Created at timestamp" , --denver is utc -7
       rid.receipt_item_id as "Receipt item - id",
       (select pc.name from sample.public.productcategories pc where rid.primary_category_id = pc.id::text) as "Receipt item - Primary Category Name",
       (select pc.name from sample.public.productcategories pc  where rid.secondary_category_id = pc.id::text) as "Receipt item - Secondary Category Name",
       (select pc.name from sample.public.productcategories pc  where rid.tertiary_category_id = pc.id::text) as "Receipt item - Tertiary Category Name",


       b.name as "Receipt item - Brand Name",

       rid.global_product_id as "Receipt item - Product UPC",

       case when ri.price is null or ri.price > 1000 then   coalesce(imputed_retailer_upc.avg_price,imputed_brand_category.avg_price,imputed_brand.avg_price, imputed_category.avg_price,imputed_retailer.avg_price,imputed_demographic.avg_price, imputed_gender.avg_price)
       when ri.price is not null then ri.price
       else NULL end as "Receipt item - Price",

       case when ri.price is null or ri.price > 1000 then true
       when ri.price is not null then false
       else false end as "Receipt item - Price Imputed Flag",

       case when ri.quantity is null or ri.quantity > 50 then     coalesce(imputed_retailer_upc.avg_quantity,imputed_brand_category.avg_quantity,imputed_brand.avg_quantity, imputed_category.avg_quantity,imputed_retailer.avg_quantity,imputed_demographic.avg_quantity, imputed_gender.avg_quantity)
       when ri.quantity is not null then ri.quantity
       else NULL end as "Receipt item - Quantity",

       case when ri.quantity is null or ri.quantity > 50 then TRUE
       when ri.quantity is not null then false
       else NULL end as "Receipt item - Quantity Imputed Flag"

from sample.public.receipts r
  left join sample.public.customers c on c.id = r.customer_id
  left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
  left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
  left join sample.public.brands b on b.id::text = rid.brand_id
  left join sample.public.retailers ret on ret.id::text = r.retailer_id

  left join sample.public.imputed_retailer_upc on imputed_retailer_upc.retailer_id =r.retailer_id and imputed_retailer_upc.global_product_id = rid.global_product_id --ddl for view attached
  left join sample.public.imputed_brand_category on imputed_brand_category.brand_id = rid.brand_id and imputed_brand_category.primary_category_id = rid.primary_category_id
  left join imputed_brand on imputed_brand.brand_id = rid.brand_id and imputed_brand.gender = c.gender and imputed_brand.retailer_type = ret.retailer_type
  left join imputed_category on imputed_category.primary_category_id = rid.primary_category_id and imputed_category.gender = c.gender and imputed_category.retailer_type = ret.retailer_type
  left join imputed_retailer on imputed_retailer.retailer_id = r.retailer_id and imputed_retailer.gender = c.gender
  left join imputed_demographic on imputed_demographic.gender = c.gender and imputed_demographic.education = c.education and imputed_demographic.state = c.state
  left join imputed_gender on imputed_gender.gender = c.gender

order by c.id, r.id
;

--update dml for sanity
begin;
update sample.public.receiptitems
set price =  NULL
where price = '(null)';
update sample.public.receiptitems
set quantity =  NULL
where quantity = '(null)';
update sample.public.receipts
set total_price = NULL
where total_price = '(null)'
--commit;


ALTER TABLE public.receipts ALTER COLUMN total_price TYPE NUMERIC USING total_price::NUMERIC;
ALTER TABLE public.receiptitems ALTER COLUMN price TYPE NUMERIC USING price::NUMERIC;
ALTER TABLE public.receiptitems ALTER COLUMN quantity TYPE NUMERIC USING quantity::NUMERIC;

create view imputed_retailer_upc as

  select distinct
    r.retailer_id, rid.global_product_id, pc.name,
    coalesce(avg(case when ri.price - mp.mode_price < 10 then ri.price
                 when ri.price - mp.mode_price  > 10 then mode_price
                 end) over (PARTITION BY r.retailer_id,rid.global_product_id),
             avg(case when ri.price - mp.mode_price < 10 then ri.price
                 when ri.price - mp.mode_price  > 10 then mode_price
                 end) over (PARTITION BY rid.global_product_id),
             avg(case when ri.price - mp.mode_price < 10 then ri.price
                 when ri.price - mp.mode_price  > 10 then mode_price
                 end) over (PARTITION BY r.retailer_id),mode_price) avg_price,

    coalesce(avg(case when ri.quantity - mp.mode_quantity < 10 then ri.price
                 when ri.quantity - mp.mode_quantity  > 10 then mode_quantity
                 end) over (PARTITION BY r.retailer_id,rid.global_product_id),
             avg(case when ri.quantity - mp.mode_quantity < 10 then ri.price
                 when ri.quantity - mp.mode_quantity  > 10 then mode_quantity
                 end) over (PARTITION BY rid.global_product_id),
             avg(case when ri.quantity - mp.mode_quantity < 10 then ri.price
                 when ri.quantity - mp.mode_quantity  > 10 then mode_quantity
                 end) over (PARTITION BY r.retailer_id),mode_quantity) avg_quantity

  from sample.public.receipts r
    left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
    left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
    left join sample.public.retailers ret on ret.id::text = r.retailer_id
    left join sample.public.mode_price mp on mp.primary_category_id = rid.primary_category_id
    left join productcategories pc on pc.id::text = rid.primary_category_id
  where  1 = 1
         and global_product_id is not null
         and rid.primary_category_id is not null


create  view   imputed_brand_category as
  select distinct  rid.brand_id, primary_category_id ,avg(ri.price::numeric) avg_price, avg(ri.quantity) avg_quantity
  from sample.public.receipts r
    left join sample.public.customers c on c.id = r.customer_id
    left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
    left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
    left join sample.public.brands b on b.id::text = rid.brand_id
    left join sample.public.retailers ret on ret.id::text = r.retailer_id
  where  1 = 1
         and ri.price is not null
         and ri.quantity is not null
  group by rid.brand_id, primary_category_id;

create  view   imputed_brand as
  select distinct c.gender,ret.retailer_type , rid.brand_id, avg(ri.price::numeric) avg_price, avg(ri.quantity) avg_quantity
  from sample.public.receipts r
    left join sample.public.customers c on c.id = r.customer_id
    left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
    left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
    left join sample.public.brands b on b.id::text = rid.brand_id
    left join sample.public.retailers ret on ret.id::text = r.retailer_id
  where  1 = 1
         and ri.price is not null
         and ri.quantity is not null
  group by c.gender,ret.retailer_type , rid.brand_id;

create  view   imputed_category as
  select distinct  c.gender,ret.retailer_type ,primary_category_id ,avg(ri.price::numeric) avg_price, avg(ri.quantity) avg_quantity
  from sample.public.receipts r
    left join sample.public.customers c on c.id = r.customer_id
    left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
    left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
    left join sample.public.brands b on b.id::text = rid.brand_id
    left join sample.public.retailers ret on ret.id::text = r.retailer_id
  where  1 = 1
         and ri.price is not null
         and ri.quantity is not null
  group by c.gender,ret.retailer_type ,primary_category_id;

create view imputed_retailer as
  select distinct  c.gender,retailer_id, avg(ri.price::numeric) avg_price, avg(ri.quantity::numeric) avg_quantity
  from sample.public.receipts r
    left join sample.public.customers c on c.id = r.customer_id
    left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
    left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
    left join sample.public.brands b on b.id::text = rid.brand_id
    left join sample.public.retailers ret on ret.id::text = r.retailer_id

  where  1 = 1
         and ri.price is not null
         and ri.quantity is not null
  group by c.gender,r.retailer_id;

create view imputed_demographic as
  select
    distinct
    c.gender,
    c.education ,
    c.state
    ,avg(ri.price::numeric) avg_price, avg(ri.quantity)avg_quantity

  from sample.public.receipts r
    left join sample.public.customers c on c.id = r.customer_id
    left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
    left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
    left join sample.public.brands b on b.id::text = rid.brand_id
    left join sample.public.retailers ret on ret.id::text = r.retailer_id
  where  1 = 1
         and ri.price is not null
         and ri.quantity is not null

  group by

    c.gender,
    c.education ,
    c.state ;


create view imputed_gender as
  select
    distinct
    c.gender
    ,avg(ri.price::numeric) avg_price, avg(ri.quantity)avg_quantity

  from sample.public.receipts r
    left join sample.public.customers c on c.id = r.customer_id
    left join sample.public.receiptitemdetails rid on rid.receipt_id = r.id
    left join sample.public.receiptitems ri on ri.receipt_item_id = rid.receipt_item_id
    left join sample.public.brands b on b.id::text = rid.brand_id
    left join sample.public.retailers ret on ret.id::text = r.retailer_id
  where  1 = 1
         and ri.price is not null
         and ri.quantity is not null

  group by

    c.gender;

select * from sample.public.receipts;

--answer to question 1
select customer_id, count(distinct id) count_of_receipts  from sample.public.receipts
group by customer_id
order by 2 desc;

--answer to question 2
select distinct customer_id from sample.public.receipts
group by customer_id
having count (distinct id) >= 3 -- "three or more"
order by 1 desc;

--I noticed '(null)' vs NULL (type) in the data updating to correctly use max function
--begin;
--update sample.public.receipts
--set total_price = NULL
--where total_price = '(null)';
--select * from sample.public.receipts
--where total_price is NULL;

--if count in query anticipated then commit; else rollback;

--answer to question 3
select customer_id, max(total_price) from sample.public.receipts
group by customer_id
order by 1 desc;

--answer to question 4
create table if not EXISTS  question4answer as -- doing this only for the metabase dashboard
  select distinct customer_id, array(select distinct retailer_id from sample.public.receipts a2 where a1.customer_id = a2.customer_id) retail_ids
  from sample.public.receipts a1
  order by 1 desc;

select * from question4answer
