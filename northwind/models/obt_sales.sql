with f_sales as (
    select * from {{ ref('fact_sales') }}
),
d_customer as (
    select * from {{ ref('dim_customer') }}
),
d_employee as (
    select * from {{ ref('dim_employee') }}
),
d_product as (
    select * from {{ ref('dim_product') }}
),
d_date as (
    select * from {{ ref('dim_date') }}
)
select
    -- Customer dimension
    d_customer.*,
    -- Employee dimension
    d_employee.*,
    -- Product dimension (excluding productkey to avoid duplication)
    d_product.productid,
    d_product.productname,
    d_product.quantityperunit,
    d_product.unitsinstock,
    d_product.unitsonorder,
    d_product.reorderlevel,
    d_product.discontinued,
    d_product.categoryid,
    d_product.categoryname,
    d_product.categorydescription,
    d_product.supplierid,
    d_product.suppliercompanyname,
    d_product.suppliercontactname,
    d_product.suppliercontacttitle,
    d_product.suppliercity,
    d_product.supplierregion,
    d_product.supplierpostalcode,
    d_product.suppliercountry,
    d_product.supplierphone,
    -- Date dimension
    d_date.*,
    -- Fact table measures
    f.orderid,
    f.orderdatekey,
    f.quantity,
    f.unitprice,
    f.discount,
    f.extendedprice
from f_sales as f
    left join d_customer on f.customerkey = d_customer.customerkey
    left join d_employee on f.employeekey = d_employee.employeekey
    left join d_product on f.productkey = d_product.productkey
    left join d_date on f.orderdatekey = d_date.datekey