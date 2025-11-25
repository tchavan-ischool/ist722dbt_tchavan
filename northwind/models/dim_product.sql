with stg_products as (
    select * from {{ source('northwind','Products') }}
),
stg_categories as (
    select * from {{ source('northwind','Categories') }}
),
stg_suppliers as (
    select * from {{ source('northwind','Suppliers') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['p.productid']) }} as productkey,
    p.productid,
    p.productname,
    p.quantityperunit,
    p.unitprice,
    p.unitsinstock,
    p.unitsonorder,
    p.reorderlevel,
    p.discontinued,
    c.categoryid,
    c.categoryname,
    c.description as categorydescription,
    s.supplierid,
    s.companyname as suppliercompanyname,
    s.contactname as suppliercontactname,
    s.contacttitle as suppliercontacttitle,
    s.city as suppliercity,
    s.region as supplierregion,
    s.postalcode as supplierpostalcode,
    s.country as suppliercountry,
    s.phone as supplierphone
from stg_products p
    left join stg_categories c on p.categoryid = c.categoryid
    left join stg_suppliers s on p.supplierid = s.supplierid