with stg_orders as (
    select
        OrderID,  
        {{ dbt_utils.generate_surrogate_key(['employeeid']) }} as employeekey,
        {{ dbt_utils.generate_surrogate_key(['customerid']) }} as customerkey,
        replace(to_date(orderdate)::varchar,'-','')::int as orderdatekey
    from {{source('northwind','Orders')}}
),
stg_order_details as (
    select
        orderid,
        {{ dbt_utils.generate_surrogate_key(['productid']) }} as productkey,
        productid,
        quantity,
        unitprice,
        discount,
        quantity * unitprice * (1-discount) as extendedprice
    from {{source('northwind','Order_Details')}}
)
select  
    o.orderid,
    od.productkey,
    od.productid,
    o.customerkey,
    o.employeekey,
    o.orderdatekey,
    od.quantity,
    od.unitprice,
    od.discount,
    od.extendedprice
from stg_order_details od
    join stg_orders o on od.orderid = o.orderid