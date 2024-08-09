with
    fonte_cliente as (
        select  

        cast(customerid as integer) as id_cliente
        ,cast(personid as integer) as id_pessoa

        from {{ source('erp adventure works', 'sales_customer') }}
    )

 select * 
 from fonte_cliente    