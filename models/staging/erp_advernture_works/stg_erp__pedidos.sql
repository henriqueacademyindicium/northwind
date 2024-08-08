with
    fonte_pedidos as (
        select 
            cast (salesorderid as integer) as id_pedido
            --, revisionnumber
            ,cast (orderdate as date) as data_pedido
            ,cast (duedate as date) as data_entrega
            --, shipdate
            ,cast (status as integer) as id_status
            ,case 
                when onlineorderflag = false then 'nao'
                else 'sim'  
            end as eh_compra_online   
            --, purchaseordernumber
            --, accountnumber
            ,cast (customerid as integer) as id_cliente
            --, salespersonid
            --, territoryid
            --, billtoaddressid
            --, shiptoaddressid
            --, shipmethodid
            ,cast (creditcardid as integer) as id_cartao_de_credito
            --, creditcardapprovalcode
            --, currencyrateid
            ,cast (subtotal as numeric) as subtotal
            ,cast (taxamt as numeric) as taxas
            ,cast (freight as numeric) as frete
            ,cast (totaldue as numeric) as total
            --, comment
            --, rowguid
            --, modifieddate

        from {{ source('erp adventure works', 'sales_salesorderheader') }}
    )

 select * 
 from fonte_pedidos    