with
    fonte_detalhe_pedidos as (
        select 
            cast(salesorderid as integer) as id_pedido
            ,cast(salesorderdetailid as integer) as id_detalhe_pedido
            ,cast(orderqty as numeric) as qtdade_pedido
            ,cast(productid as integer) as id_produto
            ,cast(specialofferid as integer) as id_oferta_especial
            ,cast(unitprice as numeric) as preco_unitario
            ,cast(unitpricediscount as numeric) as desconto_unitario
            ,cast(salesorderdetailid as integer) as id_detalhe__pedido
            --,carriertrackingnumber
            --,rowguid
            --,modifieddate
           
        from {{ source('erp adventure works', 'sales_salesorderdetail') }}
    )

 select * 
 from fonte_detalhe_pedidos

