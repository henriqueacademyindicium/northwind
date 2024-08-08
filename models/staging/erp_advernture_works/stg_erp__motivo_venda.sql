with
    fonte_motivo_vendas as (
        select 
            cast (salesreasonid as int) as id_motivo_venda
            ,cast (name as string) as nome_motivo_venda
            ,cast (reasontype as string) as tipo_motivo_venda
            --,modifieddate

        from {{ source('erp adventure works', 'sales_salesreason') }}
    )

 select * 
 from fonte_motivo_vendas