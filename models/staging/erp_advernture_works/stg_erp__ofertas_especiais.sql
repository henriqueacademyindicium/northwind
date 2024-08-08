with
    fonte_ofertas_especiais as (
        select 
        cast(specialofferid as integer) as id_oferta_especial
        ,cast(description as string) as nome_oferta_especial
        ,cast(discountpct as numeric) as porcentagem_desconto
        ,cast(type as string) as tipo_oferta_especial
        ,cast(category as string) as categoria_oferta_especial
        
        --startdate
        --enddate
        --minqty
        --maxqty
        --rowguid
        --modifieddate
            
        from {{ source('erp adventure works', 'sales_specialoffer') }}
    )

 select *
 from fonte_ofertas_especiais