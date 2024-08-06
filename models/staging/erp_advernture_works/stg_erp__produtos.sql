with
    fonte_produtos as (
        select 
            cast (productid as int) as id_produto
            , cast(name as string) as nome_produto
            --,productnumber
            , case 
                when makeflag = false then 'nao'
                else 'sim'  
            end as eh_manufaturado    
            , case 
                when finishedgoodsflag = false then 'nao'
                else 'sim'  
            end as eh_produto_final
            --,color
            --,safetystocklevel
            --,reorderpoint
            --,standardcost
            --,listprice
            --,size
            --,sizeunitmeasurecode
            --,weightunitmeasurecode
            --,weight
            --,daystomanufacture
            --,productline

        from {{ source('erp adventure works', 'production_product') }}
    )

 select * 
 from fonte_produtos