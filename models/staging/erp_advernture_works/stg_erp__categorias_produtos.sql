with
    fonte_categorias_produtos as (
        select 
            cast (productcategoryid as int) as id_categoria
            ,cast (name as string) as nome_categoria
            --,rowguid
            --,modifieddate
        from {{ source('erp adventure works', 'production_productcategory') }}
    )

 select * 
 from fonte_categorias_produtos     