with
    fonte_subcategorias_produtos as (
        select 
            cast(productsubcategoryid as int) as id_subcategoria
            ,cast (productcategoryid as int) as id_categoria
            ,cast (name as string) as nome_subcategoria
            --, rowguid
            --, modifieddate
        from {{ source('erp adventure works', 'production_productsubcategory') }}
    )

 select * 
 from fonte_subcategorias_produtos   