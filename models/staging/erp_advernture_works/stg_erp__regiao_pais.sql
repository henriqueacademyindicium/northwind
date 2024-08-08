with
    fonte_regiao_pais as (
        select 
            cast (countryregioncode as string) as id_regiao_pais
            ,cast(name as string) as nome_regiao_pais
        from {{ source('erp adventure works', 'person_countryregion') }}
    )

 select * 
 from fonte_regiao_pais   