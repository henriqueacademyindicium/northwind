with
    fonte_estado_provincia as (
        select 
        cast(stateprovinceid as integer) as id_provincia_estado
        --,stateprovincecode 
        ,cast(countryregioncode as string) as id_regiao_pais
        ,case 
                when isonlystateprovinceflag = false then 'nao'
                else 'sim'  
            end as eh_apenas_pais
        ,cast(name as string) as nome_estado_provincia
        --,territoryid
        --,rowguid
        --,modifieddate
        from {{ source('erp adventure works', 'person_stateprovince') }}
    )

 select * 
 from fonte_estado_provincia  
