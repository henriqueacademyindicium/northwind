with
    fonte_businessentity_adress as (
        select  
        cast(businessentityid as integer) as id_businessentity
        ,cast (addressid as integer) as id_endereco
        --addresstypeid
        --rowguid
        --modifieddate
        from {{ source('erp adventure works', 'person_businessentityaddress') }}
    )

 select * 
 from fonte_businessentity_adress    