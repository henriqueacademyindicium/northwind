with
    fonte_businessentity_adress as (
        select  
        cast(businessentityid as integer) as id_businessentity
        ,cast (addressid as integer) as id_endereco
        , addresstypeid 
        ,rowguid
        ,modifieddate
        from {{ source('erp adventure works', 'person_businessentityaddress') }}
    )

    select  *
    from fonte_businessentity_adress
    where addresstypeid = 2
    -- estamos considerando apenas a casa dos clientes (adresstype = 2)
    

 --5976
 --996
 --1072
 --0550
 --124
 --7298
 --6746

 --select  id_businessentity
 --from x   
 --group by id_businessentity
 --having count(*) > 1