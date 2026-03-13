/*
For data security concerns, I can not upload the full dataset !!
*/


create database augmedix_3;

use augmedix_3;

create table ehr(
patient text,
provider text,
date_ehr text,
cpt text
);

create table db(
patient text,
provider text,
date_db date,
cpt text
);

load data local infile '/Users/sazid/Documents/Augmedix/2nd task/Retool Case _ RCM/Ops Case Study Dataset - Sample EHR Data (2).csv'
INTO TABLE ehr
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


load data local infile '/Users/sazid/Documents/Augmedix/2nd task/Retool Case _ RCM/Ops Case Study Dataset - Sample DB Data (2).csv'
INTO TABLE db
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;




update ehr
set date_ehr=str_to_date(date_ehr,'%m/%d/%Y')
where date_ehr is not null 
 and date_ehr <> '';

select  distinct cpt from ehr_;
update db
set cpt = regexp_replace(cpt,'[{}]','');



select * from ehr_;
select distinct * from db;

create table errorr as
with recursive cte as(
select patient,provider,date_ehr, group_concat(trim(regexp_replace(cpt, '[^0-9a-zA-Z]','')) separator ',' ) as cpt from ehr
where date_ehr is not null and date_ehr<>''
group by 1,2,3),cte0 as(
select  patient, provider, date_db, group_concat(trim(regexp_replace(cpt, '[^0-9a-zA-Z,]','')) separator ',' ) as cpt  from db
group by 1,2,3
),cte1 as(
select cte.patient,cte.provider,cte.date_ehr,cte.cpt as cpt from cte left join cte0
on cte.patient = cte0.patient and
cte.provider=cte0.provider and
cte.date_ehr = cte0.date_db
where cte0.patient is null), cte2 as(
select * from cte1
), cte3 as(
select 
substring_index(cpt,',',1) as cptt,
substring(cpt, length(substring_index(cpt,',',1))+2) as rest
from cte2

union all

select 
substring_index(rest,',',1) as cptt,
substring(rest, length(substring_index(rest,',',1))+2) as rest
from cte3 where rest<>''
)select cpt from cte2
;



select * from accepted_details;
select * from reject_details;
select distinct provider from db_;
select * from ehr_;

select distinct rej.patient,acpt.patient from reject_details as rej left join db_ as acpt 
on acpt.patient = rej.patient 
 where acpt.patient is null
;



create table reject_phase3 as
with cte as(
select   ehr_.patient, ehr_.provider, ehr_.date_ehr, ehr_.cpt from ehr_ left join db_ on
ehr_.patient = db_.patient and
ehr_.provider = db_.provider and 
ehr_.date_ehr = db_.date_db

where db_.patient is null)
select distinct cte.patient, cte.provider, cte.date_ehr, cte.cpt from cte left join reject_phase1 
on cte.patient = reject_phase1.patient and 
cte.provider = reject_phase1.provider and
cte.date_ehr = reject_phase1.date_ehr left join reject_phase2 on
cte.patient = reject_phase2.patient and 
cte.provider = reject_phase2.provider and
cte.date_ehr = reject_phase2.date_ehr
where reject_phase1.patient  is  null and reject_phase2.patient  is  null
;





select * from ehr_;
select * from db_;
select * from reject_details;
select * from reject_phase1;
select * from reject_phase2;
select * from reject_phase3;
select * from accepted_details;
select * from accepted_cpt;
select * from rejected_cpt;
select * from rejected_cpt_ratio;
select * from errorr;
select * from patient_reject_ratio;
select * from provider_reject_ratio;
select * from missing_ratio;




with recursive cte1 as(
select *, row_number() over() as id from errorr
),
cte as(
select 1 as pos,id,
trim(substring_index(cpt, ',',1)) as cpt,
trim(substring(cpt, length(substring_index(cpt, ',',1))+2)) as rest
from cte1

union all

select
 1+pos as pos,id,
trim(substring_index(rest, ',',1)) as cpt,
trim(substring(rest, length(substring_index(rest, ',',1))+2)) as rest
from cte
where rest <>''
), cte6 as( select cpt, count(cpt) as count from cte
group by 1
order by 2 desc)
select "Bad format CPT" as CPT, sum(
case when cpt regexp '[[:alpha:]]' and cpt not like '%G0283%' then count else 0 end) as String_count,
"Good CPT" as CPT1 , sum(
case when cpt regexp '^[0-9]+$' or cpt  like '%G0283%' then count else 0 end) as numeric_count from cte6
;

select * from errorr;




create table rejected_cpt_ratio as
select  'String Cpt' as cpt ,sum(
case when cpt  regexp '[[:alpha:]]' then 1 else 0 end
) as string_count, 'Numeric Cpt' as cpt1 ,sum(
case when cpt  regexp '^[0-9]+(,[ ]*[0-9]+)*$' then 1 else 0 end
) as numeric_count
 from errorr 
;






create table missing_ratio as
with cte as(
select count(*) as ehr_encounter from ehr_
), cte1 as(
select count(*) as accepted_encounter from accepted_details
) select ehr_encounter, accepted_encounter, ehr_encounter-accepted_encounter as missing_encounter, 
round((ehr_encounter-accepted_encounter)*100/ehr_encounter,2) as missing_ratio
 from cte, cte1;





patient:
Ethan Lopez
Isabella Thomas
Layla Miller
Mateo Thomas
Sebastian Jones

provider:



alter table reject_cpt 
add column cnt int;






#97032 97163 97530
select * from reject_cpt;

create table rejected_cpt_ratio as
select distinct cptt as cpt_code, count(cptt) over(partition by cptt) as error_count, count(cptt) over() as total_error, count(cptt) over(partition by cptt)*100/count(cptt) over() as percentage from reject_cpt
;




#group by data cal
#error % of patient from total error
select patient, count(patient)as single_count, round(count(patient)*100/282,2) as per   from  reject_details group by 1 ;

#error % of patirnt from their own encounter
create table patient_reject_ratio as
with cte as(
select patient, count(patient)as single_count   from  ehr_ group by 1 ),
cte1 as(
select patient, count(patient)as single_count from  reject_details group by 1 
), cte2 as(
select cte.patient as patient, cte.single_count as total_encounter, cte1.single_count as reject_encounter
from cte left join cte1 on cte.patient = cte1.patient)
select *, case
when reject_encounter is not null then (reject_encounter*100/total_encounter) else 0 end as error_percentage from cte2 ;

select * from patient_reject_ratio;


#provider
#error % of provider from total error
select provider, count(provider)as single_count, round(count(provider)*100/282,2) as per   from  reject_details group by 1 ;

#error % of provider from their own encounter
create table provider_reject_ratio as
with cte as(
select provider, count(provider)as single_count   from  ehr_ group by 1 ),
cte1 as(
select provider, count(provider)as single_count from  reject_details group by 1 
), cte2 as(
select cte.provider as provider, cte.single_count as total, cte1.single_count as reject 
from cte left join cte1 on cte.provider = cte1.provider)
select *, case
when reject is not null then (reject*100/total) else 0 end as error_percentage from cte2 ;



#data flow:
select  patient, provider, date_db, group_concat(trim(regexp_replace(cpt, '[^0-9a-zA-Z]','')) separator ',' )  from db
group by 1,2,3
;
/*
main db = 6374, there are 44 encounter exist on db those are not available on ehr
ehr = 6612, db-ehr = 
main db exist in ehr = 6330, 282 encounter from ehr not exist on db

*/




select * from db;

select * from ehr_ left join db_ on
ehr_.patient = db_.patient and
ehr_.provider = db_.provider ;




select * from ehr_;
select * from db_;
select * from reject_details;
select * from reject_phase1;
select * from reject_phase2;
select * from reject_phase3;
select * from accepted_details;
select * from accepted_cpt;
select * from rejected_cpt;
select * from rejected_cpt_ratio;
select * from errorr;
select * from patient_reject_ratio;
select * from provider_reject_ratio;
select * from missing_ratio;


SELECT
  provider,
  cpt
FROM
  `reject_details`
WHERE
  provider = 'Aiden King' and cpt = 'NORCM';









