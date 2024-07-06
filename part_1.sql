Create OR REPLACE database at3;

Use at3;

------raw
Create OR REPLACE schema raw;
Use at3.raw;

CREATE OR REPLACE STORAGE INTEGRATION GCP
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = GCS
ENABLED = TRUE
STORAGE_ALLOWED_LOCATIONS = ('gcs://australia-southeast1-at3-7e7d9965-bucket/data');

DESCRIBE STORAGE INTEGRATION GCP;

create or replace stage stage_gcp
storage_integration = GCP
url= 'gcs://australia-southeast1-at3-7e7d9965-bucket/data/'
;

LIST @stage_gcp;

create or replace file format raw.file_format_csv 
type = 'CSV' 
field_delimiter = ',' 
skip_header = 1
NULL_IF = ('\\N', 'NULL', 'NUL', '')
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
;



create or replace external table raw.raww
with location = @stage_gcp 
file_format = raw.file_format_csv
pattern = '.*_202.*[.]csv';

create or replace external table raw.raww2
with location = @stage_gcp 
file_format = raw.file_format_csv
pattern = '.*2016Census_G01.*[.]csv';

create or replace external table raw.raww3
with location = @stage_gcp 
file_format = raw.file_format_csv
pattern = '.*2016Census_G02.*[.]csv';

create or replace external table raw.raww4
with location = @stage_gcp 
file_format = raw.file_format_csv
pattern = '.*CODE.*[.]csv';

create or replace external table raw.raww5
with location = @stage_gcp 
file_format = raw.file_format_csv
pattern = '.*SUBURB.*[.]csv';

-----staging
Create or replace schema staging;

Use at3.staging;

CREATE OR REPLACE TABLE staging.temp as
SELECT uuid_string() as ID -- adding a column containing a unique id for each row 
      ,value:c1::int as Listing_ID
      ,value:c2::int as SCRAPE_ID
      ,value:c3::varchar as SCRAPED_DATE
      ,value:c4::int as HOST_ID
      ,value:c5::varchar as HOST_NAME
      ,value:c6::varchar as HOST_SINCE
      ,value:c7::varchar as HOST_IS_SUPERHOST
      ,value:c8::varchar as HOST_NEIGHBOURHOOD
      ,value:c9::varchar as LISTING_NEIGHBOURHOOD
      ,value:c10::varchar as PROPERTY_TYPE
      ,value:c11::varchar as ROOM_TYPE
      ,value:c12::int as ACCOMMODATES
      ,value:c13::int as PRICE
      ,value:c14::varchar as HAS_AVAILABILITY
      ,value:c15::int as AVAILABILITY_30
      ,value:c16::int as NUMBER_OF_REVIEWS
      ,value:c17::int as REVIEW_SCORES_RATING
      ,value:c18::int as REVIEW_SCORES_ACCURACY
      ,value:c19::int as REVIEW_SCORES_CLEANLINESS
      ,value:c20::int as REVIEW_SCORES_CHECKIN
      ,value:c21::int as REVIEW_SCORES_COMMUNICATION
      ,value:c22::int as REVIEW_SCORES_VALUE
      ,row_number() over(partition by NUMBER_OF_REVIEWS, REVIEW_SCORES_RATING, REVIEW_SCORES_ACCURACY, REVIEW_SCORES_CLEANLINESS,           REVIEW_SCORES_CHECKIN, REVIEW_SCORES_COMMUNICATION, REVIEW_SCORES_VALUE  order by NUMBER_OF_REVIEWS) AS Review_ID
from raw.raww;
 
--Creating all the dimension tables one after another

CREATE OR REPLACE TABLE staging.dim_staging_host as
SELECT 
     
      HOST_ID
    , HOST_NAME
    , HOST_SINCE
    , HOST_IS_SUPERHOST
    , HOST_NEIGHBOURHOOD
from staging.temp;

CREATE OR REPLACE TABLE staging.dim_staging_review as
SELECT 
      
      Review_ID, NUMBER_OF_REVIEWS
     ,REVIEW_SCORES_RATING
     ,REVIEW_SCORES_ACCURACY
     ,REVIEW_SCORES_CLEANLINESS
     ,REVIEW_SCORES_CHECKIN
     ,REVIEW_SCORES_COMMUNICATION
     ,REVIEW_SCORES_VALUE
from staging.temp;

CREATE OR REPLACE TABLE staging.dim_staging_listing as
SELECT 
      Listing_ID
     ,LISTING_NEIGHBOURHOOD
     ,PROPERTY_TYPE
     ,ROOM_TYPE
     ,ACCOMMODATES
     ,HAS_AVAILABILITY
     ,AVAILABILITY_30
from staging.temp;

CREATE OR REPLACE TABLE staging.dim_staging_scrape as
SELECT 
      value:c2::int as SCRAPE_ID
     ,DATE_TRUNC('month', value:c3::date) as SCRAPED_DATE
 
 from raw.raww;


CREATE or replace table staging.dim_CENSUSG01 as
select
    cast(substr(value:c1::varchar, 4,7) as int) LGA_CODE,
    value:c2::int AS Tot_P_M,
    value:c4::int AS Tot_P_P,
    value:c5::int AS Age_0_4_yr_M,
    value:c3::int AS Tot_P_F,
    value:c6::int AS Age_0_4_yr_F,
    value:c7::int AS Age_0_4_yr_P,
    value:c8::int AS Age_5_14_yr_M,
    value:c9::int AS Age_5_14_yr_F,
    value:c10::int AS Age_5_14_yr_P,
    value:c11::int AS Age_15_19_yr_M,
    value:c12::int AS Age_15_19_yr_F,
    value:c13::int AS Age_15_19_yr_P,
    value:c14::int AS Age_20_24_yr_M,
    value:c15::int AS Age_20_24_yr_F,
    value:c16::int AS Age_20_24_yr_P,
    value:c17::int AS Age_25_34_yr_M,
    value:c18::int AS Age_25_34_yr_F,
    value:c19::int AS Age_25_34_yr_P,
    value:c20::int AS Age_35_44_yr_M,
    value:c21::int AS Age_35_44_yr_F,
    value:c22::int AS Age_35_44_yr_P,
    value:c23::int AS Age_45_54_yr_M,
    value:c24::int AS Age_45_54_yr_F,
    value:c25::int AS Age_45_54_yr_P,
    value:c26::int AS Age_55_64_yr_M,
    value:c27::int AS Age_55_64_yr_F,
    value:c28::int AS Age_55_64_yr_P,
    value:c29::int AS Age_65_74_yr_M,
    value:c30::int AS Age_65_74_yr_F,
    value:c31::int AS Age_65_74_yr_P,
    value:c32::int AS Age_75_84_yr_M,
    value:c33::int AS Age_75_84_yr_F,
    value:c34::int AS Age_75_84_yr_P,
    value:c35::int AS Age_85ov_M,
    value:c36::int AS Age_85ov_F,
    value:c37::int AS Age_85ov_P,
    value:c38::int AS Counted_Census_Night_home_M,
    value:c39::int AS Counted_Census_Night_home_F,
    value:c40::int AS Counted_Census_Night_home_P,
    value:c41::int AS Count_Census_Nt_Ewhere_Aust_M,
    value:c42::int AS Count_Census_Nt_Ewhere_Aust_F,
    value:c43::int AS Count_Census_Nt_Ewhere_Aust_P,
    value:c44::int AS Indigenous_psns_Aboriginal_M,
    value:c45::int AS Indigenous_psns_Aboriginal_F,
    value:c46::int AS Indigenous_psns_Aboriginal_P,
    value:c47::int AS Indig_psns_Torres_Strait_Is_M,
    value:c48::int AS Indig_psns_Torres_Strait_Is_F,
    value:c49::int AS Indig_psns_Torres_Strait_Is_P,
    value:c50::int AS Indig_Bth_Abor_Torres_St_Is_M,
    value:c51::int AS Indig_Bth_Abor_Torres_St_Is_F,
    value:c52::int AS Indig_Bth_Abor_Torres_St_Is_P,
    value:c53::int AS Indigenous_P_Tot_M,
    value:c54::int AS Indigenous_P_Tot_F,
    value:c55::int AS Indigenous_P_Tot_P,
    value:c56::int AS Birthplace_Australia_M,
    value:c57::int AS Birthplace_Australia_F,
    value:c58::int AS Birthplace_Australia_P,
    value:c59::int AS Birthplace_Elsewhere_M,
    value:c60::int AS Birthplace_Elsewhere_F,
    value:c61::int AS Birthplace_Elsewhere_P,
    value:c62::int AS Lang_spoken_home_Eng_only_M,
    value:c63::int AS Lang_spoken_home_Eng_only_F,
    value:c64::int AS Lang_spoken_home_Eng_only_P,
    value:c65::int AS Lang_spoken_home_Oth_Lang_M,
    value:c66::int AS Lang_spoken_home_Oth_Lang_F,
    value:c67::int AS Lang_spoken_home_Oth_Lang_P,
    value:c68::int AS Australian_citizen_M,
    value:c69::int AS Australian_citizen_F,
    value:c70::int AS Australian_citizen_P,
    value:c71::int AS Age_psns_att_educ_inst_0_4_M,
    value:c72::int AS Age_psns_att_educ_inst_0_4_F,
    value:c73::int AS Age_psns_att_educ_inst_0_4_P,
    value:c74::int AS Age_psns_att_educ_inst_5_14_M,
    value:c75::int AS Age_psns_att_educ_inst_5_14_F,
    value:c76::int AS Age_psns_att_educ_inst_5_14_P,
    value:c77::int AS Age_psns_att_edu_inst_15_19_M,
    value:c78::int AS Age_psns_att_edu_inst_15_19_F,
    value:c79::int AS Age_psns_att_edu_inst_15_19_P,
    value:c80::int AS Age_psns_att_edu_inst_20_24_MM,
    value:c81::int AS Age_psns_att_edu_inst_20_24_MF,
    value:c82::int AS Age_psns_att_edu_inst_20_24_MP,
    value:c83::int AS Age_psns_att_edu_inst_25_ov_M,
    value:c84::int AS Age_psns_att_edu_inst_25_ov_F,
    value:c85::int AS Age_psns_att_edu_inst_25_ov_P,
    value:c86::int AS High_yr_schl_comp_Yr_12_eq_M,
    value:c87::int AS High_yr_schl_comp_Yr_12_eq_F,
    value:c88::int AS High_yr_schl_comp_Yr_12_eq_P,
    value:c89::int AS High_yr_schl_comp_Yr_11_eq_M,
    value:c90::int AS High_yr_schl_comp_Yr_11_eq_F,
    value:c91::int AS High_yr_schl_comp_Yr_11_eq_P,
    value:c92::int AS High_yr_schl_comp_Yr_10_eq_M,
    value:c93::int AS High_yr_schl_comp_Yr_10_eq_F,
    value:c94::int AS High_yr_schl_comp_Yr_10_eq_P,
    value:c95::int AS High_yr_schl_comp_Yr_9_eq_M,
    value:c96::int AS High_yr_schl_comp_Yr_9_eq_F,
    value:c97::int AS High_yr_schl_comp_Yr_9_eq_P,
    value:c98::int AS High_yr_schl_comp_Yr_8_belw_M,
    value:c99::int AS High_yr_schl_comp_Yr_8_belw_F,
    value:c100::int AS High_yr_schl_comp_Yr_8_belw_P,
    value:c101::int AS High_yr_schl_comp_D_n_g_sch_M,
    value:c102::int AS High_yr_schl_comp_D_n_g_sch_F,
    value:c103::int AS High_yr_schl_comp_D_n_g_sch_P,
    value:c104::int AS Count_psns_occ_priv_dwgs_M,
    value:c105::int AS Count_psns_occ_priv_dwgs_F,
    value:c106::int AS Count_psns_occ_priv_dwgs_P,
    value:c107::int AS Count_Persons_other_dwgs_M,
    value:c108::int AS Count_Persons_other_dwgs_F,
    value:c109::int AS Count_Persons_other_dwgs_P
from raw.raww2;

CREATE OR REPLACE TABLE staging.dim_CENSUSG02 as
SELECT 
      cast(substr(value:c1::varchar, 4,7) as int) LGA_CODE
     ,value:c2::int as Median_age_persons
     ,value:c3::int as Median_mortgage_repay_monthly
     ,value:c4::int as Median_tot_prsnl_inc_weekly
     ,value:c5::int as Median_rent_weekly
     ,value:c6::int as Median_tot_fam_inc_weekly
     ,value:c7::int as Average_num_psns_per_bedroom
     ,value:c8::int as Median_tot_hhd_inc_weekly
     ,value:c9::int as Average_household_size
 from raw.raww3;
 
CREATE OR REPLACE TABLE staging.dim_NSW_LGA_SUB as
SELECT 
      value:c1::varchar as LGA_NAME
      ,value:c2::varchar as SUBURB_NAME

from raw.raww5;

CREATE OR REPLACE TABLE staging.dim_NSW_LGA_CODE as
SELECT 
      value:c1::int as LGA_CODE
      ,value:c2::varchar as LGA_NAME
from raw.raww4;

create or replace table staging.DIM_NSW_LGA_SUBURB as 
select lc.LGA_CODE, lc.lga_name, ls.suburb_name
from staging.DIM_NSW_LGA_CODE lc
LEFT JOIN staging.dim_NSW_LGA_SUB ls on upper(lc.lga_name) = ls.lga_name;

select * from staging.DIM_NSW_LGA_SUBURB;


create or replace table staging.facts as 
select 
      ID,
      Price,
      HOST_ID,
      LISTING_ID,
      scrape_id,
      review_id,
      LGA_CODE
FROM staging.temp t
LEFT JOIN staging.dim_NSW_LGA_CODE lga on t.listing_neighbourhood = lga.lga_name;  
  
--data warehouse (star schema)
create or replace schema warehouse;

Use at3.warehouse;

create or replace table warehouse.facts as select * from staging.facts; 
alter table warehouse.facts add primary key(ID);

create or replace table warehouse.dim_staging_host as select * from staging.dim_staging_host;
alter table warehouse.dim_staging_host add primary key(HOST_ID);

create or replace table warehouse.dim_staging_review as select * from staging.dim_staging_review;
alter table warehouse.dim_staging_review add primary key(Review_ID);

create or replace table warehouse.dim_staging_listing as select * from staging.dim_staging_listing;
alter table warehouse.dim_staging_listing add primary key(Listing_ID);

create or replace table warehouse.dim_staging_scrape as select * from staging.dim_staging_scrape;
alter table warehouse.dim_staging_scrape add primary key(SCRAPE_ID);

create or replace table warehouse.dim_CENSUSG01 as select * from staging.dim_CENSUSG01;
alter table warehouse.dim_CENSUSG01 add primary key(LGA_CODE);

create or replace table warehouse.dim_CENSUSG02 as select * from staging.DIM_CENSUSG02;
alter table warehouse.dim_CENSUSG02 add primary key(LGA_CODE);

create or replace table warehouse.dim_NSW_LGA_CODE as select * from staging.dim_NSW_LGA_CODE;
alter table warehouse.dim_NSW_LGA_CODE add primary key(LGA_CODE);

create or replace table warehouse.dim_NSW_LGA_SUBURB as select * from staging.dim_NSW_LGA_SUBURB;
alter table warehouse.dim_NSW_LGA_SUBURB add primary key(LGA_CODE);


ALTER TABLE warehouse.facts ADD CONSTRAINT FK_Listing FOREIGN KEY (Listing_ID) REFERENCES warehouse.dim_staging_listing(Listing_ID);

ALTER TABLE warehouse.facts ADD CONSTRAINT FK_Host FOREIGN KEY (HOST_ID) REFERENCES warehouse.dim_staging_host(HOST_ID);

ALTER TABLE warehouse.facts ADD CONSTRAINT FK_Review FOREIGN KEY (Review_ID) REFERENCES warehouse.dim_staging_Review(Review_ID);

ALTER TABLE warehouse.facts ADD CONSTRAINT FK_Scrape FOREIGN KEY (Scrape_ID) REFERENCES warehouse.dim_staging_Scrape(Scrape_ID);

ALTER TABLE warehouse.facts ADD CONSTRAINT FK_CENSUSG01 FOREIGN KEY (LGA_CODE) REFERENCES warehouse.dim_CENSUSG01(LGA_CODE);

ALTER TABLE warehouse.facts ADD CONSTRAINT FK_CENSUSG02 FOREIGN KEY (LGA_CODE) REFERENCES warehouse.dim_CENSUSG02(LGA_CODE);

ALTER TABLE warehouse.facts ADD CONSTRAINT FK_NSW_LGA_CODE FOREIGN KEY (LGA_CODE) REFERENCES warehouse.dim_NSW_LGA_CODE(LGA_CODE);

ALTER TABLE warehouse.facts ADD CONSTRAINT FK_NSW_LGA_SUBURB FOREIGN KEY (LGA_CODE) REFERENCES warehouse.DIM_NSW_LGA_SUBURB(LGA_CODE);


--datamart      

create or replace schema datamart;
use at3.datamart;

--table dm_listing neighbourhood
create or replace table datamart.dm_listing_neighbourhood as
SELECT dsl.listing_neighbourhood, 

vs.scraped_date,

(count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 end )/count(HAS_AVAILABILITY))*100 as Active_Listing_rate,

min(CASE WHEN HAS_AVAILABILITY = 't' THEN PRICE end) as Min_Active_Listings, 

max(CASE WHEN HAS_AVAILABILITY = 't' THEN PRICE end) as Max_Active_Listings, 

PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN HAS_AVAILABILITY = 't' THEN  PRICE end)) as Median_ActiveListings, 

avg(CASE WHEN HAS_AVAILABILITY = 't' THEN PRICE end) as Average_ActiveListings,

count(distinct(HOST_ID)) as number_of_distinct_hosts, 

((count(distinct(CASE WHEN HOST_IS_SUPERHOST = 't' THEN (HOST_ID) END)))/count(distinct(HOST_ID)))*100 as Superhost_rate,

avg(CASE WHEN HAS_AVAILABILITY = 't' THEN review_scores_rating end) as avg_review_scores_rating,

(((count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 END)) - LAG(count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 END)) over(order by LISTING_NEIGHBOURHOOD, vs.scraped_date))/NULLIF(LAG(count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 END)) over(order by LISTING_NEIGHBOURHOOD, vs.scraped_date), 0))*100 as Percentage_change_active_listings, 

(((count(CASE WHEN HAS_AVAILABILITY = 'f' THEN 1 END)) - LAG(count(CASE WHEN HAS_AVAILABILITY = 'f' THEN 1 END)) over(order by LISTING_NEIGHBOURHOOD, vs.scraped_date))/NULLIF(LAG(count(CASE WHEN HAS_AVAILABILITY = 'f' THEN 1 END)) over(order by LISTING_NEIGHBOURHOOD,vs.scraped_date ), 0))*100 as Percentage_change_inactive_listings,

sum(CASE WHEN HAS_AVAILABILITY = 't' THEN (30 - AVAILABILITY_30) end) as number_of_stays,

avg(CASE WHEN HAS_AVAILABILITY = 't' THEN ((30 - AVAILABILITY_30)*PRICE) END) as avg_estimated_revenue

FROM warehouse.dim_staging_listing dsl  
    RIGHT JOIN (
     SELECT distinct f.id, f.host_id,f.PRICE, f.scrape_id, sc.scraped_date, f.listing_id, h.host_is_superhost, r.review_scores_rating
     from warehouse.facts f Left JOIN warehouse.dim_staging_host h  on f.host_id = h.host_id 
    left join warehouse.dim_staging_scrape sc on f.scrape_id = sc.scrape_id
    left join warehouse.dim_staging_review r on f.review_id = r.review_id)as vs 
       
     ON  vs.listing_id = dsl.listing_id
     group by dsl.listing_neighbourhood, vs.SCRAPED_DATE
     order by dsl.listing_neighbourhood, vs.SCRAPED_DATE;

------------------------------------------------------------------------------------

--table dm_property_type
create or replace table datamart.dm_property_type as
SELECT dsl.property_type,dsl.room_type, dsl.accommodates, vs.SCRAPED_DATE,

(count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 end )/count(HAS_AVAILABILITY))*100 as Active_Listing_rate,

min(CASE WHEN HAS_AVAILABILITY = 't' THEN PRICE end) as Min_Active_Listings, 

max(CASE WHEN HAS_AVAILABILITY = 't' THEN PRICE end) as Max_Active_Listings, 

PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY (CASE WHEN HAS_AVAILABILITY = 't' THEN  PRICE end)) as Median_ActiveListings, 

avg(CASE WHEN HAS_AVAILABILITY = 't' THEN PRICE end) as Average_ActiveListings,

count(distinct(HOST_ID)) as number_of_distinct_hosts, 

((count(distinct(CASE WHEN HOST_IS_SUPERHOST = 't' THEN (HOST_ID) END)))/count(distinct(HOST_ID)))*100 as Superhost_rate,

avg(CASE WHEN HAS_AVAILABILITY = 't' THEN review_scores_rating end) as avg_review_scores_rating,

(((count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 END)) - LAG(count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 END)) over(order by dsl.property_type,dsl.room_type, dsl.accommodates, vs.SCRAPED_DATE))/NULLIF(LAG(count(CASE WHEN HAS_AVAILABILITY = 't' THEN 1 END)) over(order by dsl.property_type,dsl.room_type, dsl.accommodates, vs.SCRAPED_DATE), 0))*100 as Percentage_change_active_listings, 

(((count(CASE WHEN HAS_AVAILABILITY = 'f' THEN 1 END)) - LAG(count(CASE WHEN HAS_AVAILABILITY = 'f' THEN 1 END)) over(order by dsl.property_type,dsl.room_type, dsl.accommodates, vs.SCRAPED_DATE))/NULLIF(LAG(count(CASE WHEN HAS_AVAILABILITY = 'f' THEN 1 END)) over(order by dsl.property_type,dsl.room_type, dsl.accommodates, vs.SCRAPED_DATE ), 0))*100 as Percentage_change_inactive_listings,

sum(CASE WHEN HAS_AVAILABILITY = 't' THEN (30 - AVAILABILITY_30) end) as number_of_stays,

avg(CASE WHEN HAS_AVAILABILITY = 't' THEN ((30 - AVAILABILITY_30)*PRICE) END) as avg_estimated_revenue

FROM warehouse.dim_staging_listing dsl  
    RIGHT JOIN (
     SELECT distinct f.id, f.host_id,f.PRICE, f.scrape_id, sc.scraped_date, f.listing_id, h.host_is_superhost, r.review_scores_rating
     from warehouse.facts f Left JOIN warehouse.dim_staging_host h  on f.host_id = h.host_id 
    left join warehouse.dim_staging_scrape sc on f.scrape_id = sc.scrape_id
    left join warehouse.dim_staging_review r on f.review_id = r.review_id)as vs 
       
     ON  vs.listing_id = dsl.listing_id
     group by dsl.property_type,dsl.room_type, dsl.accommodates, vs.SCRAPED_DATE
     order by dsl.property_type,dsl.room_type, dsl.accommodates, vs.SCRAPED_DATE;
     
------------------------------------------------------------------------------------
--table dm_host_neighbourhood
create or replace table datamart.dm_host_neighbourhood as
select LISTING_NEIGHBOURHOOD as host_neighbourhood_lga, date_trunc('month',date(scraped_date)) as year_month, count(distinct(host_id)) as number_of_distinct_host, avg((30 - AVAILABILITY_30)*price) as Estimated_Revenue, (SUM(distinct(CASE WHEN HAS_AVAILABILITY = 't' THEN (30 - AVAILABILITY_30)*price END)))/count(distinct(HOST_ID)) as Estimated_Revenue_per_host 

from staging.temp
where HOST_NEIGHBOURHOOD is not null
group by host_neighbourhood_lga,  scraped_date;

------------------------------------------------------------------------------------
