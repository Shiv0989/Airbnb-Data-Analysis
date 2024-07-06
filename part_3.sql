--Ad-Hoc Analysis 

--q1
Select
round(avg(CASE WHEN HAS_AVAILABILITY = 't' THEN ((30 - AVAILABILITY_30)*PRICE) END),2) as best_estimated_revenue,
listing_neighbourhood, sum(age_5_14_yr_p) as population_age_5_14 , sum(age_15_19_yr_p) as population_age_15_19 , sum(age_20_24_yr_p) as population_age_20_24, sum(age_35_44_yr_p) as population_age_35_44, sum(age_45_54_yr_p) as population_age_45_54, sum(age_55_64_yr_p)as population_age_55_64, sum(age_65_74_yr_p) as population_age_65_74
from warehouse.dim_censusg01 as g1
Right Join(
SELECT distinct f.lga_code, f.price, sc.scraped_date, lc.listing_neighbourhood, lc.has_availability, lc.availability_30
from warehouse.facts f Left join warehouse.dim_staging_listing lc on f.listing_id = lc.listing_id
left join warehouse.dim_staging_scrape sc on f.scrape_id = sc.scrape_id
where scraped_date > '2020-03-31') as ll
on ll.lga_code = g1.lga_code
group by listing_neighbourhood
order by best_estimated_revenue desc;

--q2
SELECT LISTING_NEIGHBOURHOOD, Number_of_stays, property_type,room_type, accommodates FROM(
Select property_type,room_type, accommodates, sum(30 - AVAILABILITY_30) as number_of_stays, listing_neighbourhood ,  rank() over (partition by listing_neighbourhood order by number_of_stays desc) as rk1
from(
select distinct lc.property_type, lc.room_type, lc.accommodates, lc.listing_neighbourhood, lc.availability_30, lc.has_availability, f.price
from warehouse.facts f Left join warehouse.DIM_STAGING_LISTING lc on f.listing_id = lc.listing_id)
where listing_neighbourhood = 'Northern Beaches' or listing_neighbourhood = 'Woollahra' or listing_neighbourhood = 'Mosman' or listing_neighbourhood = 'Waverley' or listing_neighbourhood = 'Hunters Hill'
group by listing_neighbourhood,property_type,room_type, accommodates
order by rk1)
where rk1 = 1;

--q3
select 
count(CASE WHEN upper(Ln_LGA) = H_LGA THEN 1 END) as number_of_hosts_lives_in_the_same_lga, 
count(CASE WHEN upper(Ln_LGA) != H_LGA THEN 1 END) as number_of_hosts_lives_in_different_lga from(
select * from (
select h.host_id, 
max(dl.listing_neighbourhood) as Ln_LGA,
count(f.id) as Multiple_Listings,
max(tp.lga_name) as H_LGA 
FROM warehouse.facts f
LEFT JOIN warehouse.dim_staging_listing dl on f.listing_id = dl.listing_id
LEFT JOIN warehouse.dim_staging_host h on f.host_id = h.host_id
LEFT JOIN
(select * from warehouse.dim_staging_host h
LEFT JOIN warehouse.dim_nsw_lga_suburb ds on upper(h.host_neighbourhood) = ds.suburb_name) tp on f.host_id = tp.host_id
where h.host_neighbourhood is not NULL
group by h.host_id)
where Multiple_Listings > 1 and H_LGA is not NULL);

--q4
SELECT count(CASE WHEN Sum_Estimated_revenue_per_active_listings > annualised_median_mortgage_repayment THEN 1 END) AS Number_of_rows_when_host_with_unique_listing_can_cover_annualised_median_mortgage_repayment,
count(CASE WHEN Sum_Estimated_revenue_per_active_listings < annualised_median_mortgage_repayment THEN 1 END) AS Number_of_rows_when_host_with_unique_listing_cannot_cover_annualised_median_mortgage_repayment FROM(
SELECT dl.listing_neighbourhood ,  round(sum(CASE WHEN dl.has_availability = 't' THEN ((30 - dl.availability_30)*price) END), 2) AS Sum_Estimated_revenue_per_active_listings, sum(12*tt.median_mortgage_repay_monthly) AS annualised_median_mortgage_repayment

FROM warehouse.dim_staging_listing dl  RIGHT JOIN(
SELECT f.listing_id,f.price as price, sc.scraped_date, c2.median_mortgage_repay_monthly

FROM warehouse.facts f LEFT JOIN warehouse.dim_staging_scrape sc on f.scrape_id = sc.scrape_id
LEFT JOIN warehouse.dim_censusg02 c2 on f.lga_code = c2.lga_code
WHERE scraped_date > '2020-03-31') as tt on dl.listing_id = tt.listing_id
GROUP BY dl.listing_id, dl.listing_neighbourhood
ORDER BY Sum_Estimated_revenue_per_active_listings DESC)
;





