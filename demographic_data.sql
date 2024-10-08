SELECT * FROM get_demographic_data_one(-86.5315257, 32.5467483, 62500,'Acme');

CREATE OR REPLACE FUNCTION get_demographic_data_one(  lat FLOAT, long FLOAT, total_area numeric, banner varchar )
RETURNS TABLE(
    store_fit                                               varchar,
    area_total                                              NUMERIC,
    area_sales                                              NUMERIC,
    longitude                                               FLOAT,
    latitude                                                FLOAT,
    pop_per_sqmi                                            NUMERIC,
    pop                                                     NUMERIC,
    growth5yr                                               NUMERIC,
    avg_hh_inc                                              NUMERIC,
    unemployment                                            NUMERIC,
    bach_degree_plus                                        NUMERIC,
    white_pct                                               NUMERIC,
    black_pct                                               NUMERIC,
    asian_pct                                               NUMERIC,
    hisp_pct                                                NUMERIC,
    other_race_pct                                          NUMERIC,
    median_age                                              DOUBLE PRECISION,
    people_per_hh                                           NUMERIC,
    qc_1                                                    NUMERIC,
    qc_2                                                    NUMERIC,
    qc_3                                                    NUMERIC,
    qc_4                                                    NUMERIC,
    qc_5                                                    NUMERIC,
    qc_6                                                    NUMERIC,
    avg_mortgage_risk                                       NUMERIC,
    pov_pct                                                 NUMERIC,
    avg_vehicles                                            NUMERIC,
    gq                                                      NUMERIC,
    institutionalized_pop                                   NUMERIC,
    non_institutionalized_pop                               NUMERIC,
    institutionalized_pop_pct                               NUMERIC,
    gq_college_pct                                          NUMERIC,
    gq_military_pct                                         NUMERIC,
    pcw_reg                                                 NUMERIC,
    sales_potential                                         NUMERIC,
    density_class                                           VARCHAR, 
    fit_score                                               NUMERIC,
    aldi_fit                                                NUMERIC,
    asian_fit                                               NUMERIC,
    club_fit                                                NUMERIC,
    conv_fit        		                                NUMERIC,
    disc_fit                                             	NUMERIC,
    hisp_fit                                                NUMERIC,
    nat_org_fit                                             NUMERIC,
    qual_serv_fit                                           NUMERIC,
    sal_fit                                                 NUMERIC,
    sprouts_fit                                             NUMERIC,
    sc_fit                                                  NUMERIC,
    tj_fit                                                  NUMERIC,
    warehouse_fit                                           NUMERIC,
    whole_fds_fit                                           NUMERIC, 
    aldi_count                                              NUMERIC,
    asian_count                                             NUMERIC,
    club_count                                              NUMERIC,
    conventional_count                                      NUMERIC,
    discount_count                                          NUMERIC,
    hisp_count                                              NUMERIC,
    nat_org_count                                           NUMERIC,
    qual_serv_count                                         NUMERIC,
    sal_count                                               NUMERIC,
    sprouts_count                                           NUMERIC,
    sc_count                                                NUMERIC,
    tj_count                                                NUMERIC,
    warehouse_count                                         NUMERIC,
    whole_fds_count                                         NUMERIC,
    total_count                                             NUMERIC,
    sqft_per_pop                                            NUMERIC,
    conventional_sqft                                       NUMERIC,
    discount_sqft                                           NUMERIC,
    hisp_sqft                                               NUMERIC,
    sal_sqft                                                NUMERIC,
    nat_org_sqft                                            NUMERIC,
    sc_sqft                                                 NUMERIC,
    warehouse_sqft                                          NUMERIC,
    club_sqft                                               NUMERIC,
    tj_sqft                                                 NUMERIC,
    whole_fds_sqft                                          NUMERIC,
    sprouts_sqft                                            NUMERIC,
    aldi_sqft                                               NUMERIC, 
    asian_sqft                                              NUMERIC,
    qual_serv_sqft                                          NUMERIC,
	total_sqft                                               NUMERIC,
    banner_name                                             Varchar,
    store_group_fitsize                                     varchar,
    banner_avg_spsf     	                                 NUMERIC,
    store_group_fitsize_avg_spsf                            NUMERIC,
    daytime_pop                                             NUMERIC,
    employees                                               NUMERIC,
    establishments                                          NUMERIC,
    traveler_accommodation                                  NUMERIC,
    white_collar                                            NUMERIC,
    blue_collar                                             NUMERIC,
    so_sqft                                                 NUMERIC,
    so_sqft_class                                           varchar,
    avg_ms_so_sqft_class                                    varchar,
    state                                                   varchar
) AS $$
BEGIN
 	RETURN QUERY
     WITH geom AS (
        SELECT ST_Buffer(ST_SetSRID(ST_MakePoint(lat, long), 4326), 0.3) AS shape
    ),
    area AS (
        SELECT ST_Area(g.shape::geography) AS area_sq_meters
        FROM geom g
    ),
    block_points AS (
        SELECT bp.*
        FROM "mtn-dev".mtn_depot.block_point bp
        JOIN geom ON ST_Within(ST_SetSRID(point, 4326), (SELECT shape FROM geom))
        WHERE release_date = '2024-06-01'
    ),
 	store_fit_name as (
       					select default_fit  
						from mtn_lookups.lookup_banner_name lbn  
						where store_name = banner
	),
	check_store AS (
	    SELECT s.*
	    FROM "mtn-dev".mtn_identity.store s
	   JOIN geom g ON ST_Intersects(g.shape, ST_SetSRID(s.geofence, 4326)) 
	),
	all_fits as (
		select 
			max(asian_fit_count)			     	as asian_fit_count,
			max(aldi_fit_count)                    as aldi_fit_count,
			max(warehouse_fit_count)               as warehouse_fit_count,
			max(supercenter_fit_count)             as supercenter_fit_count,
			max(conventional_fit_count)            as conventional_fit_count,
			max(natural_foods_fit_count)           as natural_foods_fit_count,
			max(hispanic_fit_count)                as hispanic_fit_count,
			max(club_fit_count)                    as club_fit_count,
			max(trader_joes_fit_count)             as trader_joes_fit_count,
			max(quality_service_fit_count)         as quality_service_fit_count,
			max(sprouts_fit_count)                 as sprouts_fit_count,
			max(discount_fit_count)                as discount_fit_count,
			max(save_a_lot_fit_count)              as save_a_lot_fit_count,
			max(whole_foods_fit_count)             as whole_foods_fit_count,
			max(all_fit_count) 						   as all_fit_count,
			max(asian_fit_sqft)					      as asian_fit_sqft,
			max(aldi_fit_sqft)                     as aldi_fit_sqft,
			max(warehouse_fit_sqft)                as warehouse_fit_sqft,
			max(supercenter_fit_sqft)              as supercenter_fit_sqft,
			max(conventional_fit_sqft)             as conventional_fit_sqft,
			max(natural_foods_fit_sqft)            as natural_foods_fit_sqft,
			max(hispanic_fit_sqft)                 as hispanic_fit_sqft,
			max(club_fit_sqft)                     as club_fit_sqft,
			max(trader_joes_fit_sqft)              as trader_joes_fit_sqft,
			max(quality_service_fit_sqft)          as quality_service_fit_sqft,
			max(sprouts_fit_sqft)                  as sprouts_fit_sqft,
			max(discount_fit_sqft)                 as discount_fit_sqft,
			max(save_a_lot_fit_sqft)               as save_a_lot_fit_sqft,
			max(whole_foods_fit_sqft)              as whole_foods_fit_sqft,
			max(all_fit_sqft )					      as all_fit_sqft
			from (
			select 
			case when fit=upper('asian')  then count(*) else 0 end                                                   as asian_fit_count,
			case when fit=upper('aldi')  then count(*) else 0 end                                                    as aldi_fit_count,
			case when fit=upper('warehouse')  then count(*) else 0 end                                               as warehouse_fit_count,
			case when fit=upper('supercenter')  then count(*) when upper(fit) = upper(default_fit) then count(*) + 1 else 0 end                                             as supercenter_fit_count,
			case when fit=upper('conventional')  then count(*) when upper(fit) = upper(default_fit) then count(*) + 1 else 0 end                            as conventional_fit_count,
			case when fit=upper('natural_foods' ) then count(*) else 0 end                                           as natural_foods_fit_count,
			case when fit=upper('hispanic')  then count(*) else 0 end                                                as hispanic_fit_count,
			case when fit=upper('club')  then count(*) else 0 end                                                    as club_fit_count,
			case when fit=upper('trader_joes')  then count(*) else 0 end                                             as trader_joes_fit_count,
			case when fit=upper('quality_service')  then count(*) else 0 end                                         as quality_service_fit_count,
			case when fit=upper('sprouts')  then count(*) else 0 end                                                 as sprouts_fit_count,
			case when fit=upper('discount' ) then count(*) else 0 end                                                as discount_fit_count,
			case when fit=upper('save_a_lot')  then count(*) else 0 end                                              as save_a_lot_fit_count,
			case when fit=upper('whole_foods')  then count(*) else 0 end                                             as whole_foods_fit_count,
			count(*)                                                                                                 as all_fit_count,
			case when fit=upper('asian')  then sum(grocery_sales_area) else 0 end                                    as asian_fit_sqft,
			case when fit=upper('aldi')  then sum(grocery_sales_area) else 0 end                                     as aldi_fit_sqft,
			case when fit=upper('warehouse')  then sum(grocery_sales_area) else 0 end                                as warehouse_fit_sqft,
			case when fit=upper('supercenter')  then sum(grocery_sales_area) else 0 end                              as supercenter_fit_sqft,
			case when fit=upper('conventional')  then sum(grocery_sales_area) else 0 end                             as conventional_fit_sqft,
			case when fit=upper('natural_foods' ) then sum(grocery_sales_area) else 0 end                            as natural_foods_fit_sqft,
			case when fit=upper('hispanic')  then sum(grocery_sales_area) else 0 end                                 as hispanic_fit_sqft,
			case when fit=upper('club')  then sum(grocery_sales_area) else 0 end                                     as club_fit_sqft,
			case when fit=upper('trader_joes')  then sum(grocery_sales_area) else 0 end                              as trader_joes_fit_sqft,
			case when fit=upper('quality_service')  then sum(grocery_sales_area) else 0 end                          as quality_service_fit_sqft,
			case when fit=upper('sprouts')  then sum(grocery_sales_area) else 0 end                                  as sprouts_fit_sqft,
			case when fit=upper('discount' ) then sum(grocery_sales_area) else 0 end                                 as discount_fit_sqft,
			case when fit=upper('save_a_lot')  then sum(grocery_sales_area) else 0 end                               as save_a_lot_fit_sqft,
			case when fit=upper('whole_foods')  then sum(grocery_sales_area) else 0 end                              as whole_foods_fit_sqft,
			sum(grocery_sales_area)                                                                                  as all_fit_sqft
			from check_store
			left join store_fit_name on true
			group by fit, default_fit ) A
	),
    block_group_weights AS (
        SELECT block_group_fips AS fips,
               SUM(population_percentage)::numeric AS population_weight,
               SUM(workplace_percentage)::numeric AS workplace_weight,
               SUM(household_percentage)::numeric AS household_weight,
               SUM(household_population_percentage)::numeric AS household_population_weight
        FROM block_points
        GROUP BY block_group_fips
    ),
    demographic_record_popstats_view AS (
        SELECT t.age_female_25_30 + t.age_female_30_35 + t.age_female_35_40 + t.age_female_40_45 +
               t.age_female_45_50 + t.age_female_50_55 + t.age_female_55_60 + t.age_65_plus +
               t.age_male_25_30 + t.age_male_30_35 + t.age_male_35_40 + t.age_male_40_45 +
               t.age_male_45_50 + t.age_male_50_55 + t.age_male_55_60 AS age_25_plus,
               t.education_college_bachelors + t.education_college_doctorate + t.education_college_masters +
               t.education_college_professional AS bachelor_or_higher,
               t.*
        FROM mtn_depot.demographic_record_popstats t
    ),
    bg_weighted_values AS (
        SELECT p.age_25_plus * bg.population_weight::numeric 									AS weighted_age_25_plus,
               p.population_estimate * bg.population_weight::numeric       						as weighted_population_estimate,
						    p.population_forecast_5_year * bg.population_weight::numeric        as weighted_population_forecast_5_year,
						    p.households_average_income 										as households_average_income,
						    p.unemployment_rate                                                 as unemployment_rate,
						    p.ethnicity_white * bg.population_weight ::numeric                  as weighted_ethnicity_white,
						    p.ethnicity_black * bg.population_weight ::numeric                  as weighted_ethnicity_black,
						    p.ethnicity_asian * bg.population_weight ::numeric                  as weighted_ethnicity_asian,
						    p.ethnicity_hispanic * bg.population_weight ::numeric               as weighted_ethnicity_hispanic,
						    p.ethnicity_other * bg.population_weight ::numeric                  as weighted_ethnicity_other,
						    p.age_median                                                        as age_median,
						    p.households_persons_per ::numeric                                  as households_persons_per,
						    p.population_seasonal_estimate_q_minus_1 *
                            bg.population_weight ::numeric                                      as weighted_population_seasonal_estimate_q_minus_1,
                            p.population_seasonal_estimate_q_minus_2 *
                            bg.population_weight ::numeric                                      as weighted_population_seasonal_estimate_q_minus_2,
                            p.population_seasonal_estimate_q_minus_3 *
                            bg.population_weight ::numeric                                      as weighted_population_seasonal_estimate_q_minus_3,
                            p.population_seasonal_estimate_q_minus_4 *
                            bg.population_weight ::numeric                                    	as weighted_population_seasonal_estimate_q_minus_4,
                            p.population_seasonal_estimate_q_minus_5 *
                            bg.population_weight ::numeric                                      as weighted_population_seasonal_estimate_q_minus_5,
                            p.population_seasonal_estimate_q_minus_6 *
                            bg.population_weight ::numeric                                      as weighted_population_seasonal_estimate_q_minus_6,
	                        p.mortgage_risk_average                                             as mortgage_risk_average,
                            p.group_quarters_college * bg.population_weight ::numeric           as weighted_group_quarters_college,
                            p.group_quarters_military * bg.population_weight ::numeric          as weighted_group_quarters_military,
                            p.households_in_poverty * bg.household_weight ::numeric             as weighted_households_in_poverty,
                            p.fit_score_aldi * bg.population_weight::numeric                    as weighted_fit_score_aldi,
							p.fit_score_asian * bg.population_weight::numeric                   as weighted_fit_score_asian,
                            p.fit_score_club * bg.population_weight::numeric                    as weighted_fit_score_club,
                            p.fit_score_conventional * bg.population_weight::numeric            as weighted_fit_score_conventional,
                            p.fit_score_discount * bg.population_weight::numeric                as weighted_fit_score_discount,
                            p.fit_score_hispanic * bg.population_weight::numeric                as weighted_fit_score_hispanic,
                            p.fit_score_natural_organic * bg.population_weight::numeric         as weighted_fit_score_natural_organic,
                            p.fit_score_quality_service * bg.population_weight::numeric         as weighted_fit_score_quality_service,
                            p.fit_score_save_a_lot * bg.population_weight::numeric              as weighted_fit_score_save_a_lot,
                            p.fit_score_sprouts * bg.population_weight::numeric                 as weighted_fit_score_sprouts,
                            p.fit_score_supercenter * bg.population_weight::numeric             as weighted_fit_score_supercenter,
                       		p.fit_score_trader_joes * bg.population_weight::numeric             as weighted_fit_score_trader_joes,
                            p.fit_score_warehouse * bg.population_weight::numeric               as weighted_fit_score_warehouse,
                            p.fit_score_whole_foods * bg.population_weight::numeric             as weighted_fit_score_whole_foods,
                            p.households_estimate * bg.household_weight ::numeric               as weighted_households_estimate,
                            p.bachelor_or_higher * bg.population_weight ::numeric               as weighted_bachelor_or_higher,
                            coalesce(w.daytime_population, 0) * bg.workplace_weight ::numeric   as weighted_daytime_population,
                            coalesce(w.white_collar, 0) * bg.workplace_weight ::numeric         as weighted_white_collar,
                            coalesce(w.blue_collar, 0) * bg.workplace_weight ::numeric          as weighted_blue_collar,
                            p.group_quarters_institutionalized * bg.population_weight ::numeric     		as weighted_group_quarters_institutionalized,
                            p.group_quarters_non_institutionalized * bg.population_weight ::numeric 		as weighted_group_quarters_non_institutionalized,
							w.employees::numeric															as employees,
							w.establishments::numeric														as establishments,
							w.traveler_accommodation::numeric												as traveler_accommodation,
							p.pcw_region::numeric															as pcw_region,
							p.number_of_vehicles::numeric													as number_of_vehicles,
							p.households_estimate::numeric 													as households_estimate
					from 
					    block_group_weights bg
					left join 
					    demographic_record_popstats_view p
					    on p.fips = bg.fips
					    and p.release_date = '2024-06-01'	
					    and bg.population_weight > 0
					left join 
					    mtn_depot.demographic_record_workplace w
					    on w.fips = bg.fips
					    and w.release_date = '2024-06-01'
					where 
					    bg.population_weight > 0 
		),
		area_density_class as (
				select  ldc.density_class
				from mtn_lookups.lookup_density_class ldc  where (select  sum(bg.weighted_population_estimate::numeric)::numeric / (max(area.area_sq_meters)::numeric / 2.59e6) 
      															  from bg_weighted_values bg 
      															  join area on true )  >= min_value  
      														and (select  sum(bg.weighted_population_estimate::numeric)::numeric / (max(area.area_sq_meters)::numeric / 2.59e6) 
																 from bg_weighted_values bg 
       															join area on true ) < max_value       
       	),
       	store_fit_size as (
       					select default_fit||' '||(UPPER(LEFT(size, 1)) || LOWER(SUBSTRING(size FROM 2))) as sizes, size 
       					from "mtn-dev".mtn_lookups.lookup_fit_size lfs
						join store_fit_name on true  
       					where (0.75*total_area) >= sales_area_sqft_min  and (0.75*total_area) <= sales_area_sqft_max 
		),
		banner_spsf as (select banner_name_avg_spsf as avg_spsf
						from "mtn-dev".mtn_lookups.lookup_banner_spsf lbs 
						where lower(name) = lower(banner) 		
		),
		fit_size_spsf as (
					select fitsize_avg_spsf 
					 from "mtn-dev".mtn_lookups.lookup_fit_size_spsf lfss 
					 join store_fit_name on true 
					 join store_fit_size on true
					 where lower(fitsize) = lower(default_fit)||' '||lower(size) 
		),
		sqft_class_one as (
			select lsc."class" as class_sqft
			from "mtn-dev".mtn_lookups.lookup_sosf_class lsc
			join all_fits on true
			where ((0.75*total_area)/all_fits.all_fit_sqft) >= min_value  and  ((0.75*total_area)/all_fits.all_fit_sqft) <= max_value
		),
		avg_soft_class_one as (
				select lmsc.avg_sosf_class as avg_ms_sosf_class
				from mtn_lookups.lookup_ms_sosf_class lmsc 
				join sqft_class_one on true
				join area_density_class on true 
				where area_density_class.density_class = lmsc.density_class and 
			  		  sqft_class_one.class_sqft = lmsc.sosf_class
		)		
   		select
		    max(default_fit)::varchar																						as store_fit,
		    total_area  																									as area_total,
			(CASE 
            			WHEN LOWER(default_fit) = 'supercenter' THEN (0.45 * total_area) 
            			WHEN LOWER(default_fit) = 'club' THEN (0.55 * total_area)
            			ELSE (0.725 * total_area)
        			END	)::numeric																							as area_sales,
		    long  																											as longitude,
		    lat 																								    		as latitude,
    coalesce(sum(bg.weighted_population_estimate::numeric)::numeric / 					
       (max(area.area_sq_meters)::numeric / 2.59e6),0) 																		as pop_per_sqmi,
    coalesce(sum(bg.weighted_population_estimate),0) 																		as pop,
    coalesce(sum(bg.weighted_population_forecast_5_year),0)																	as growth5yr,
	coalesce(sum(bg.weighted_population_estimate::numeric * bg.households_average_income::numeric)::numeric / 					
     sum(bg.weighted_population_estimate)::numeric,0) 				    													as avg_hh_inc,
    coalesce((sum(bg.weighted_population_estimate::numeric * unemployment_rate::numeric)::numeric /					
        sum(bg.weighted_population_estimate)::numeric) /					
       100.0 ,0)                                                       														as unemployment,
    coalesce(sum(bg.weighted_bachelor_or_higher)::numeric /					
       sum(bg.weighted_age_25_plus)::numeric ,0)                       														as bach_degree_plus,
    coalesce(sum(bg.weighted_ethnicity_white)::numeric /														
       sum(bg.weighted_population_estimate)::numeric ,0)  			    													as white_pct,
    coalesce(sum(bg.weighted_ethnicity_black)::numeric /														
       sum(bg.weighted_population_estimate)::numeric ,0)																	as black_pct,
    coalesce(sum(bg.weighted_ethnicity_asian)::numeric /														
       sum(bg.weighted_population_estimate)::numeric ,0)																	as asian_pct,
    coalesce(sum(bg.weighted_ethnicity_hispanic)::numeric /					
       sum(bg.weighted_population_estimate)::numeric,0)  																	as hisp_pct,
    coalesce(sum(bg.weighted_ethnicity_other)::numeric /														
       sum(bg.weighted_population_estimate)::numeric,0)  																	as other_race_pct,
    coalesce(percentile_cont(0.5) within group ( order by bg.age_median),0)     											as median_age,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.households_persons_per::numeric)::numeric /					
       sum(bg.weighted_population_estimate)::numeric,0)																		as people_per_hh,
    coalesce(sum(bg.weighted_population_seasonal_estimate_q_minus_1),0)      												as qc_1,
    coalesce(sum(bg.weighted_population_seasonal_estimate_q_minus_2),0)														as qc_2,
    coalesce(sum(bg.weighted_population_seasonal_estimate_q_minus_3),0)														as qc_3,
    coalesce(sum(bg.weighted_population_seasonal_estimate_q_minus_4),0)														as qc_4,
    coalesce(sum(bg.weighted_population_seasonal_estimate_q_minus_5),0)														as qc_5,
    coalesce(sum(bg.weighted_population_seasonal_estimate_q_minus_6),0)														as qc_6,
    coalesce(sum(bg.weighted_households_estimate::numeric * mortgage_risk_average::numeric)::numeric /					
       sum(bg.weighted_households_estimate)::numeric,0)   																	as avg_mortgage_risk,
    coalesce(sum(bg.weighted_households_in_poverty),0) 																		as pov_pct,
    coalesce((sum(number_of_vehicles)::numeric / NULLIF(sum(households_estimate), 0)::numeric), 0)::numeric							as avg_vehicles,			
    coalesce(sum(bg.weighted_group_quarters_institutionalized) + 					
	 sum(bg.weighted_group_quarters_non_institutionalized),0) 																as gq,
    coalesce(sum(bg.weighted_group_quarters_institutionalized),0) 															as institutionalized_pop,
    coalesce(sum(bg.weighted_group_quarters_non_institutionalized),0)														as non_institutionalized_pop,
    coalesce(sum(bg.weighted_group_quarters_institutionalized) / 					
	 (sum(bg.weighted_group_quarters_institutionalized) + 		
	 sum(bg.weighted_group_quarters_non_institutionalized)),0)  															as institutionalized_pop_pct,
	coalesce(sum(bg.weighted_group_quarters_college) / (sum(bg.weighted_group_quarters_institutionalized) + 					
	 sum(bg.weighted_group_quarters_non_institutionalized)),0)  			                      							as gq_college_pct,
	coalesce(sum(bg.weighted_group_quarters_military) / (sum(bg.weighted_group_quarters_institutionalized) + 					
	 sum(bg.weighted_group_quarters_non_institutionalized)),0) 																as gq_military_pct,
    sum(bg.pcw_region)::numeric  																							as pcw_reg, -- need to know the status of this datapoint with srini
     coalesce(sum(bg.weighted_population_estimate),0) - coalesce(sum(bg.weighted_group_quarters_institutionalized),0) - ( coalesce(sum(bg.weighted_group_quarters_non_institutionalized),0)/2) * sum(bg.pcw_region)::numeric  as sales_potential, 
    max(area_density_class.density_class)::varchar 																			as density_class, -- lookup table 
    CASE WHEN lower(default_fit) = 'conventional' 
    		THEN coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_conventional)::numeric /		
    		sum(bg.weighted_population_estimate)::numeric,0) 
    	 WHEN lower(default_fit) = 'aldi'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_aldi)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
       WHEN lower(default_fit) = 'asian'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_asian)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'club'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_club)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'discount'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_discount)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'hispanic'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_hispanic)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'natural_organic'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_natural_organic)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'quality_service'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_quality_service)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'save_a_lot'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_save_a_lot)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'sprouts'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_sprouts)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'supercenter'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_supercenter)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'trader_joes'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_trader_joes)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'warehouse'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_warehouse)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
		WHEN lower(default_fit) = 'whole_foods'
    		then coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_whole_foods)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)
    		end::numeric  																									as fit_score, 
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_aldi)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as aldi_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg. weighted_fit_score_asian)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0) ::numeric 															as asian_fit,   
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_club)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as club_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_conventional)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)   																	as conv_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_discount)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as disc_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_hispanic)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as hisp_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_natural_organic)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0) 																	as nat_org_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_quality_service)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0) 																	as qual_serv_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_save_a_lot)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as sal_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_sprouts)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as sprouts_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_supercenter)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as sc_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_trader_joes)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0) 																	as tj_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_warehouse)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)  																	as warehouse_fit,
    coalesce(sum(bg.weighted_population_estimate::numeric * bg.weighted_fit_score_whole_foods)::numeric /				
       sum(bg.weighted_population_estimate)::numeric,0)   																	as whole_fds_fit,
			
   	max(all_fits.aldi_fit_count)::numeric 				        															as aldi_count,
    max(all_fits.asian_fit_count)::numeric   																				as asian_count,			
    max(all_fits.club_fit_count)::numeric  			            															as club_count,
    max(all_fits.conventional_fit_count)::numeric  	            															as conventional_count,
    max(all_fits.discount_fit_count)::numeric  		            															as discount_count,
    max(all_fits.hispanic_fit_count)::numeric  		            															as hisp_count,
    max(all_fits.natural_foods_fit_count)::numeric  	        															as nat_org_count,
    max(all_fits.quality_service_fit_count)::numeric 	        															as qual_serv_count,
    max(all_fits.save_a_lot_fit_count)::numeric 		        															as sal_count,
    max(all_fits.sprouts_fit_count)::numeric 		            															as sprouts_count,
    max(all_fits.supercenter_fit_count)::numeric 		        															as sc_count,
    max(all_fits.trader_joes_fit_count)::numeric 		        															as tj_count,
    max(all_fits.warehouse_fit_count)::numeric 		            															as warehouse_count,
    max(all_fits.whole_foods_fit_count)::numeric 		        															as whole_fds_count,
    max(all_fits.all_fit_count)::numeric  																					as total_count,
    (max(all_fits.all_fit_sqft)/coalesce(sum(bg.weighted_population_estimate),0))::numeric  								as sqft_per_pop,
    max(all_fits.conventional_fit_sqft)::numeric  																			as conventional_sqft,
    max(all_fits.discount_fit_sqft)::numeric  																				as discount_sqft,
    max(all_fits.hispanic_fit_sqft)::numeric  																				as hisp_sqft,
    max(all_fits.save_a_lot_fit_sqft)::numeric  																			as sal_sqft,
    max(all_fits.natural_foods_fit_sqft)::numeric  																			as nat_org_sqft,
    max(all_fits.supercenter_fit_sqft)::numeric  																			as sc_sqft,
    max(all_fits.warehouse_fit_sqft)::numeric  																				as warehouse_sqft,
    max(all_fits.club_fit_sqft)::numeric  																					as club_sqft,
    max(all_fits.trader_joes_fit_sqft)::numeric  																			as tj_sqft,
    max(all_fits.whole_foods_fit_sqft)::numeric  																			as whole_fds_sqft,
    max(all_fits.sprouts_fit_sqft)::numeric  																				as sprouts_sqft,
    max(all_fits.aldi_fit_sqft)::numeric  																					as aldi_sqft,
    max(all_fits.asian_fit_sqft)::numeric  																					as asian_sqft,
    max(all_fits.quality_service_fit_sqft)::numeric  																		as qual_serv_sqft,
	max(all_fits.all_fit_sqft)::numeric  																					as total_sqft,	
    banner										   																			as banner_name,
    max(store_fit_size.sizes)::varchar 																						as store_group_fitsize,
    max(banner_spsf.avg_spsf)::numeric  																					as banner_avg_spsf,
    max(fit_size_spsf.fitsize_avg_spsf) ::numeric  																			as store_group_fitsize_avg_spsf,
    sum(bg.weighted_daytime_population) 																					as daytime_pop,
    sum(bg.employees)::numeric  																							as employees,
    sum(bg.establishments)::numeric  																						as establishments,
    sum(bg.traveler_accommodation)::numeric  																				as traveler_accommodation,
    sum(bg.weighted_white_collar)::numeric /					
       sum(bg.weighted_white_collar + bg.weighted_blue_collar)::numeric  													as white_collar,
    sum(bg.weighted_blue_collar)::numeric /																				
       sum(bg.weighted_white_collar + bg.weighted_blue_collar)::numeric	 													as blue_collar,
    sum((0.75*total_area)/all_fits.all_fit_sqft)::numeric																    as so_sqft,
    max(sqft_class_one.class_sqft)::varchar  																				as so_sqft_class,
    max(avg_soft_class_one.avg_ms_sosf_class)::varchar  																	as avg_ms_so_sqft_class,
    null::varchar  																											as state
from 
   bg_weighted_values bg
join area on true	
join area_density_class on true
join store_fit_name on true
join all_fits on true
join store_fit_size on true
join banner_spsf on true
join fit_size_spsf on true
join sqft_class_one on true
join avg_soft_class_one on true
group by default_fit;
END;
$$ LANGUAGE plpgsql;