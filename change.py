'''
state ---> find

'''

'''
avg_vehicles   Aggregate Number of Vehicles/Households Estimate CrYr.
pcw_reg  à temp_pcw  à this may have old data à needed to loaded
sales_potential  à Apparently Puneet knows how to get ( based on earlier discussions with Jake)
fit_score à this is one of the 14 fit scores ( which fit score is determined by banner fit type)
Employees - W016001    à need to be added to workplace table and load from workplace datafile
Establishments - W015001  à need to be added to workplace table and load load from workplace datafile
Traveler accommodation- W157211 à  need to be added to workplace table and load from workplace datafile
'''



'''

FIT_Conv int4,
FIT_Discount int4,
FIT_Hispanic int4,
FIT_SaveALot int4,
FIT_NatOrg int4,
FIT_QualServ int4,
FIT_SuperC int4,
FIT_Warehouse int4,
FIT_Club int4,
FIT_TrJoes int4,
FIT_WholeFds int4,
FIT_Sprouts int4,
FIT_Aldi int4,
FIT_Asian int4,
FIT_Kroger int4,
PCW_USA_Avg int4,
PCW_Region int4,
'''





'''


pcw_reg -> temp_store_fits table 
sales_potential -> Population - Institutionalized Population - (Non-Institutionalized Population /2) * PCW
fit_score 
state

CASE 
    WHEN store_fit = 'conventional' THEN fit_score_conv
    WHEN store_fit = 'aldi' THEN fit_score_aldi
    WHEN store_fit = 'club' THEN fit_score_club
    WHEN store_fit = 'discount' THEN fit_score_discount
    WHEN store_fit = 'hispanic' THEN fit_score_hispanic
    WHEN store_fit = 'natural_organic' THEN fit_score_natural_organic
    WHEN store_fit = 'quality_service' THEN fit_score_quality_service
    WHEN store_fit = 'save_a_lot' THEN fit_score_save_a_lot
    WHEN store_fit = 'sprouts' THEN fit_score_sprouts
    WHEN store_fit = 'supercenter' THEN fit_score_supercenter
    WHEN store_fit = 'trader_joes' THEN fit_score_trader_joes
    WHEN store_fit = 'warehouse' THEN fit_score_warehouse
    WHEN store_fit = 'whole_foods' THEN fit_score_whole_foods
    WHEN store_fit = 'asian' THEN fit_score_asian
    WHEN store_fit = 'kroger' THEN fit_score_kroger
END;


task one:- need to add pcw columns in the popstats table taken by temp_store_fits table
task two:- add pcw_reg in the main query


'''



