'''
Task one:- 

--> Adjust Size to reflect estimated Sales Area  
        All Fit Types, with the exception of  SuperCenter and Club, will use 72.5% of Total Area to calculate Sales Area 
        SuperCenter will use 45% 
        Club will use 55% 



(0.75*total_area) as area_sales,

case 
    when lower(default_fit) != 'SuperCenter' and lower(default_fit) != 'Club' then (0.45 * total_area) 
    when lower(default_fit) = 'SuperCenter' then (0.45 * total_area) 
    when lower(default_fit) = 'Club' then (0.55 * total_area) 
    end as area_sales
'''
