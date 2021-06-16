with total_traffic_view as
(
	select site, sum(number_of_visitors) as site_traffic group by site
), promotion_dates_traffic as
(
	select site, sum(number_of_visitors), as promotion_traffic from site_visitors left join promotions_dates on site = site and date >= start_date and date <= end_date 
)

select site, promotion_traffic / site_traffic * 100 as promotion_traffic_precentage


