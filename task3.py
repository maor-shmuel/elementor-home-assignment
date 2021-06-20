'''

query:

SELECT site,
    SUM(number_of_visitors) as total_visitors_count,
    SUM(CASE
          WHEN is_promotion_date = 1 THEN number_of_visitors
          ELSE 0
        END) * 100 / sum(number_of_visitors) AS percent
FROM
(
 SELECT site_visitors.*,
        CASE
          WHEN (date >= start_date AND date <= end_date) THEN 1
          ELSE 0
        END as is_promotion_date
 FROM site_visitors
 LEFT JOIN promotion_dates
 ON site_visitors.site = promotion_dates.site
)
GROUP BY site;

'''

from util import create_connection

db_con = create_connection("sqlite.db")
db_cur = db_con.cursor()

db_cur.execute(""" DROP table  IF EXISTS site_visitors;""")
db_cur.execute(""" DROP table  IF EXISTS promotion_dates;""")
db_cur.execute("""  
                   CREATE TABLE IF NOT EXISTS site_visitors(
                   date text, 
                   site text,                 
                   number_of_visitors integer 
                   ) 
               """)
db_cur.execute(""" 
                   CREATE TABLE IF NOT EXISTS promotion_dates(
                   start_date text,                 
                   end_date text,
                   site text,
                   promotion_code text) 
               """)


db_cur.execute(""" INSERT INTO site_visitors(date, 
                                             site, 
                                             number_of_visitors) 
                           VALUES('2021-03-05', 'www.site1.com', 1000),
                                 ('2021-03-06', 'www.site1.com', 908),
                                 ('2021-03-07', 'www.site1.com', 1230),
                                 ('2021-03-05', 'www.site2.com', 2300),
                                 ('2021-03-06', 'www.site2.com', 2103),
                                 ('2021-03-07', 'www.site2.com', 1987),
                                 ('2021-03-05', 'www.site3.com', 230),
                                 ('2021-03-06', 'www.site3.com', 119),
                                 ('2021-03-07', 'www.site3.com', 122)""")


db_cur.execute(""" INSERT INTO promotion_dates(start_date, 
                                               end_date, 
                                               site, 
                                               promotion_code) 
                           VALUES('2021-03-04', '2021-03-06', 'www.site1.com', '1'),
                                 ('2021-03-04', '2021-03-06', 'www.site2.com', '2'),
                                 ('2021-03-04', '2021-03-06', 'www.site3.com', '3')
                                 """)


db_con.commit()

res = db_cur.execute("""                                                                                       
                     SELECT site, 
                            SUM(number_of_visitors) as total_visitors_count,
                            SUM(CASE 
                                  WHEN is_promotion_date = 1 THEN number_of_visitors 
                                  ELSE 0 
                                END) AS promotions_count,
                            SUM(CASE 
                                  WHEN is_promotion_date = 1 THEN number_of_visitors 
                                  ELSE 0 
                                END) * 100 / sum(number_of_visitors) AS percent
                     FROM 
                       ( SELECT site_visitors.*, 
                                CASE 
                                  WHEN (date >= start_date AND date <= end_date) THEN 1 
                                  ELSE 0 
                                END as is_promotion_date  
                         FROM site_visitors 
                         LEFT JOIN promotion_dates 
                         ON site_visitors.site = promotion_dates.site
                       )
                     GROUP BY site;                                           
                     """)
print(res.fetchall())
db_con.close()
