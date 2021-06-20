ELEMENTOR-HOME-ASSIGNMENT

##Task 1:

### How to run:
virtualenv .venv;
. .venv/bin/activate;
pip install -r requirements.txt;
python task1.py

### Details:
module reads configuration from ./config/config.json

####config sample:
``` json
{
  "path_to_input_file": "input/ds1.csv",
  "virus_total_api_key": "key",
  "data_freshness_threshold_minutes": 30, 
  "db_location": "sqlite.db"
}
```

prints the results from the db at the end of execution

##Task2

### the query
``` sql
SELECT * FROM
 ( SELECT *,
         salary - LEAD(salary) OVER () AS salary_diff
   FROM
   (
     SELECT department_name,
            row_number() OVER (PARTITION BY employees.department_id ORDER BY salary DESC) top_employees_order,
            employees.*
     FROM employees LEFT JOIN departments
     ON employees.department_id = departments.department_id
   ) ORDER BY department_name, top_employees_order ASC
 )
 WHERE top_employees_order = 1
 ```

the module task2.py populates some minimal test data and executes the query

### How to run:
python task2.py

##Task3

### the query
``` sql
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
 ```

the module task3.py populates some minimal test data and executes the query

### How to run:
python task3.py