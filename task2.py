'''

query:

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
'''

from util import create_connection

db_con = create_connection("sqlite.db")
db_cur = db_con.cursor()


db_cur.execute(""" DROP table  IF EXISTS employees;""")
db_cur.execute(""" DROP table  IF EXISTS departments;""")

db_cur.execute(""" CREATE TABLE IF NOT EXISTS employees(
                   employee_id integer, 
                   first_name text, 
                   last_name text, 
                   hire_date text, 
                   salary integer, 
                   manager_id integer, 
                   department_id integer) 
               """)
db_cur.execute(""" CREATE TABLE IF NOT EXISTS departments(
                   location_id integer,                 
                   department_name text, 
                   department_id integer) 
               """)

db_cur.execute(""" 
                INSERT INTO  departments(location_id, 
                                         department_name, 
                                         department_id) 
                             VALUES(1, 'dname1', 1),
                                   (2, 'dname2', 2),
                                   (1, 'dname3', 3)
              """)

db_cur.execute(""" 
                INSERT INTO employees(employee_id, 
                                      first_name, 
                                      last_name, 
                                      hire_date, 
                                      salary, 
                                      manager_id, 
                                      department_id) 
                        VALUES(1, 'name1', 'last1', '2021-02-02', 10000, 1, 1),
                              (2, 'name2', 'last2', '2021-02-02', 9000, 1, 1),
                              (3, 'name3', 'last3', '2021-02-02', 20000, 1, 1),
                              (4, 'name4', 'last4', '2021-01-02', 15000, 1, 2),
                              (5, 'name5', 'last5', '2021-01-03', 12000, 1, 2),
                              (6, 'name6', 'last6', '2021-01-04', 13000, 1, 2)                              
               """)
db_con.commit()

res = db_cur.execute("""                         
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
                """)

print(res.fetchall())
db_con.close()
