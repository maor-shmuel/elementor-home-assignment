select * from 
(
	select *, salary - LAG(salary) OVER (ORDER BY salary) as salary_diff from 
	(
		select department_name, row_number() OVER (PARTITION BY employees.department_id ORDER BY salary DESC) top_employees_order, employees.*  from employees left join departments 
		on employees.department_id = departments.department_id
	) order by department_name, top_employees_order ASC 
) 
where top_employees_order = 1""")
