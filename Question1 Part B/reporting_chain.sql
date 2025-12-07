SELECT e.employee_id as emp_id, e.first_name, e.last_name,
    CONCAT(m1.first_name, ' ', m1.last_name) AS manager_1_full_name,
    CONCAT(m2.first_name, ' ', m2.last_name) AS manager_2_full_name
from tblEmployee e left join tblEmployee m1 
       on e.manager_id = m1.employee_id
left join tblEmployee m2 
       on m1.manager_id = m2.employee_id;