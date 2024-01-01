/* Management is trying to better understand who their highest paid employees are. They
currently have identified their first and second most paid employees, but would like to know
the third most. Your task is to find the employee with the third highest compensation as well as
provide their name, salary, division, region, and manager's name from the company's internal
database.


Column Headers:

employee_id: unique ID for each employee
employee_name: full name of each employee
division_id: unique ID for each division
manager_id: for each employee, the ID of their manager if they have one
salary: salary of each employee
division_name: the full text name of each division
division_region: the region in which each division is located


Example Table Output:

Employee
+-------------+---------------+-------------+------------+--------+
| employee_id | employee_name | division_id | manager_id | salary |
+-------------+---------------+-------------+------------+--------+
| 054612317   | John_Smith    | div_103     | 134215748  | 40000  |
| 345461456   | Jane_Doe      | div_102     | 958472937  | 55000  |
...

Division
+-------------+---------------+-----------------+
| division_id | division_name | division_region |
+-------------+---------------+-----------------+
| div_101     | Accounting    | APAC            |
| div_102     | IT            | EMEA            |
...
*/

/*---------------------------------- Creating Tables ------------------------------------*/
/*---------------------------------------------------------------------------------------*/
/*---------------------------------------------------------------------------------------*/

CREATE TABLE Employee (
    employee_id VARCHAR(9) NOT NULL,
    employee_name VARCHAR(9),
    division_id VARCHAR(7) NOT NULL,
    manager_id VARCHAR(9),
    salary int
);

CREATE TABLE Division (
    division_id VARCHAR(7) NOT NULL,
    division_name VARCHAR(10) NOT NULL,
    division_region VARCHAR(7) NOT NULL
);

INSERT INTO Employee
    (employee_id, employee_name, division_id, manager_id, salary)
VALUES
    (054612317, 'John_Smith', 'div_103', 134215748, 40000),
    (345461456, 'Jane_Doe', 'div_102', 958472937, 55000),
    (958472937, 'James_Jones', 'div_102', NULL, 75000),
    (646873937, 'Sally_Cruz', 'div_101', 134215748, 45000),
    (134215748, 'Michael_Lee', 'div_101', NULL, 70000);

INSERT INTO Division
    (division_id, division_name, division_region)
VALUES
    ('div_101', 'Accounting', 'APAC'),
    ('div_102', 'IT', 'EMEA'),
    ('div_103', 'Legal', 'Americas');


select * from (select employee.employee_name as name, employee.salary as salary, division.division_name as division,
 division.division_region as region, employee.manager_id as managerName from division inner join employee on
 employee.division_id = division.division_id order by salary desc Limit 3) as emp_division_info order by
 salary asc Limit 1;