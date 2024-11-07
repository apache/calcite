# Query Examples

## Table of Contents
- [Basic Queries](#basic-queries)
- [Window Functions](#window-functions)
- [Common Table Expressions](#common-table-expressions)
- [Set Operations](#set-operations)
- [Advanced Aggregations](#advanced-aggregations)
- [Complex Joins](#complex-joins)
- [Subqueries](#subqueries)
- [Performance Optimization](#performance-optimization)

## Basic Queries

### Simple Selections
```sql
-- Basic selection
SELECT name, email FROM users;

-- With conditions
SELECT * FROM users WHERE age > 21;

-- With sorting and pagination
SELECT * FROM users 
ORDER BY name DESC 
LIMIT 10 OFFSET 20;

-- Pattern matching
SELECT * FROM users 
WHERE email LIKE '%@company.com' 
  AND name ILIKE 'john%';
```

### Filtering and Sorting
```sql
-- Multiple conditions
SELECT * FROM orders
WHERE status = 'PENDING'
  AND total_amount > 1000
  AND created_at >= CURRENT_DATE - INTERVAL '30' DAY
ORDER BY created_at DESC;

-- IN clauses
SELECT * FROM products
WHERE category IN ('Electronics', 'Books')
  AND price BETWEEN 10 AND 100
ORDER BY price ASC;

-- NULL handling
SELECT * FROM customers
WHERE phone IS NULL
  AND email IS NOT NULL;
```

## Window Functions

### Row Numbers and Ranking
```sql
-- Row numbers within partitions
SELECT 
  department_id,
  employee_name,
  salary,
  ROW_NUMBER() OVER (
    PARTITION BY department_id 
    ORDER BY salary DESC
  ) as salary_rank
FROM employees;

-- Multiple window functions
SELECT 
  department_id,
  employee_name,
  salary,
  RANK() OVER (ORDER BY salary DESC) as overall_rank,
  DENSE_RANK() OVER (
    PARTITION BY department_id 
    ORDER BY salary DESC
  ) as dept_rank,
  PERCENT_RANK() OVER (
    PARTITION BY department_id 
    ORDER BY salary
  ) as percentile
FROM employees;
```

### Moving Aggregates
```sql
-- Moving averages
SELECT 
  date,
  value,
  AVG(value) OVER (
    ORDER BY date 
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ) as moving_avg,
  MIN(value) OVER (
    ORDER BY date 
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ) as moving_min,
  MAX(value) OVER (
    ORDER BY date 
    ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
  ) as moving_max
FROM time_series;

-- Cumulative totals
SELECT 
  date,
  amount,
  SUM(amount) OVER (
    ORDER BY date
    ROWS UNBOUNDED PRECEDING
  ) as running_total
FROM sales;
```

## Common Table Expressions

### Basic CTE
```sql
WITH monthly_sales AS (
  SELECT 
    DATE_TRUNC('month', sale_date) as month,
    SUM(amount) as total_sales
  FROM sales
  GROUP BY DATE_TRUNC('month', sale_date)
)
SELECT 
  month,
  total_sales,
  LAG(total_sales) OVER (ORDER BY month) as prev_month_sales,
  (total_sales - LAG(total_sales) OVER (ORDER BY month)) / 
    LAG(total_sales) OVER (ORDER BY month) * 100 as growth_percent
FROM monthly_sales;
```

### Recursive CTE
```sql
WITH RECURSIVE employee_hierarchy AS (
  -- Base case: top-level employees
  SELECT 
    id, 
    name, 
    manager_id, 
    1 as level,
    ARRAY[name] as path
  FROM employees
  WHERE manager_id IS NULL
  
  UNION ALL
  
  -- Recursive case: employees with managers
  SELECT 
    e.id, 
    e.name, 
    e.manager_id, 
    h.level + 1,
    h.path || e.name
  FROM employees e
  JOIN employee_hierarchy h ON e.manager_id = h.id
  WHERE h.level < 5  -- Prevent infinite recursion
)
SELECT 
  id,
  name,
  level,
  array_to_string(path, ' -> ') as hierarchy_path
FROM employee_hierarchy
ORDER BY path;
```

## Set Operations

### UNION and UNION ALL
```sql
-- Active users from both systems
SELECT username, 'system1' as source
FROM system1_users
WHERE status = 'ACTIVE'
UNION ALL
SELECT username, 'system2' as source
FROM system2_users
WHERE status = 'ACTIVE'
ORDER BY username;

-- Distinct categories across all products
SELECT category FROM products1
UNION
SELECT category FROM products2
ORDER BY category;
```

### INTERSECT and EXCEPT
```sql
-- Find users present in both systems
SELECT username FROM system1_users
INTERSECT
SELECT username FROM system2_users;

-- Find categories in system1 but not in system2
SELECT category FROM system1_products
EXCEPT
SELECT category FROM system2_products;
```

## Advanced Aggregations

### GROUPING SETS
```sql
SELECT 
  COALESCE(region, 'ALL') as region,
  COALESCE(department, 'ALL') as department,
  COALESCE(TO_CHAR(hire_date, 'YYYY'), 'ALL') as year,
  COUNT(*) as employee_count,
  AVG(salary) as avg_salary
FROM employees
GROUP BY GROUPING SETS (
  (region, department, TO_CHAR(hire_date, 'YYYY')),
  (region, department),
  (region),
  ()
)
ORDER BY region, department, year;
```

### CUBE and ROLLUP
```sql
-- CUBE: all possible combinations
SELECT 
  COALESCE(category, 'ALL') as category,
  COALESCE(status, 'ALL') as status,
  COUNT(*) as count,
  SUM(amount) as total_amount
FROM orders
GROUP BY CUBE(category, status);

-- ROLLUP: hierarchical combinations
SELECT 
  COALESCE(year, 'ALL') as year,
  COALESCE(quarter, 'ALL') as quarter,
  COALESCE(month, 'ALL') as month,
  SUM(sales) as total_sales
FROM sales_data
GROUP BY ROLLUP(year, quarter, month);
```

## Complex Joins

### Multi-table Joins
```sql
SELECT 
  o.order_id,
  c.name as customer_name,
  p.name as product_name,
  s.name as salesperson_name,
  o.quantity,
  o.price,
  o.quantity * o.price as total
FROM orders o
JOIN customers c ON o.customer_id = c.id
JOIN products p ON o.product_id = p.id
LEFT JOIN salespeople s ON o.salesperson_id = s.id
WHERE o.order_date >= CURRENT_DATE - INTERVAL '30' DAY;
```

### Self Joins
```sql
-- Employee hierarchy with multiple levels
SELECT 
  e.name as employee,
  m1.name as manager,
  m2.name as managers_manager
FROM employees e
LEFT JOIN employees m1 ON e.manager_id = m1.id
LEFT JOIN employees m2 ON m1.manager_id = m2.id
ORDER BY m2.name, m1.name, e.name;
```

## Subqueries

### IN Clause
```sql
SELECT 
  product_name,
  price,
  category
FROM products
WHERE category IN (
  SELECT category
  FROM product_categories
  WHERE active = true
  AND promotion_eligible = true
);
```

### EXISTS Clause
```sql
SELECT 
  customer_id,
  name,
  email
FROM customers c
WHERE EXISTS (
  SELECT 1
  FROM orders o
  WHERE o.customer_id = c.id
  AND o.amount > 1000
  AND o.created_at >= CURRENT_DATE - INTERVAL '90' DAY
);
```

### Scalar Subqueries
```sql
SELECT 
  department_id,
  department_name,
  employee_count,
  employee_count / (
    SELECT AVG(dept_count)
    FROM (
      SELECT COUNT(*) as dept_count
      FROM employees
      GROUP BY department_id
    ) t
  ) as relative_size
FROM (
  SELECT 
    d.id as department_id,
    d.name as department_name,
    COUNT(e.id) as employee_count
  FROM departments d
  LEFT JOIN employees e ON d.id = e.department_id
  GROUP BY d.id, d.name
) dept_stats;
```

## Performance Optimization

### Indexed Queries
```sql
-- Use indexes effectively
SELECT *
FROM orders
WHERE created_at >= CURRENT_DATE - INTERVAL '30' DAY
  AND status = 'PENDING'
ORDER BY created_at DESC;

-- Include all indexed columns in equality predicates
SELECT *
FROM products
WHERE category = 'Electronics'
  AND brand = 'Apple'
  AND price > 500
ORDER BY name;
```

### Efficient Aggregations
```sql
-- Pre-aggregate in CTE
WITH daily_stats AS (
  SELECT
    DATE_TRUNC('day', created_at) as day,
    status,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
  FROM orders
  WHERE created_at >= CURRENT_DATE - INTERVAL '90' DAY
  GROUP BY DATE_TRUNC('day', created_at), status
)
SELECT 
  day,
  status,
  order_count,
  total_amount,
  AVG(total_amount) OVER (
    PARTITION BY status
    ORDER BY day
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) as moving_avg_amount
FROM daily_stats
ORDER BY status, day;
```

See also:
- [Features](features.md)
- [SQL Processing](sql-processing.md)
- [Type System](type-system.md)
