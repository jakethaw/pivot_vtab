# pivot_vtab
Implementation of an SQLite3 pivot virtual table

To compile with gcc as a run-time loadable extension:

```bash
  UNIX-like : gcc -g -O3 -fPIC -shared pivot_vtab.c -o pivot_vtab.so
  Mac       : gcc -g -O3 -fPIC -dynamiclib pivot_vtab.c -o pivot_vtab.dylib
  Windows   : gcc -g -O3 -shared pivot_vtab.c -o pivot_vtab.dll
```
## Usage example: 

```sql
CREATE VIRTUAL TABLE pivot USING pivot_vtab(
  (SELECT id r_id FROM r),        -- Pivot table key query
  (SELECT id c_id, name FROM c),  -- Pivot table column definition query
  (SELECT val                     -- Pivot query
     FROM x 
    WHERE r_id = ?1
      AND c_id = ?2)
);
```

See script below for a more detailed usage example, and an expanded 
definition of the virtual table arguments

```sql
--
-- The following usage example can be run using the SQLite shell
--

.load ./pivot_vtab
.headers on
.mode column

-- Rows
CREATE TABLE r AS
SELECT 1 id UNION SELECT 2 UNION SELECT 3;

-- Columns
CREATE TABLE c(
  id   INTEGER PRIMARY KEY,
  name TEXT
);
INSERT INTO c (name) VALUES
('a'),('b'),('c'),('d');

CREATE TABLE x(
  r_id INT,
  c_id INT,
  val  TEXT
);
INSERT INTO x (r_id, c_id, val)
SELECT r.id, c.id, c.name || r.id
  FROM c, r;

.width 4 4 4
SELECT * FROM x;

-- r_id  c_id  val 
-- ----  ----  ----
-- 1     1     a1  
-- 2     1     a2  
-- 3     1     a3  
-- 1     2     b1  
-- 2     2     b2  
-- 3     2     b3  
-- 1     3     c1  
-- 2     3     c2  
-- 3     3     c3  
-- 1     4     d1  
-- 2     4     d2  
-- 3     4     d3  

.width 4 5 5 5 5

CREATE VIRTUAL TABLE pivot USING pivot_vtab(
  --
  -- Pivot table row key query
  --
  -- Defines first column of the pivot table
  --
  -- Required to perform a full table scan of the pivot table. This is run when
  -- not joining or filtering by the pivot table key.
  --
  -- The first column name in this query will become the name of the pivot table key column.
  -- The value of the pivot table key column is provided to the pivot query as ?1.
  --
 (SELECT id r_id -- pivot table key
    FROM r),
  
  --
  -- Pivot table column definition query
  --
  -- Defines second+ column(s) of the pivot table
  --
  -- This query should return pivot table column key/name pairs.
  -- It is run during vtab creation to define the pivot table column names.
  --
  -- The first column of this query is the pivot column key, and is provided
  -- to the pivot query as ?2
  --
  -- The second column of this query is used to name the pivot table columns.
  -- This column is required to return unique values.
  -- 
  -- Changes to this query can only be propagated by dropping and 
  -- re-creating the virtual table
  --
 (SELECT id c_id,   -- pivot column key - can be referenced in pivot query as ?2
         name       -- pivot column name
    FROM c),
    
  --
  -- Pivot query
  --
  -- This query should define a single value in the pivot table when
  -- filtered by the pivot table row key (1?) and a column key (2?)
  --
 (SELECT val
    FROM x 
   WHERE r_id = ?1
     AND c_id = ?2)
);

SELECT *
  FROM pivot;

-- r_id  a      b      c      d    
-- ----  -----  -----  -----  -----
-- 1     a1     b1     c1     d1   
-- 2     a2     b2     c2     d2   
-- 3     a3     b3     c3     d3   
  
UPDATE x
   SET val = 'hello'
 WHERE c_id = 3
   AND r_id = 2;
   
UPDATE x
   SET val = 'world'
 WHERE c_id = 4
   AND r_id = 2;
 
DELETE 
  FROM x
 WHERE c_id = 2
   AND r_id = 3;

SELECT *
  FROM pivot;

-- r_id  a      b      c      d    
-- ----  -----  -----  -----  -----
-- 1     a1     b1     c1     d1   
-- 2     a2     b2     hello  world   
-- 3     a3            c3     d3  
```
