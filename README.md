# max_to_min() agreegate func on C

Aggregate function that generates a custom-formatted text for a given numerical column, displaying the minimum and maximum values of that column.

- [max\_to\_min() agreegate func on C](#max_to_min-agreegate-func-on-c)
  - [Limitation](#limitation)
  - [Installation](#installation)
    - [Prerequisites](#prerequisites)
    - [Installation](#installation-1)
  - [Configuration](#configuration)
  - [Examples](#examples)
  - [Tests](#tests)
  - [Notes](#notes)

## Limitation

-  Only number types
-  Order of min and max values fixed

## Installation

### Prerequisites
- Tested in
  - Extension  public docker container `postgres:15`
  - OS: `Debian GNU/Linux 12 (bookworm)`

```bash
$ apt install gcc ;
$ apt install make ;
$ apt install postgresql-server-dev-15 ;
```
### Installation
In target server 
```bash
$ git clone https://github.com/skilyazhnev/mtm_c.git
$ cd ./mtm_c/mtm
$ make ;
$ sudo make install ;
```
In target database run
```sql
target_db=# create extension mtm version "0.1" ;
```

## Configuration

Parameters can be configured in `postgresql.conf` or in session  

- `mtm.output_format`: Output format based on C `printf like format` where %s `is` a placeholder for a values (first `%s` always MAX and second %s always MIN).
 <br>
  - Ensure that the format string has exactly two `%s` placeholders to match the number of arguments provided.
  
**important**: I didn't add the ability to change the order of maximum and minimum because this would begin to contradict the name of the function <br>

## Examples

Clear calling
```sql
SELECT max_to_min(val)
    FROM (VALUES(5),(3),(6),(7),(9),(10),(7)) t(val);

max_to_min 
-----------
 10 -> 3
```
Change output format in session
```sql
-- Change output format in session
set mtm.output_format='%s -> %s';
select max_to_min(val), text from t group by text;
  max_to_min | text
------------+------
 0 -> 0     | Q
 10 -> 2    | B
 1 -> 1     | k
 4 -> 1     | A
 -1 -> -1   | l
 12 -> 1    | F
(6 rows)

-- Change output format in session
SET mtm.output_format='{ "max": %I, "min": %I }';
SELECT max_to_min(val), text FROM t GROUP BY text;

          max_to_min          | text
------------------------------+------
 { "max": "0", "min": "0" }   | Q
 { "max": "10", "min": "2" }  | B
 { "max": "1", "min": "1" }   | k
 { "max": "4", "min": "1" }   | A
 { "max": "-1", "min": "-1" } | l
 { "max": "12", "min": "1" }  | F
(6 rows)
```

## Tests

Check basic input\output parameters of function
```bash
$ psql -d <target_db> -f ./mtm_tests/simple_test.sql
```

```sql
$ psql -d db_name -f ./mtm_tests/simple_test.sql
Сhecking a monotonic data:
 text |                 main                 |                target                | check
------+--------------------------------------+--------------------------------------+-------
 B    | 9999.21466834803 -> 8.2752532608743  | 9999.21466834803 -> 8.2752532608743  | Ok
 C    | 9986.48740927796 -> 3.46478478020273 | 9986.48740927796 -> 3.46478478020273 | Ok
 Q    | 9998.5422345872 -> 12.0614781488348  | 9998.5422345872 -> 12.0614781488348  | Ok
 A    | 9985.19874414025 -> 3.66361051441499 | 9985.19874414025 -> 3.66361051441499 | Ok

Checking data with NULL:
 check
-------
 Ok

Checking full NULL data:
 text | max_to_min | check
------+------------+-------
 B    |            | Ok
 A    |            | Ok
```
## Notes

- Tested on:
  - PG15 
- `NaN` values will be excluded for numeric types 
