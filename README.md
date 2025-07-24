## The substrait integration is experimental. The source code is publicly available and you are free to use it under the licensing terms - but the source code is provided as-is. We do not have the capacity to engage with issues or pull requests from external contributors at present. Support for the extension is currently only available [on request.](https://duckdblabs.com/contact/) 

# Substrait - DuckDB
Substrait - DuckDB is an extension that provides substrait support to [DuckDB](https://www.duckdb.org).
The main goal of this extension is to support a both production and consumption of substrait query plans in DuckDB.

To build, type 
```
git submodule update --init --recursive
git submodule update --remote
make
```

To run, run the bundled `duckdb` shell:
```
 ./duckdb/build/release/duckdb 
```

Then, load the Substrait - DuckDB extension like so:
```SQL
LOAD 'build/release/substrait.duckdb_extension';
```

## Support
This extension is mainly supported in 3 different APIs. 1) The SQL API, 2) The Python API, 3) The R API.
Here we depict how to consume and produce substrait query plans in each API.

### SQL
In the SQL API, users can generate substrait plans (into a blob or a JSON) and consume substrait plans.

Before using the extension, you must always properly install and load it. 
To install and load the released version of the substrait library, you must execute the following SQL commands.
```sql
INSTALL substrait;
LOAD substrait;
```

1) Blob Generation
     
     To generate a substrait blob the ```get_substrait(SQL)``` function must be called with a valid SQL select query.
     ```sql
     CREATE TABLE crossfit (exercise text,difficulty_level int);
     INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);
     
     CALL get_substrait('select count(exercise) as exercise from crossfit where difficulty_level <=5');
     ----
     \x12\x09\x1A\x07\x10\x01\x1A\x03lte\x12\x11\x1A\x0F\x10\x02\x1A\x0Bis_not_null\x12\x09\x1A\x07\x10\x03\x1A\x03and\x12\x10\x1A\x0E\x10\x04\x1A\x0Acount_star\x1A\xCB\x01\x12\xC8\x01\x0A\xBB\x01:\xB8\x01\x12\xAB\x01"\xA8\x01\x12\x97\x01\x0A\x94\x01\x12.\x0A\x08exercise\x0A\x0Fdifficulty_level\x12\x11\x0A\x07\xB2\x01\x04\x08\x0D\x18\x01\x0A\x04*\x02\x10\x01\x18\x02\x1AJ\x1AH\x08\x03\x1A\x04\x0A\x02\x10\x01""\x1A \x1A\x1E\x08\x01\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x06\x1A\x04\x0A\x02(\x05"\x1A\x1A\x18\x1A\x16\x08\x02\x1A\x04*\x02\x10\x01"\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01"\x00"\x0A\x0A\x06\x0A\x02\x08\x01\x0A\x00\x10\x01:\x0A\x0A\x08crossfit\x1A\x00"\x0A\x0A\x08\x08\x04*\x04:\x02\x10\x01\x1A\x08\x12\x06\x0A\x02\x12\x00"\x00\x12\x08exercise
    ```
2) Json Generation
     
     To generate a json representing  the substrait plan the ```get_substrait_json(SQL)``` function must be called with a valid SQL select query.
     ```sql
     CALL get_substrait_json('select count(exercise) as exercise from crossfit where difficulty_level <=5');
     ----
     {"extensions":[{"extensionFunction":{"functionAnchor":1,"name":"lte"}},{"extensionFunction":{"functionAnchor":2,"name":"is_not_null"}},{"extensionFunction":{"functionAnchor":3,"name":"and"}},{"extensionFunction":{"functionAnchor":4,"name":"count_star"}}],"relations":[{"root":{"input":{"project":{"input":{"aggregate":{"input":{"read":{"baseSchema":{"names":["exercise","difficulty_level"],"struct":{"types":[{"varchar":{"length":13,"nullability":"NULLABILITY_NULLABLE"}},{"i32":{"nullability":"NULLABILITY_NULLABLE"}}],"nullability":"NULLABILITY_REQUIRED"}},"filter":{"scalarFunction":{"functionReference":3,"outputType":{"bool":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"scalarFunction":{"functionReference":1,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}},{"value":{"literal":{"i32":5}}}]}}},{"value":{"scalarFunction":{"functionReference":2,"outputType":{"i32":{"nullability":"NULLABILITY_NULLABLE"}},"arguments":[{"value":{"selection":{"directReference":{"structField":{"field":1}},"rootReference":{}}}}]}}}]}},"projection":{"select":{"structItems":[{"field":1},{}]},"maintainSingularStruct":true},"namedTable":{"names":["crossfit"]}}},"groupings":[{}],"measures":[{"measure":{"functionReference":4,"outputType":{"i64":{"nullability":"NULLABILITY_NULLABLE"}}}}]}},"expressions":[{"selection":{"directReference":{"structField":{}},"rootReference":{}}}]}},"names":["exercise"]}}]}
     ```
3) Blob Consumption
     
     To consume a substrait blob the ```from_substrait(blob)``` function must be called with a valid substrait BLOB plan.
     ```sql
     CALL from_substrait('\x12\x07\x1A\x05\x1A\x03lte\x12\x11\x1A\x0F\x10\x01\x1A\x0Bis_not_null\x12\x09\x1A\x07\x10\x02\x1A\x03and\x12\x10\x1A\x0E\x10\x03\x1A\x0Acount_star\x1A\xA4\x01\x12\xA1\x01\x0A\x94\x01:\x91\x01\x12\x86\x01"\x83\x01\x12y:w\x12c\x12a\x12+\x0A)\x12\x1B\x0A\x08exercise\x0A\x0Fdifficulty_level:\x0A\x0A\x08crossfit\x1A2\x1A0\x08\x02"\x18\x1A\x16\x1A\x14"\x0A\x1A\x08\x12\x06\x0A\x04\x12\x02\x08\x01"\x06\x1A\x04\x0A\x02(\x05"\x12\x1A\x10\x1A\x0E\x08\x01"\x0A\x1A\x08\x12\x06\x0A\x04\x12\x02\x08\x01\x1A\x08\x12\x06\x0A\x04\x12\x02\x08\x01\x1A\x06\x12\x04\x0A\x02\x12\x00\x1A\x00"\x04\x0A\x02\x08\x03\x1A\x06\x12\x04\x0A\x02\x12\x00\x12\x08exercise'::BLOB);
     ----
     2
   ```

#### Controlling Query Optimization

The `get_substrait(SQL)` and `get_substrait_json(SQL)` functions accept an optional parameter, `enable_optimizer`,
to explicitly enable or disable query optimization when generating Substrait:

```sql
CALL get_substrait('select count(exercise) as exercise from crossfit', enable_optimizer=false);
CALL get_substrait_json('select count(exercise) as exercise from crossfit', enable_optimizer=true);
```

If `enable_optimizer` is not specified, it is inferred from the connection-level settings: if query optimization
is disabled at the connection level (e.g. using `PRAGMA disable_optimizer`), the Substrait generation functions
will not optimize the query; otherwise, they will.

If any specific optimizers are disabled at the connection level (e.g. using `SET disabled_optimizers TO '...'`),
they will also be disabled when generating Substrait.

The `from_substrait(blob)` function **always** respects the connection-level settings when deciding whether to
optimize a Substrait plan before executing it.


### End-to-end example
```bash
# First install the substrait-duckdb
# convert s modified query (substrait-duckdb doesn't support string yet) - 6d.sql to a blob or json format
duckdb -line -c "LOAD '/home/pei/Project/substrait/duckdb/build/release/extension/substrait/substrait.duckdb_extension'; CALL get_substrait('SELECT MIN(keyword.keyword),        MIN(name.name),        MIN(title.title) FROM cast_info,      keyword,      movie_keyword,      name,      title WHERE title.kind_id IN (1,2,6,7)       AND keyword.id = movie_keyword.keyword_id   AND title.id = movie_keyword.movie_id   AND title.id = cast_info.movie_id   AND cast_info.movie_id = movie_keyword.movie_id   AND name.id = cast_info.person_id;');" imdb.db
# OR duckdb -line -c "LOAD '/home/pei/Project/substrait/duckdb/build/release/extension/substrait/substrait.duckdb_extension'; CALL get_substrait_json('SELECT MIN(keyword.keyword),        MIN(name.name),        MIN(title.title) FROM cast_info,      keyword,      movie_keyword,      name,      title WHERE title.kind_id IN (1,2,6,7)  AND keyword.id = movie_keyword.keyword_id   AND title.id = movie_keyword.movie_id   AND title.id = cast_info.movie_id   AND cast_info.movie_id = movie_keyword.movie_id   AND name.id = cast_info.person_id;');" imdb.db

# From another terminal, execute the blob or json. But not sure why it cannot work in one command line
# E.g.
duckdb -line imdb.db
LOAD '/home/pei/Project/substrait/duckdb/build/release/extension/substrait/substrait.duckdb_extension';
call from_substrait('\x12\x09\x1A\x07\x10\x01\x1A\x03gte\x12\x09\x1A\x07\x10\x02\x1A\x03lte\x12\x09\x1A\x07\x10\x03\x1A\x03and\x12\x11\x1A\x0F\x10\x04\x1A\x0Bis_not_null\x12\x0B\x1A\x09\x10\x05\x1A\x05equal\x12\x09\x1A\x07\x10\x06\x1A\x03min\x1A\xCA\x0D\x12\xC7\x0D\x0A\x9F\x0D:\x9C\x0D\x12\xF7\x0C\x22\xF4\x0C\x12\xA1\x0C:\x9E\x0C\x12\xC9\x0B2\xC6\x0B\x12\x9B\x09:\x98\x09\x12\xDB\x082\xD8\x08\x12\x91\x02\x0A\x8E\x02\x12p\x0A\x02id\x0A\x09person_id\x0A\x08movie_id\x0A\x0Eperson_role_id\x0A\x04note\x0A\x08nr_order\x0A\x07role_id\x12,\x0A\x04*\x02\x10\x02\x0A\x04*\x02\x10\x02\x0A\x04*\x02\x10\x02\x0A\x04*\x02\x10\x01\x0A\x04b\x02\x10\x01\x0A\x04*\x02\x10\x01\x0A\x04*\x02\x10\x02\x18\x02\x1A\x7F\x1A}\x08\x03\x1A\x04\x0A\x02\x10\x01\x22W\x1AU\x1AS\x08\x03\x1A\x04\x0A\x02\x10\x01\x22\x22\x1A \x1A\x1E\x08\x01\x1A\x04*\x02\x10\x01\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x02\x22\x00\x22\x06\x1A\x04\x0A\x02(\x02\x22%\x1A#\x1A!\x08\x02\x1A\x04*\x02\x10\x01\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x02\x22\x00\x22\x09\x1A\x07\x0A\x05(\x93\x96\x9A\x01\x22\x1A\x1A\x18\x1A\x16\x08\x04\x1A\x04*\x02\x10\x01\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x02\x22\x00\x22\x0C\x0A\x08\x0A\x02\x08\x02\x0A\x02\x08\x01\x10\x01:\x0B\x0A\x09cast_info\x1A\x99\x06:\x96\x06\x12\xCD\x052\xCA\x05\x12\xD0\x04:\xCD\x04\x12\x9C\x042\x99\x04\x12S\x0AQ\x120\x0A\x02id\x0A\x08movie_id\x0A\x0Akeyword_id\x12\x14\x0A\x04*\x02\x10\x02\x0A\x04*\x02\x10\x02\x0A\x04*\x02\x10\x02\x18\x02\x22\x0C\x0A\x08\x0A\x02\x08\x02\x0A\x02\x08\x01\x10\x01:\x0F\x0A\x0Dmovie_keyword\x1A\x97\x03\x12\x94\x03\x12\xEB\x02\x0A\xE8\x02\x12\xD1\x01\x0A\x02id\x0A\x05title\x0A\x0Aimdb_index\x0A\x07kind_id\x0A\x0Fproduction_year\x0A\x07imdb_id\x0A\x0Dphonetic_code\x0A\x0Depisode_of_id\x0A\x09season_nr\x0A\x0Aepisode_nr\x0A\x0Cseries_years\x0A\x06md5sum\x12J\x0A\x04*\x02\x10\x02\x0A\x04b\x02\x10\x02\x0A\x04b\x02\x10\x01\x0A\x04*\x02\x10\x02\x0A\x04*\x02\x10\x01\x0A\x04*\x02\x10\x01\x0A\x04b\x02\x10\x01\x0A\x04*\x02\x10\x01\x0A\x04*\x02\x10\x01\x0A\x04*\x02\x10\x01\x0A\x04b\x02\x10\x01\x0A\x04b\x02\x10\x01\x18\x02\x1Ay\x1Aw\x08\x03\x1A\x04\x0A\x02\x10\x01\x22S\x1AQ\x1AO\x08\x03\x1A\x04\x0A\x02\x10\x01\x22 \x1A\x1E\x1A\x1C\x08\x01\x1A\x04*\x02\x10\x01\x22\x0A\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x22\x06\x1A\x04\x0A\x02(\x02\x22#\x1A!\x1A\x1F\x08\x02\x1A\x04*\x02\x10\x01\x22\x0A\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x22\x09\x1A\x07\x0A\x05(\x93\x96\x9A\x01\x22\x18\x1A\x16\x1A\x14\x08\x04\x1A\x04*\x02\x10\x01\x22\x0A\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x22\x0E\x0A\x0A\x0A\x02\x08\x03\x0A\x00\x0A\x02\x08\x01\x10\x01:\x07\x0A\x05title\x1A$B\x22\x0A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x12\x04\x0A\x02(\x01\x12\x04\x0A\x02(\x02\x12\x04\x0A\x02(\x06\x12\x04\x0A\x02(\x07\x22&\x1A$\x08\x05\x1A\x04\x0A\x02\x10\x01\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01\x22\x00\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x03\x22\x000\x01\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x03\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x04\x22\x00\x1AM\x0AK\x122\x0A\x02id\x0A\x07keyword\x0A\x0Dphonetic_code\x12\x14\x0A\x04*\x02\x10\x02\x0A\x04b\x02\x10\x02\x0A\x04b\x02\x10\x01\x18\x02\x22\x0A\x0A\x06\x0A\x00\x0A\x02\x08\x01\x10\x01:\x09\x0A\x07keyword\x22$\x1A\x22\x08\x05\x1A\x04\x0A\x02\x10\x01\x22\x0A\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x04\x22\x000\x01\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x02\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x03\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x04\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x05\x22\x00\x22$\x1A\x22\x08\x05\x1A\x04\x0A\x02\x10\x01\x22\x0A\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x03\x22\x000\x01\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x03\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x05\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x07\x22\x00\x1A\xFB\x01\x0A\xF8\x01\x12\x96\x01\x0A\x02id\x0A\x04name\x0A\x0Aimdb_index\x0A\x07imdb_id\x0A\x06gender\x0A\x0Dname_pcode_cf\x0A\x0Dname_pcode_nf\x0A\x0Dsurname_pcode\x0A\x06md5sum\x128\x0A\x04*\x02\x10\x02\x0A\x04b\x02\x10\x02\x0A\x04b\x02\x10\x01\x0A\x04*\x02\x10\x01\x0A\x04b\x02\x10\x01\x0A\x04b\x02\x10\x01\x0A\x04b\x02\x10\x01\x0A\x04b\x02\x10\x01\x0A\x04b\x02\x10\x01\x18\x02\x1AI\x1AG\x08\x03\x1A\x04\x0A\x02\x10\x01\x22#\x1A!\x1A\x1F\x08\x02\x1A\x04*\x02\x10\x01\x22\x0A\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x22\x09\x1A\x07\x0A\x05(\xE6\xF5\xF7\x01\x22\x18\x1A\x16\x1A\x14\x08\x04\x1A\x04*\x02\x10\x01\x22\x0A\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x22\x0A\x0A\x06\x0A\x00\x0A\x02\x08\x01\x10\x01:\x06\x0A\x04name\x22&\x1A$\x08\x05\x1A\x04\x0A\x02\x10\x01\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01\x22\x00\x22\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x05\x22\x000\x01\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x02\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x03\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x04\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x05\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x06\x22\x00\x1A\x00\x22\x18\x0A\x16\x08\x06*\x04b\x02\x10\x01:\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x04\x22\x00\x22\x18\x0A\x16\x08\x06*\x04b\x02\x10\x01:\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x06\x22\x00\x22\x18\x0A\x16\x08\x06*\x04b\x02\x10\x01:\x0C\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x03\x22\x00\x1A\x08\x12\x06\x0A\x02\x12\x00\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x01\x22\x00\x1A\x0A\x12\x08\x0A\x04\x12\x02\x08\x02\x22\x00\x12\x0Cmin(keyword)\x12\x09min(name)\x12\x0Amin(title)2\x0A\x10\x27*\x06DuckDB'::BLOB);
# OR with the json
```

### Python
Before using the extension you must remember to properly load it. To load an extension in python, you must execute the sql commands within a connection.
```python
import duckdb

con = duckdb.connect()
con.install_extension("substrait")
con.load_extension("substrait")
```

> [!TIP]
> See [Controlling Query Optimization](#controlling-query-optimization) for more information on how to
> enable or disable the optimizer when generating Substrait. The Substrait generation functions below
> support an `enable_optimizer=bool` keyword argument for convenience.

1) Blob Generation
     
     To generate a substrait blob the ```get_substrait(SQL)``` function must be called, from a connection, with a valid SQL select query.
     ```python
     con.execute(query='CREATE TABLE crossfit (exercise text,difficulty_level int);')
     con.execute(query="INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);")
     
     proto_bytes = con.get_substrait(query="select count(exercise) as exercise from crossfit where difficulty_level <=5").fetchone()[0]
   ```
2) Json Generation
     
     To generate a json representing  the substrait plan the ```get_substrait_json(SQL)``` function, from a connection, must be called with a valid SQL select query.
     ```python
     json =  con.get_substrait_json("select count(exercise) as exercise from crossfit where difficulty_level <=5").fetchone()[0]
     ```
3) Blob Consumption
     
     To consume a substrait blob the ```from_substrait(blob)``` function must be called, from the connection, with a valid substrait BLOB plan.
     ```python
     query_result = con.from_substrait(proto=proto_bytes)
    ```

### R
Before using the extension you must remember to properly load it. To load an extension in R, you must execute the sql commands within a connection.
```r
con <- dbConnect(duckdb::duckdb(config=list("allow_unsigned_extensions"="true")))
dbExecute(con, "LOAD('substrait')")
dbExecute(con, "INSTALL('substrait')"))
```

> [!TIP]
> See [Controlling Query Optimization](#controlling-query-optimization) for more information on how to
> enable or disable the optimizer when generating Substrait. The Substrait generation functions below
> support an `enable_optimizer=bool` keyword argument for convenience.

1) Blob Generation
     
     To generate a substrait blob the ```duckdb_get_substrait(con,SQL)``` function must be called, with a connection and a valid SQL select query.
     ```r
     dbExecute(con, "CREATE TABLE crossfit (exercise text,difficulty_level int);")
     dbExecute(con, "INSERT INTO crossfit VALUES ('Push Ups', 3), ('Pull Ups', 5) , (' Push Jerk', 7), ('Bar Muscle Up', 10);")
     
     proto_bytes <- duckdb::duckdb_get_substrait(con, "select * from integers limit 5")    
   ```
2) Json Generation
     
     To generate a json representing  the substrait plan  ```duckdb_get_substrait_json(con,SQL)``` function, with a connection and a valid SQL select query.
     ```r
     json <- duckdb::duckdb_get_substrait_json(con, "select count(exercise) as exercise from crossfit where difficulty_level <=5")
     ```
3) Blob Consumption
     
     To consume a substrait blob the ```duckdb_prepare_substrait(con,blob)``` function must be called, with a connection and a valid substrait BLOB plan.
     ```r
      result <- duckdb::duckdb_prepare_substrait(con, proto_bytes)
      df <- dbFetch(result)
    ```

## Setting up CLion 
Configuring CLion with the extension template requires a little work. Firstly, make sure that the DuckDB submodule is available. 
Then make sure to open `./duckdb/CMakeLists.txt` (so not the top level `CMakeLists.txt` file from this repo) as a project in CLion.
Now to fix your project path go to `tools->CMake->Change Project Root`([docs](https://www.jetbrains.com/help/clion/change-project-root-directory.html)) to set the project root to the root dir of this repo.

Now to configure the build targets, copy the CMake variables specified in the Makefile and ensure
the build directory is set to `../build/<build_mode>`.
