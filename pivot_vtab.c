/*
** pivot_vtab.c - 2019-03-03 - jakethaw
**
*************************************************************************
**
** MIT License
** 
** Copyright (c) 2019 jakethaw
** 
** Permission is hereby granted, free of charge, to any person obtaining a copy
** of this software and associated documentation files (the "Software"), to deal
** in the Software without restriction, including without limitation the rights
** to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
** copies of the Software, and to permit persons to whom the Software is
** furnished to do so, subject to the following conditions:
** 
** The above copyright notice and this permission notice shall be included in all
** copies or substantial portions of the Software.
** 
** THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
** IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
** FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
** AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
** LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
** OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
** SOFTWARE.
** 
*************************************************************************
**
** Implementation of a pivot virtual table
**
** To compile with gcc as a run-time loadable extension:
**
**   UNIX-like : gcc -g -O3 -fPIC -shared pivot_vtab.c -o pivot_vtab.so
**   Mac       : gcc -g -O3 -fPIC -dynamiclib pivot_vtab.c -o pivot_vtab.dylib
**   Windows   : gcc -g -O3 -shared pivot_vtab.c -o pivot_vtab.dll
**
*************************************************************************
**
** Usage example: 
**
** CREATE VIRTUAL TABLE pivot USING pivot_vtab(
**   (SELECT id r_id FROM r),        -- Pivot table key query
**   (SELECT id c_id, name FROM c),  -- Pivot table column definition query
**   (SELECT val                     -- Pivot query
**      FROM x 
**     WHERE r_id = ?1
**       AND c_id = ?2)
** );
**
** See script below for a more detailed usage example, and an expanded 
** definition of the virtual table arguments
**
*************************************************************************
** --
** -- The following usage example can be run using the SQLite shell
** --
** 
** .load ./pivot_vtab
** .headers on
** .mode column
** 
** -- Rows
** CREATE TABLE r AS
** SELECT 1 id UNION SELECT 2 UNION SELECT 3;
** 
** -- Columns
** CREATE TABLE c(
**   id   INTEGER PRIMARY KEY,
**   name TEXT
** );
** INSERT INTO c (name) VALUES
** ('a'),('b'),('c'),('d');
** 
** CREATE TABLE x(
**   r_id INT,
**   c_id INT,
**   val  TEXT
** );
** INSERT INTO x (r_id, c_id, val)
** SELECT r.id, c.id, c.name || r.id
**   FROM c, r;
** 
** .width 4 4 4
** SELECT * FROM x;
**
** -- r_id  c_id  val 
** -- ----  ----  ----
** -- 1     1     a1  
** -- 2     1     a2  
** -- 3     1     a3  
** -- 1     2     b1  
** -- 2     2     b2  
** -- 3     2     b3  
** -- 1     3     c1  
** -- 2     3     c2  
** -- 3     3     c3  
** -- 1     4     d1  
** -- 2     4     d2  
** -- 3     4     d3  
** 
** .width 4 5 5 5 5
** 
** CREATE VIRTUAL TABLE pivot USING pivot_vtab(
**   --
**   -- Pivot table row key query
**   --
**   -- Defines first column of the pivot table
**   --
**   -- Required to perform a full table scan of the pivot table. This is run when
**   -- not joining or filtering by the pivot table key.
**   --
**   -- The first column name in this query will become the name of the pivot table key column.
**   -- The value of the pivot table key column is provided to the pivot query as ?1.
**   --
**  (SELECT id r_id -- pivot table key
**     FROM r),
**   
**   --
**   -- Pivot table column definition query
**   --
**   -- Defines second+ column(s) of the pivot table
**   --
**   -- This query should return pivot table column key/name pairs.
**   -- It is run during vtab creation to define the pivot table column names.
**   --
**   -- The first column of this query is the pivot column key, and is provided
**   -- to the pivot query as ?2
**   --
**   -- The second column of this query is used to name the pivot table columns.
**   -- This column is required to return unique values.
**   -- 
**   -- Changes to this query can only be propagated by dropping and 
**   -- re-creating the virtual table
**   --
**  (SELECT id c_id,   -- pivot column key - can be referenced in pivot query as ?2
**          name       -- pivot column name
**     FROM c),
**     
**   --
**   -- Pivot query
**   --
**   -- This query should define a single value in the pivot table when
**   -- filtered by the pivot table row key (?1) and a column key (?2)
**   --
**  (SELECT val
**     FROM x 
**    WHERE r_id = ?1
**      AND c_id = ?2)
** );
**
** SELECT *
**   FROM pivot;
**
** -- r_id  a      b      c      d    
** -- ----  -----  -----  -----  -----
** -- 1     a1     b1     c1     d1   
** -- 2     a2     b2     c2     d2   
** -- 3     a3     b3     c3     d3   
**   
** UPDATE x
**    SET val = 'hello'
**  WHERE c_id = 3
**    AND r_id = 2;
**    
** UPDATE x
**    SET val = 'world'
**  WHERE c_id = 4
**    AND r_id = 2;
**  
** DELETE 
**   FROM x
**  WHERE c_id = 2
**    AND r_id = 3;
** 
** SELECT *
**   FROM pivot;
**
** -- r_id  a      b      c      d    
** -- ----  -----  -----  -----  -----
** -- 1     a1     b1     c1     d1   
** -- 2     a2     b2     hello  world   
** -- 3     a3            c3     d3  
*/

#include "sqlite3ext.h"
SQLITE_EXTENSION_INIT1
#include <string.h>
#include <stdio.h>

/*
** pivot_vtab is a subclass of sqlite3_vtab which is
** underlying representation of the virtual table
*/
typedef struct pivot_vtab pivot_vtab;
struct pivot_vtab {
  sqlite3_vtab base;             // Base class. Must be first
  sqlite3 *db;                   // Database connection
  int nRow_key;                  // Number of row key values (number of pivot query bound params minus 1)
  int nRow_cols;                 // Number of row columns
  int nCol_key;                  // Number of column key values
  sqlite3_stmt **col_stmt;       // List of column pivot query stmts
  char *key_sql_full_table_scan; // Full table scan key query
  char **key_sql_col_names;      // Array of key query column names
};

/* 
** pivot_cursor is a subclass of sqlite3_vtab_cursor which will
** serve as the underlying representation of a cursor that scans
** over rows of the result
*/
typedef struct pivot_cursor pivot_cursor;
struct pivot_cursor {
  sqlite3_vtab_cursor base;  // Base class - must be first
  sqlite3_int64 iRowid;      // The rowid
  sqlite3_stmt *stmt;        // Row key prepared stmt - used for full table scan
  int rc;                    // Return value for stmt
  sqlite3_value **pivot_key; // Array of row keys
};

#define PIVOT_VTAB_CONNECT_ERROR \
  sqlite3_finalize(stmt_key_query); \
  sqlite3_finalize(stmt_pivot_query); \
  sqlite3_finalize(stmt_col_query); \
  sqlite3_free(sql); \
  sqlite3_free(pivot_query_sql); \
  sqlite3_free(tab->key_sql_full_table_scan); \
  sqlite3_free(zMsg); \
  sqlite3_free_table(azData); \
  sqlite3_free(sqlite3_str_finish(create_vtab_sql)); \
  for( i=0; i<tab->nCol_key; i++ ) \
    sqlite3_finalize(tab->col_stmt[i]); \
  sqlite3_free(tab->col_stmt); \
  for( i=0; i<tab->nRow_cols; i++ ) \
    sqlite3_free(tab->key_sql_col_names[i]); \
  sqlite3_free(tab->key_sql_col_names); \
  sqlite3_free(tab); \
  return SQLITE_ERROR;

/*
** The pivotConnect() method is invoked to create a new
** template virtual table.
**
** Think of this routine as the constructor for pivot_vtab objects.
**
** All this routine needs to do is:
**
**    (1) Allocate the pivot_vtab object and initialize all fields.
**
**    (2) Tell SQLite (via the sqlite3_declare_vtab() interface) what the
**        result set of queries against the virtual table will look like.
*/
static int pivotConnect(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  pivot_vtab *tab;
  char *sql = 0;
  char *pivot_query_sql = 0;
  sqlite3_stmt *stmt_key_query = 0;
  sqlite3_stmt *stmt_pivot_query = 0;
  sqlite3_stmt *stmt_col_query = 0;
  int rc;
  int i, j;

  sqlite3_str *create_vtab_sql = 0;
  
  tab = (pivot_vtab*)sqlite3_malloc(sizeof(pivot_vtab));
  if( tab==0 ) return SQLITE_NOMEM;
  memset(tab, 0, sizeof(*tab));
  tab->db = db;
  *ppVtab = (sqlite3_vtab*)tab;

  // vars for sqlite3_get_table
  int nRow = 0;
  int nCol = 0;
  char **azData = 0;
  char *zMsg = 0;

  // CREATE TABLE string
  create_vtab_sql = sqlite3_str_new(db);
  sqlite3_str_appendall(create_vtab_sql, "CREATE TABLE x(");

  ///////////////////////////////////////////////////
  // Pivot table key query
  ///////////////////////////////////////////////////

  tab->key_sql_full_table_scan =  sqlite3_mprintf("SELECT * FROM \n%s", argv[3]);
  rc = sqlite3_prepare_v2(db, tab->key_sql_full_table_scan, -1, &stmt_key_query, 0);

  // Validate pivot table key query
  if( rc!=SQLITE_OK ){
    *pzErr = sqlite3_mprintf("Pivot table key query prepare error - %s", sqlite3_errmsg(db));
    PIVOT_VTAB_CONNECT_ERROR
  }

  // get row key columnn count
  tab->nRow_cols = sqlite3_column_count(stmt_key_query);

  // get row key column names
  tab->key_sql_col_names = sqlite3_malloc(tab->nRow_cols*sizeof(char*));
  memset(tab->key_sql_col_names, 0, tab->nRow_cols*sizeof(char*));

  for( i=0; i<tab->nRow_cols; i++ ){
    tab->key_sql_col_names[i] = sqlite3_mprintf("\"%w\"", sqlite3_column_name(stmt_key_query, i));
    if( i>0 )
      sqlite3_str_appendall(create_vtab_sql, ",");
    sqlite3_str_appendf(create_vtab_sql, "%s", tab->key_sql_col_names[i]);
  }

  sqlite3_finalize(stmt_key_query);
  stmt_key_query = 0;

  ///////////////////////////////////////////////////
  // Pivot query
  ///////////////////////////////////////////////////

  pivot_query_sql =  sqlite3_mprintf("SELECT * FROM \n%s", argv[5]);
  rc = sqlite3_prepare_v2(db, pivot_query_sql, -1, &stmt_pivot_query, 0);

  // Validate pivot query 
  if( rc!=SQLITE_OK ){
    *pzErr = sqlite3_mprintf("Pivot query prepare error - %s", sqlite3_errmsg(db));
    PIVOT_VTAB_CONNECT_ERROR
  }

  tab->nRow_key = sqlite3_bind_parameter_count(stmt_pivot_query)-1;

  // Validate bound param count
  if( tab->nRow_key > tab->nRow_cols ){
    *pzErr = sqlite3_mprintf("Pivot table key query error - Unexpected number of bound parameters.");
    PIVOT_VTAB_CONNECT_ERROR
  }

  sqlite3_finalize(stmt_pivot_query);
  stmt_pivot_query = 0;

  ///////////////////////////////////////////////////
  // Pivot table column definition query
  ///////////////////////////////////////////////////

  sql =  sqlite3_mprintf("SELECT * FROM \n%s", argv[4]);
  rc = sqlite3_prepare_v2(db, sql, -1, &stmt_col_query, 0);

  // Validate pivot table column definition query
  if( rc!=SQLITE_OK ){
    *pzErr = sqlite3_mprintf("Pivot table column definition query prepare error - %s", sqlite3_errmsg(db));
    PIVOT_VTAB_CONNECT_ERROR
  }

  // Validate pivot table column definition query count == 2
  if( sqlite3_column_count(stmt_col_query) != 2 ){
    *pzErr = sqlite3_mprintf("Pivot table column definition query expects 2 result column. Query contains %d columns.", sqlite3_column_count(stmt_col_query));
    PIVOT_VTAB_CONNECT_ERROR
  }
  
  // Validate pivot columns for uniqueness
  rc = sqlite3_get_table(db, sql, &azData, &nRow, &nCol, &zMsg);

  if( rc!=SQLITE_OK ){
    *pzErr = sqlite3_mprintf("%s", zMsg);
    PIVOT_VTAB_CONNECT_ERROR
  }

  if( nRow > 1 ){
    for( i=1; i<nRow-1; i++ ){
      for( j=i+1; j<nRow; j++ ){
        if( !strcmp(azData[i*nCol], azData[j*nCol]) ){
          *pzErr = sqlite3_mprintf("Pivot table column keys must be unique. Duplicate column key \"%s\".", azData[i*nCol]);
          PIVOT_VTAB_CONNECT_ERROR
        }
        if( !sqlite3_stricmp(azData[i*nCol+1], azData[j*nCol+1]) ){
          *pzErr = sqlite3_mprintf("Pivot table column names must be unique. Duplicate column \"%s\".", azData[i*nCol+1]);
          PIVOT_VTAB_CONNECT_ERROR
        }
      }
    }
  }
  sqlite3_free_table(azData);
  sqlite3_free(sql);

  ///////////////////////////////////////////////////
  // Construct remainder of vtab definition
  ///////////////////////////////////////////////////

  tab->nCol_key = 0;
  while( sqlite3_step(stmt_col_query)==SQLITE_ROW ){
    tab->nCol_key++;
    tab->col_stmt = sqlite3_realloc(tab->col_stmt, tab->nCol_key*sizeof(sqlite3_stmt*));
    
    // prepare pivot col stmt
    sqlite3_prepare_v2(db, pivot_query_sql, -1, &(tab->col_stmt[tab->nCol_key-1]), 0);
    sqlite3_bind_value(tab->col_stmt[tab->nCol_key-1], tab->nRow_key+1, sqlite3_column_value(stmt_col_query, 0));
    sqlite3_str_appendf(create_vtab_sql, ",\"%w\"", sqlite3_column_text(stmt_col_query, 1));
  }
  sqlite3_finalize(stmt_col_query);
  sqlite3_free(pivot_query_sql);
  sqlite3_str_appendall(create_vtab_sql, ")");
  
  sql = sqlite3_str_finish(create_vtab_sql);
  // printf("%s\n", sql);
  rc = sqlite3_declare_vtab(db, sql);
  sqlite3_free(sql);
  
  return rc;
}

/*
** The xConnect and xCreate methods do the same thing, but they must be
** different so that the virtual table is not an eponymous virtual table.
*/
static int pivotCreate(
  sqlite3 *db,
  void *pAux,
  int argc, const char *const*argv,
  sqlite3_vtab **ppVtab,
  char **pzErr
){
  return pivotConnect(db, pAux, argc, argv, ppVtab, pzErr);
}

/*
** Implementation of pivot xRename method.
*/
static int pivotRename(
  sqlite3_vtab *pVtab, // Virtual table handle
  const char *zName    // New name of table
){
  return SQLITE_OK;
}

/*
** This method is the destructor for pivot_vtab objects.
*/
static int pivotDisconnect(sqlite3_vtab *pVtab){
  pivot_vtab *tab = (pivot_vtab*)pVtab;

  int i;
  for( i=0; i<tab->nCol_key; i++ )
    sqlite3_finalize(tab->col_stmt[i]);
  sqlite3_free(tab->col_stmt);

  for( i=0; i<tab->nRow_cols; i++ )
    sqlite3_free(tab->key_sql_col_names[i]);
  sqlite3_free(tab->key_sql_col_names);

  sqlite3_free(tab->key_sql_full_table_scan);

  sqlite3_free(tab);
  return SQLITE_OK;
}

/*
** Constructor for a new pivot_cursor object.
*/
static int pivotOpen(sqlite3_vtab *pVtab, sqlite3_vtab_cursor **ppCur){
  pivot_cursor *cur;
  cur = sqlite3_malloc( sizeof(*cur) );
  if( cur==0 ) return SQLITE_NOMEM;
  memset(cur, 0, sizeof(*cur));
  *ppCur = &cur->base;
  return SQLITE_OK;
}

/*
** Destructor for a pivot_cursor.
*/
static int pivotClose(sqlite3_vtab_cursor *pCur){
  pivot_vtab *tab = (pivot_vtab*)pCur->pVtab;
  pivot_cursor *cur = (pivot_cursor*)pCur;
  int i;

  if( cur->pivot_key ){
    for( i=0; i<tab->nRow_cols; i++ )
      sqlite3_value_free(cur->pivot_key[i]);
    sqlite3_free(cur->pivot_key);
  }
  if( cur->stmt )
    sqlite3_finalize(cur->stmt);
  sqlite3_free(cur);
  return SQLITE_OK;
}

/*
** Advance a pivot_cursor to its next row of output.
*/
static int pivotNext(sqlite3_vtab_cursor *pCur){
  pivot_vtab *tab = (pivot_vtab*)pCur->pVtab;
  pivot_cursor *cur = (pivot_cursor*)pCur;
  int i;
  
  if( cur->pivot_key ){
    for( i=0; i<tab->nRow_cols; i++ ){
      sqlite3_value_free(cur->pivot_key[i]);
      cur->pivot_key[i] = 0;
    }
  }
  
  cur->rc = sqlite3_step(cur->stmt);
  if( cur->rc != SQLITE_DONE ){
    for( i=0; i<tab->nRow_cols; i++ )
      cur->pivot_key[i] = sqlite3_value_dup(sqlite3_column_value(cur->stmt, i));
  }
  
  cur->iRowid++;
  return SQLITE_OK;
}

/*
** Return values of columns for the row at which the pivot_cursor
** is currently pointing.
*/
static int pivotColumn(
  sqlite3_vtab_cursor *pCur,  // The cursor
  sqlite3_context *ctx,       // First argument to sqlite3_result_...()
  int i                       // Which column to return
){
  pivot_vtab *tab = (pivot_vtab*)pCur->pVtab;
  pivot_cursor *cur = (pivot_cursor*)pCur;

  if( i<tab->nRow_cols ){
    // return the row key
    sqlite3_result_value(ctx, cur->pivot_key[i]);
  }else{
    // return column value, or null
    sqlite3_stmt *stmt = tab->col_stmt[i-tab->nRow_cols];

    for( i=0; i<tab->nRow_key; i++ )
      sqlite3_bind_value(stmt, i+1, cur->pivot_key[i]);

    if( sqlite3_step(stmt)==SQLITE_ROW ){
      sqlite3_result_value(ctx, sqlite3_column_value(stmt, 0));
    }else{
      sqlite3_result_null(ctx);
    }
    sqlite3_reset(stmt);
  }
  
  return SQLITE_OK;
}

/*
** Return the rowid for the current row.
*/
static int pivotRowid(sqlite3_vtab_cursor *pCur, sqlite_int64 *pRowid){
  pivot_cursor *cur = (pivot_cursor*)pCur;
  *pRowid = cur->iRowid;
  return SQLITE_OK;
}

/*
** Return TRUE if the cursor has been moved off of the last
** row of output.
*/
static int pivotEof(sqlite3_vtab_cursor *pCur){
  pivot_vtab *tab = (pivot_vtab*)pCur->pVtab;
  pivot_cursor *cur = (pivot_cursor*)pCur; 
  int i;

  if( cur->rc == SQLITE_DONE ){
    if( cur->pivot_key ){
      for( i=0; i<tab->nRow_cols; i++ )
        sqlite3_value_free(cur->pivot_key[i]);
      sqlite3_free(cur->pivot_key);
    }
    sqlite3_finalize(cur->stmt);
    cur->stmt = 0;
    return 1;
  }
  return 0;
}

/*
** This method is called to "rewind" the templatevtab_cursor object back
** to the first row of output.  This method is always called at least
** once prior to any call to templatevtabColumn() or templatevtabRowid() or 
** templatevtabEof().
*/
static int pivotFilter(
  sqlite3_vtab_cursor *pVtabCursor, 
  int idxNum, const char *idxStr,
  int argc, sqlite3_value **argv
){
  pivot_vtab *tab = (pivot_vtab*)pVtabCursor->pVtab;
  pivot_cursor *cur = (pivot_cursor*)pVtabCursor;
  int i;
  
  cur->pivot_key = sqlite3_malloc(tab->nRow_cols*sizeof(sqlite3_value*));
  
  // Row query
  sqlite3_prepare_v2(tab->db, idxStr, -1, &(cur->stmt), 0);
  for( i=0; i<argc; i++ )
    sqlite3_bind_value(cur->stmt, i+1, argv[i]);

  // printf("%s\n", sqlite3_expanded_sql(cur->stmt));

  cur->rc = sqlite3_step(cur->stmt);
  if( cur->rc == SQLITE_DONE ){
    cur->pivot_key = 0;
  }else{
    for( i=0; i<tab->nRow_cols; i++ )
      cur->pivot_key[i] = sqlite3_value_dup(sqlite3_column_value(cur->stmt, i));
  }
  
  cur->iRowid = 1;
  return SQLITE_OK;
}

/*
** SQLite will invoke this method one or more times while planning a query
** that uses the pivot virtual table.  This routine needs to create
** a query plan for each invocation and compute an estimated cost for that
** plan.
*/
static int pivotBestIndex(
  sqlite3_vtab *pVtab,
  sqlite3_index_info *pIdxInfo
){
  pivot_vtab *tab = (pivot_vtab*)pVtab;
  int i;
  int argvIndex = 1;
  int orderIdx = 0;
  char *op;

  sqlite3_str *key_sql_filtered;

  key_sql_filtered = sqlite3_str_new(tab->db);
  sqlite3_str_appendall(key_sql_filtered, tab->key_sql_full_table_scan);

  const struct sqlite3_index_constraint *pConstraint;
  pConstraint = pIdxInfo->aConstraint;
  for(i=0; i<pIdxInfo->nConstraint; i++, pConstraint++){
    if( pConstraint->usable==0 ) continue;
    if( !(pConstraint->iColumn < tab->nRow_cols) ) continue;
    switch( pConstraint->op ){
      case SQLITE_INDEX_CONSTRAINT_EQ:
        op = "=";
        break;
      case SQLITE_INDEX_CONSTRAINT_LT:
        op = "<";
        break;
      case SQLITE_INDEX_CONSTRAINT_LE:
        op = "<=";
        break;
      case SQLITE_INDEX_CONSTRAINT_GT:
        op = ">";
        break;
      case SQLITE_INDEX_CONSTRAINT_GE:
        op = ">=";
        break;
      case SQLITE_INDEX_CONSTRAINT_MATCH:
        op = "MATCH";
        break;
      case SQLITE_INDEX_CONSTRAINT_LIKE:
        op = "LIKE";
        break;
      case SQLITE_INDEX_CONSTRAINT_GLOB:
        op = "GLOB";
        break;
      case SQLITE_INDEX_CONSTRAINT_REGEXP:
        op = "REGEXP";
        break;
      case SQLITE_INDEX_CONSTRAINT_NE:
        op = "<>";
        break;
      case SQLITE_INDEX_CONSTRAINT_ISNOT:
      case SQLITE_INDEX_CONSTRAINT_ISNOTNULL:
        op = "IS NOT";
        break;
      case SQLITE_INDEX_CONSTRAINT_ISNULL:
      case SQLITE_INDEX_CONSTRAINT_IS:
        op = "IS";
        break;
      case SQLITE_INDEX_CONSTRAINT_FUNCTION:
      default:
        pIdxInfo->aConstraintUsage[i].omit = 0;
        op = 0;
        break;
    }

    if( op ){
      if( argvIndex==1 ){
        sqlite3_str_appendall(key_sql_filtered, "\n WHERE ");
      } else{
        sqlite3_str_appendall(key_sql_filtered, " AND ");
      }
      sqlite3_str_appendf(key_sql_filtered, "%s %s ?", tab->key_sql_col_names[pConstraint->iColumn], op);
      pIdxInfo->aConstraintUsage[i].argvIndex = argvIndex++;
      pIdxInfo->aConstraintUsage[i].omit = 1;
    }
  }

  const struct sqlite3_index_orderby *pOrderBy;
  pOrderBy = pIdxInfo->aOrderBy;
  for(i=0; i<pIdxInfo->nOrderBy; i++, pOrderBy++){
    if( !(pOrderBy->iColumn < tab->nRow_cols) ) continue;
    if( orderIdx==0 ){
      sqlite3_str_appendall(key_sql_filtered, "\n ORDER BY ");
    } else{
      sqlite3_str_appendall(key_sql_filtered, ", ");
    }
    sqlite3_str_appendf(key_sql_filtered, "%s %s", tab->key_sql_col_names[pOrderBy->iColumn], pOrderBy->desc ? "DESC" : "");
    pIdxInfo->orderByConsumed = 1;
    orderIdx++;
  }

  pIdxInfo->idxNum = 0;
  pIdxInfo->estimatedCost = (double)2147483647/argvIndex;
  pIdxInfo->estimatedRows = 10;
  pIdxInfo->idxStr = sqlite3_str_finish(key_sql_filtered);
  pIdxInfo->needToFreeIdxStr = 1;
  
  return SQLITE_OK;
}

/*
** This following structure defines all the methods for the 
** pivot virtual table.
*/
static sqlite3_module pivotModule = {
  0,                 // iVersion
  pivotCreate,       // xCreate
  pivotConnect,      // xConnect
  pivotBestIndex,    // xBestIndex
  pivotDisconnect,   // xDisconnect
  pivotDisconnect,   // xDestroy
  pivotOpen,         // xOpen
  pivotClose,        // xClose
  pivotFilter,       // xFilter
  pivotNext,         // xNext
  pivotEof,          // xEof
  pivotColumn,       // xColumn
  pivotRowid,        // xRowid
  0,                 // xUpdate
  0,                 // xBegin
  0,                 // xSync
  0,                 // xCommit
  0,                 // xRollback
  0,                 // xFindFunction
  pivotRename,       // xRename
};

#ifdef _WIN32
__declspec(dllexport)
#endif
int sqlite3_pivotvtab_init(
  sqlite3 *db, 
  char **pzErrMsg, 
  const sqlite3_api_routines *pApi
){
  int rc;
  SQLITE_EXTENSION_INIT2(pApi);
  rc = sqlite3_create_module(db, "pivot_vtab", &pivotModule, 0);
  return rc;
}
