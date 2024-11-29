
SimpleSqlDB is a very, VERY crude SQL wrapper around a set of DataTable, thus creatinga simple in-memory SQL database.
It implements all of the typical IDb*** classes so it works interchangeably with much database code.  The SQL variant it 
implements is compatible with DB2 at the current time but it would be easy to make it more flexible.
     
An example of using this class is very similar to other .NET database code:
     
```
         SimpleSqlDB myDB = new SimpleSqlDB();
     
         using (IDbConnection conn = myDB.GetConnection())
         {
             using (IDbCommand cmd = conn.CreateCommand())  
             {
                 cmd.CommandText = "SELECT * FROM MYTABLE";
     
                 DataReader reader = cmd.ExecuteReader();
                 while (reader.Reader())
                 {
                     ...
                 }
     
                 reader.Dispose()
             }
         }
```

The supported syntax is limited in the sense that it does not support JOINs.  Queries can only be against
one table at a time.  However, here are some of the types of queries that ARE supported against a single table:
 
```
         CREATE TABLE MYTABLE(ColName1 Type1, ColName2 Type2, ...) 
         DROP MYTABLE
     
         INSERT INTO MYTABLE(ColName1, ColName2) VALUES('Value1', 'Value2')
         INSERT INTO MYTABLE(ColName1, ColName2) VALUES(?, ?)
     
         UPDATE MYTABLE SET ColName1='Value1', ColName2='Value2' [WHERE ColName1 = 'Value1' AND ... ]
         UPDATE MYTABLE SET ColName1='Value1', ColName2='Value2' [WHERE ColName1 = 'Value1' OR  ... ]
         UPDATE MYTABLE SET ColName1=?, ColName2=? [WHERE ColName1 = ? AND ... ]
     
         DELETE MYTABLE
         DELETE MYTABLE [WHERE ColName1 = 'Value1' AND ... ]
     
         SELECT * FROM MYTABLE [WHERE ColName1 = 'Value1' AND ... ]
         SELECT ColName1, ColName2 AS 'MyValue' WHERE ColName1 = 12 
         SELECT ColName1, ColName2 AS 'MyValue' WHERE ColName1 = ? 
         SELECT ColName1, ColName2 AS 'MyValue' [WHERE ColName1 = ? AND ... ]
         SELECT ColName1 WHERE ColName1 IN (1, 2) 
         SELECT ColName1 WHERE ColName2 LIKE '%TestVal%' 
         SELECT DISTINCT * FROM MYTABLE
     
         SELECT COUNT(*) FROM MYTABLE [WHERE ColName1 = 'Value1' AND ... ]
         SELECT SUM(ColName1) FROM MYTABLE
         SELECT AVG(ColName1) FROM MYTABLE
         SELECT MIN(ColName1) FROM MYTABLE
         SELECT MAX(ColName1) FROM MYTABLE
         SELECT STDEV(ColName1) FROM MYTABLE
         SELECT VAR(ColName1) FROM MYTABLE
```    
Currently, WHERE clauses can contain multiple comparisons but they must be either all 'AND' or all 'OR', not a mixture.
The SQL parsing implementation supports bound parameters with "?" markers.
     
Data types are limited to these:
 
     ```
         CHAR
         NUMERIC
         NUMERIC(nnn)
         DECIMAL
         DECIMAL(nnn)
         DECIMAL(mmm, nnn)
         TIMESTAMP
```
This class is thread-safe.
