using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Xml.Serialization;

namespace Com.DeepBlue523.SimpleSqlMemoryDB
{
    /// <summary>
    /// SimpleSqlDB is a very, VERY crude SQL wrapper around a set of DataTable, thus creatinga simple in-memory SQL database.
    /// It implements all of the typical IDb*** classes so it works interchangeably with much database code.  The SQL variant it 
    /// implements is compatible with DB2 at the current time but it would be easy to make it more flexible.
    /// 
    /// An example of using this class is very similar to other .NET database code:
    /// 
    ///     SimpleSqlDB myDB = new SimpleSqlDB();
    /// 
    ///     using (IDbConnection conn = myDB.GetConnection())
    ///     {
    ///         using (IDbCommand cmd = conn.CreateCommand())  
    ///         {
    ///             cmd.CommandText = "SELECT * FROM OEP40";
    /// 
    ///             DataReader reader = cmd.ExecuteReader();
    ///             while (reader.Reader())
    ///             {
    ///                 ...
    ///             }
    /// 
    ///             reader.Dispose()
    ///         }
    ///     }
    /// 
    /// The supported syntax is limited in the sense that it does not support JOINs.  Queries can only be against
    /// one table at a time.  However, here are some of the types of queries that ARE supported against a single table:
    /// 
    ///     CREATE TABLE MYTABLE(ColName1 Type1, ColName2 Type2, ...) 
    ///     DROP MYTABLE
    /// 
    ///     INSERT INTO MYTABLE(ColName1, ColName2) VALUES('Value1', 'Value2')
    ///     INSERT INTO MYTABLE(ColName1, ColName2) VALUES(?, ?)
    /// 
    ///     UPDATE MYTABLE SET ColName1='Value1', ColName2='Value2' [WHERE ColName1 = 'Value1' AND ... ]
    ///     UPDATE MYTABLE SET ColName1='Value1', ColName2='Value2' [WHERE ColName1 = 'Value1' OR  ... ]
    ///     UPDATE MYTABLE SET ColName1=?, ColName2=? [WHERE ColName1 = ? AND ... ]
    /// 
    ///     DELETE MYTABLE
    ///     DELETE MYTABLE [WHERE ColName1 = 'Value1' AND ... ]
    /// 
    ///     SELECT * FROM MYTABLE [WHERE ColName1 = 'Value1' AND ... ]
    ///     SELECT ColName1, ColName2 AS 'MyValue' WHERE ColName1 = 12 
    ///     SELECT ColName1, ColName2 AS 'MyValue' WHERE ColName1 = ? 
    ///     SELECT ColName1, ColName2 AS 'MyValue' [WHERE ColName1 = ? AND ... ]
    ///     SELECT ColName1 WHERE ColName1 IN (1, 2) 
    ///     SELECT ColName1 WHERE ColName2 LIKE '%TestVal%' 
    ///     SELECT DISTINCT * FROM MYTABLE
    /// 
    ///     SELECT COUNT(*) FROM MYTABLE [WHERE ColName1 = 'Value1' AND ... ]
    ///     SELECT SUM(ColName1) FROM MYTABLE
    ///     SELECT AVG(ColName1) FROM MYTABLE
    ///     SELECT MIN(ColName1) FROM MYTABLE
    ///     SELECT MAX(ColName1) FROM MYTABLE
    ///     SELECT STDEV(ColName1) FROM MYTABLE
    ///     SELECT VAR(ColName1) FROM MYTABLE
    /// 
    /// Currently, WHERE clauses can contain multiple comparisons but they must be either all 'AND' or all 'OR', not a mixture.
    /// The SQL parsing implementation supports bound parameters with "?" markers.
    /// 
    /// Data types are limited to these:
    /// 
    ///     CHAR
    ///     NUMERIC
    ///     NUMERIC(nnn)
    ///     DECIMAL
    ///     DECIMAL(nnn)
    ///     DECIMAL(mmm, nnn)
    ///     TIMESTAMP
    /// 
    /// They are the ones used commonly on the iSeries.  It would not take much to extend them to SQL Server-like types.
    /// 
    /// This class is thread-safe.
    /// </summary>
    public class SimpleSqlDB : IDisposable
    {
        // Backing database structures.
        private Dictionary<string, DataTable> _tableDataByName = new Dictionary<string, DataTable>();
        
        public SimpleSqlDB()
        {
            this.IgnoreConstraintViolationsUponInsert = false;
        }

        public void Dispose()
        {
            lock(_tableDataByName)
            {
                _tableDataByName.Clear();
                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Load an existing database from disk.
        /// </summary>
        public static SimpleSqlDB LoadFromDisk(string absLocation)
        {
            if (!File.Exists(absLocation))
            {
                throw new FileNotFoundException("File not found: " +absLocation);
            }

            using (Stream inStream = new FileStream(absLocation, FileMode.Open))
            {
                // Load the list of tables.
                XmlSerializer ser = new XmlSerializer(typeof(List<DataTable>));
                List<DataTable> dbTableList = ser.Deserialize(inStream) as List<DataTable>;

                // Organize.
                SimpleSqlDB loadedDB = new SimpleSqlDB();
                foreach (DataTable table in dbTableList)
                {
                    loadedDB._tableDataByName[table.TableName] = table;
                }

                return loadedDB;
            }

        }

        /// <summary>
        /// Save this database to disk.
        /// </summary>
        public void SaveToDisk(string absLocation)
        {
            List<DataTable> dbTableList = new List<DataTable>(_tableDataByName.Values);

            using (Stream inStream = new FileStream(absLocation, FileMode.Create))
            {
                XmlSerializer ser = new XmlSerializer(typeof(List<DataTable>));
                ser.Serialize(inStream, dbTableList);
            }
        }

        /// <summary>
        /// Converts a DB2 type into a .NET type.
        /// </summary>
        /// <param name="db2Type"></param>
        /// <returns></returns>
        private Type GetDotNetTypeForDB2Type(string db2Type)
        {
            if (db2Type == "CHAR")
            {
                return typeof(string);
            }
            else if (db2Type == "NUMERIC")
            {
                return typeof(int);
            }
            else if ((db2Type == "NUMERIC") && db2Type.Contains(","))
            {
                return typeof(decimal);
            }
            else if (db2Type == "DECIMAL")
            {
                return typeof(decimal);
            }
            else if ((db2Type == "DECIMAL") && db2Type.Contains(","))
            {
                return typeof(decimal);
            }
            else if ((db2Type == "TIMESTAMP") || (db2Type == "TIMESTMP"))
            {
                return typeof(DateTime);
            }
            else
            {
                return typeof(string);
            }
        }

        /// <summary>
        /// Inner logic for a "CREATE TABLE" statement that has already been parsed.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="colNamesAndTypeMap"></param>
        /// <returns></returns>
        private DataTable SqlImpl_CreateTable(string tableName, Dictionary<string, string> colNamesAndTypeMap, List<string> keyColumnNames)
        {
            string tableNameUpper = tableName.ToUpper();

            // Make sure not a duplicate.
            if (_tableDataByName.ContainsKey(tableNameUpper))
            {
                throw new SimpleSqlDBException("Table '" + tableNameUpper + "' already exists.");
            }

            // Make sure all the key column names are valid.
            foreach(string keyCol in keyColumnNames)
            {
                if (!colNamesAndTypeMap.ContainsKey(keyCol))
                {
                    throw new SimpleSqlDBException("Invalid CREATE TABLE statement (key column name " + keyCol + " is invalid)");
                }
            }

            // Otherwise, construct a DataTable for this guy.
            DataTable dt = new DataTable();
            dt.TableName = tableNameUpper;

            List<DataColumn> keyColList = new List<DataColumn>();

            foreach(string colName in colNamesAndTypeMap.Keys)
            {
                // Create the column.
                string dataTypeStr = colNamesAndTypeMap[colName];
                Type dotNetType = GetDotNetTypeForDB2Type(dataTypeStr);

                DataColumn col = new DataColumn();
                col.ColumnName = colName.Replace("[", "").Replace("]", "");
                col.DataType = dotNetType;

                dt.Columns.Add(col);

                // Collect key columns.
                if (keyColumnNames.Contains(colName.Replace("[", "").Replace("]", "")))
                {
                    keyColList.Add(col);
                }
            }

            dt.PrimaryKey = keyColList.ToArray();

            _tableDataByName[tableNameUpper] = dt;
            return dt;
        }

        /// <summary>
        /// Inner logic for a "DELETE" statement that has already been parsed.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="whereConditionList"></param>
        /// <param name="boundParms"></param>
        /// <returns></returns>
        private int SqlImpl_DeleteRows(string tableName, string condition)
        {
            string tableNameUpper = tableName.ToUpper();

            // Make sure not a duplicate.
            if (!_tableDataByName.ContainsKey(tableNameUpper))
            {
                throw new SimpleSqlDBException("Table '" + tableNameUpper + "' does not exist.");
            }

            // Perform the delete.
            DataTable dt = _tableDataByName[tableNameUpper];
            int rowsAffected;

            // No condition - delete everything!
            if (string.IsNullOrWhiteSpace(condition))
            {
                rowsAffected = dt.Rows.Count;
                dt.Rows.Clear();
            }
            // Conditional - process it!
            else
            {
                DataRow[] matchingRows = dt.Select(condition);

                foreach (DataRow row in matchingRows)
                {
                    dt.Rows.Remove(row);
                }

                rowsAffected = matchingRows.Length;
            }

            return rowsAffected;
        }

        /// <summary>
        /// Implements "UPDATE" statement logic on an already-parsed statement.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="setList"></param>
        /// <param name="whereConditionList"></param>
        /// <param name="boundParms"></param>
        /// <param name="firstBoundParmIdxForWhere"></param>
        /// <returns></returns>
        private int SqlImpl_UpdateRows(string tableName, List<Tuple<string, string, string>> setList, string condition)
        {
            string tableNameUpper = tableName.ToUpper();

            // Make sure not a duplicate.
            if (!_tableDataByName.ContainsKey(tableNameUpper))
            {
                throw new SimpleSqlDBException("Table '" + tableNameUpper + "' does not exist.");
            }

            // Perform the delete.
            DataTable dt = _tableDataByName[tableNameUpper];

            // Condition - delete everything!
            DataRow[] matchingRows = dt.Select(condition);;

            foreach (DataRow row in matchingRows)
            {
                foreach (Tuple<string, string, string> set in setList)
                {
                    // Validate the column name.
                    string colNameUpper = set.Item1.ToUpper().Replace("[", "").Replace("]", "");

                    if (!row.Table.Columns.Contains(colNameUpper))
                    {
                        throw new SimpleSqlDBException("Invalid column name '" + colNameUpper + "'.");
                    }
                    else
                    {
                        int colIdx = dt.Columns.IndexOf(dt.Columns[colNameUpper]);
                        UpdateColumnValue(row, row, condition, colIdx, set.Item3);
                    }
                }
            }

            return matchingRows.Length;
        }

        /// <summary>
        /// Updates a single column value for a row.
        /// </summary>
        /// <param name="targetRow"></param>
        /// <param name="colName"></param>
        /// <param name="newValue"></param>
        /// <param name="boundParms"></param>
        /// <param name="boundParmIdx"></param>
        private void UpdateColumnValue(DataRow sourceRow, DataRow targetRow, string condition, int targetColIdx, string newValueStr)
        {
            // Validate the column name.
            string colNameUpper = string.IsNullOrEmpty(newValueStr) ? null : newValueStr.ToUpper();

            // Convert the value we are comparing to a .NET value.
            Type val2Type = targetRow.Table.Columns[targetColIdx].DataType;
            object newValueObj;

            // Unwrap string and DateTime literals.
            try
            {
                if (newValueStr == null)
                {
                    newValueObj = null;
                }
                else if (colNameUpper.Contains("("))
                {
                    if (colNameUpper.Contains("COUNT"))
                    {
                        DataRow[] countedRows = sourceRow.Table.Select(condition);
                        newValueObj = countedRows.Length;
                    }
                    else
                    {
                        newValueObj = sourceRow.Table.Compute(newValueStr, condition);
                    }
                }
                else if (sourceRow.Table.Columns.Contains(colNameUpper))
                {
                    newValueObj = sourceRow[colNameUpper];
                }
                else if (val2Type == typeof(String))
                {
                    string trimmedNewValueStr = newValueStr.Trim();
                    newValueObj = trimmedNewValueStr.StartsWith("'") 
                                        ? trimmedNewValueStr.Substring(1, trimmedNewValueStr.Length - 2) 
                                        : newValueStr;
                }
                else if (val2Type == typeof(DateTime))
                {
                    string trimmedNewValueStr = newValueStr.Trim();
                    newValueStr = trimmedNewValueStr.StartsWith("'") ? trimmedNewValueStr.Substring(1) : trimmedNewValueStr;
                    newValueStr = newValueStr.EndsWith("'") ? newValueStr.Substring(0, newValueStr.Length - 1) : newValueStr;
                    newValueObj = DateTime.Parse(newValueStr);
                }
                else 
                {
                    newValueObj = (newValueStr == null) ? DBNull.Value : Convert.ChangeType(newValueStr, val2Type);
                }
            }
            catch
            {
                throw new SimpleSqlDBException("Unable to convert value " + newValueStr + " to column type " + val2Type.Name);
            }

            // Set the new cell value.
            targetRow[targetColIdx] = (newValueObj == null) ? DBNull.Value : newValueObj;
        }

        /// <summary>
        /// Distributes "INSERT", "UPDATE", and "DELETE" statements to the correct parser.
        /// </summary>
        /// <param name="CommandText"></param>
        /// <param name="parms"></param>
        /// <returns></returns>
        public int ExecuteNonQuery(string CommandText, IDataParameterCollection parms)
        {
            CommandText = unfoldBoundParms(CommandText, parms);
            string[] stmtElements = CommandText.ToUpper().Split(new string[] { " ", ",", "(", ")" }, StringSplitOptions.RemoveEmptyEntries);

            lock(_tableDataByName)
            {
                // Handle a CREATE TABLE.
                if ((stmtElements.Length > 2) && stmtElements[0].Equals("CREATE") && stmtElements[1].Equals("TABLE"))
                {
                    ParseAndRun_CreateTable(CommandText);
                    return 0;
                }

                // Handle a DROP.
                if ((stmtElements.Length > 1) && stmtElements[0].Equals("DROP"))
                {
                    SqlImpl_DropTable(stmtElements[1]);
                    return 0;
                }

                // Handle an INSERT statement.
                if ((stmtElements.Length > 1) && stmtElements[0].Equals("INSERT"))
                {
                    return ParseAndRun_Insert(CommandText);
                }

                // Handle an UPDATE statement.
                if ((stmtElements.Length > 1) && stmtElements[0].Equals("UPDATE"))
                {
                    return ParseAndRun_Update(CommandText);
                }

                // Handle a DELETE statement.
                if ((stmtElements.Length > 1) && stmtElements[0].Equals("DELETE"))
                {
                    return ParseAndRun_Delete(CommandText);
                }
            }

            // We don't understand the request.
            throw new SimpleSqlDBException("Invalid SQL text: " + CommandText);
        }

        private List<Tuple<string, string, string>> ParseCondition(string conditionText, string delim)
        {
            // Split into pieces separated by ANDs.
            List<Tuple<string, string, string>> whereConditionList = null;

            if (conditionText != null)
            {
                // make sure space between operators to simplify parsing.
                conditionText = conditionText.Replace("=", " = ");
                conditionText = conditionText.Replace("<", " < ");
                conditionText = conditionText.Replace(">", " > ");
                conditionText = conditionText.Replace("<>", " <> ");

                // Chop into pieces.
                whereConditionList = new List<Tuple<string, string, string>>();
                string[] conditionElements = conditionText.Split(new string[] { delim }, StringSplitOptions.RemoveEmptyEntries);

                // Iterate over each condition.
                foreach (string check in conditionElements)
                {
                    string[] checkElements = check.Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                    if (checkElements.Length >= 3)
                    {
                        string newValue = checkElements[2];
                        for (int i=3; i<checkElements.Length; i++) { newValue += " " + checkElements[i]; }

                        Tuple<string, string, string> op =
                            new Tuple<string, string, string>(checkElements[0].Trim(), checkElements[1].Trim(), newValue.Trim());

                        whereConditionList.Add(op);
                    }
                }
            }

            return whereConditionList;
        }

        private void ParseAndRun_CreateTable(string CommandText)
        {
            string[] stmtElements = CommandText.ToUpper().Split(new string[] { " ", ",", "(", ")" }, StringSplitOptions.RemoveEmptyEntries);

            // Handle a CREATE TABLE.
            string tableName = stmtElements[2];

            // Get a list of fields and their types.  Also look for a PRIMARY KEY clause.
            Dictionary<string, string> colNamesAndTypeMap = new Dictionary<string, string>();
            List<string> keyColumnNames = new List<string>();

            for (int i = 3; i < (stmtElements.Length - 1); i += 2)
            {
                if (stmtElements[i] == "PRIMARY")
                {
                    // Suck in the list of primary key columns.  Then we're done since it is always at the end.
                    for (int j=i + 2; j< stmtElements.Length; j++)
                    {
                        keyColumnNames.Add(stmtElements[j].ToUpper().Replace("[", "").Replace("]", ""));
                    }

                    break;
                }
                else
                {
                    int work;
                    if (!int.TryParse(stmtElements[i], out work)) // Don't treat field sizes as column names.
                    {
                        colNamesAndTypeMap[stmtElements[i].Replace("[", "").Replace("]", "")] = stmtElements[i + 1];
                    }
                }
            }

            // Call into the database to construct the table.
            SqlImpl_CreateTable(tableName, colNamesAndTypeMap, keyColumnNames);
        }

        private int ParseAndRun_Delete(string CommandText)
        {
            // Break out the table name and WHERE condition.
            string tableName = CommandText.Replace("DELETE", "").Replace("FROM", "").Trim();
            string condition = null;

            int whereIdx = tableName.IndexOf("WHERE");
            if (whereIdx > 0)
            {
                condition = tableName.Substring(whereIdx + "WHERE".Length).Trim();
                condition = escapeColumnNames(tableName, condition);
                tableName = tableName.Substring(0, whereIdx).Trim();
            }
            
            // Call into the database to delete rows.
            return SqlImpl_DeleteRows(tableName, condition);
        }

        private int ParseAndRun_Update(string CommandText)
        {
            // Break out the table name.
            string tableName;

            int setIdx = CommandText.IndexOf("SET");
            if (setIdx > 0)
            {
                tableName = CommandText.Substring(0, setIdx).Replace("UPDATE", "").Trim();
            }
            else
            {
                throw new SimpleSqlDBException("Invalid UPDATE statement: " + CommandText);
            }

            // Break out the table name and WHERE condition.
            string setColumns = null;

            int whereIdx = CommandText.IndexOf("WHERE");
            if (whereIdx > 0)
            {
                setColumns = CommandText.Substring(setIdx + "SET".Length, whereIdx - setIdx - "SET".Length).Trim();
            }
            else
            {
                setColumns = CommandText.Substring(setIdx + "SET".Length).Trim();
            }

            // Break out the table name and WHERE condition.
            string condition = null;

            if (whereIdx > 0)
            {
                condition = CommandText.Substring(whereIdx + "WHERE".Length).Trim();
                condition = condition.Replace("(", "").Replace(")", "");
                condition = escapeColumnNames(tableName, condition);
            }

            // Split into pieces separated by ANDs.
            List<Tuple<string, string, string>> setList = ParseCondition(setColumns, ",");

            int firstBoundParmIdxForWhere = CountChar(setColumns, '?');

            // Call into the database to update the rows.
            return SqlImpl_UpdateRows(tableName, setList, condition);
        }

        private int ParseAndRun_Insert(string CommandText)
        {
            // isolate the elements.
            int firstOpenParentIdx = CommandText.IndexOf("(");
            int valuesKeywordIdx = CommandText.ToUpper().IndexOf("VALUES");

            if ((firstOpenParentIdx < 0) || (valuesKeywordIdx < 0))
            {
                throw new SimpleSqlDBException("Invalid INSERT statement: " + CommandText);
            }

            string tableName = CommandText.Substring(0, firstOpenParentIdx).Replace("INSERT", "").Replace("INTO", "").Trim();
            string insertColumnsStr = CommandText.Substring(firstOpenParentIdx, valuesKeywordIdx - firstOpenParentIdx).Replace("INSERT", "");
            string insertValuesStr = CommandText.Substring(valuesKeywordIdx + "VALUES".Length);

            insertColumnsStr = insertColumnsStr.Replace("INSERT", "").Replace("INTO", "").Replace("(", "").Replace(")", "").Trim();
            insertValuesStr = insertValuesStr.Replace("(", "").Replace(")", "").Trim();

            // Break column names and values out.
            string[] insertColumns = insertColumnsStr.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);
            string[] insertValues = GetElementsConsideringQuotes(insertValuesStr, ',').ToArray();

            if (insertColumns.Length != insertValues.Length)
            {
                throw new SimpleSqlDBException("Invalid INSERT statement (number of columns does not match number of values): " + CommandText);
            }

            // Pair up the names and values and then insert.
            List<Tuple<string, string>> colNamesAndValue = new List<Tuple<string, string>>();
            for(int idx=0;idx<insertColumns.Length; idx++)
            {
                colNamesAndValue.Add(new Tuple<string, string>(insertColumns[idx].Trim().Replace("[", "").Replace("]", ""), insertValues[idx]));
            }

            // Call into the database to update the rows.
            return SqlImpl_InsertRows(tableName, colNamesAndValue);
        }

        private List<string> GetElementsConsideringQuotes(string inputText, char delim)
        {
            List<string> resultSet = new List<string>();
            string work = "";
            bool inQuote = false;
            inputText = inputText + delim; // Simplify break logic by adding a final delimeter.

            for (int chIdx= 0; chIdx < inputText.Length;chIdx++)
            {
                char thisChar = inputText[chIdx];

                // Handle quotes.
                if (thisChar == '\'')
                {
                    inQuote = !inQuote;
                }

                // Break on value boundary if it is time.
                if ((thisChar == delim) && !inQuote)
                {
                    if (!string.IsNullOrWhiteSpace(work))
                    {
                        resultSet.Add(work);
                    }

                    work = "";
                }
                // Otherwise accumulate chars.
                else
                {
                    work += thisChar;
                }
            }

            return resultSet;
        }

        /// <summary>
        /// Implements "INSERT" logic on an already-parsed statement.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="colNamesAndValue"></param>
        /// <param name="parms"></param>
        /// <returns></returns>
        private int SqlImpl_InsertRows(string tableName, List<Tuple<string, string>> colNamesAndValue)
        {
            string tableNameUpper = tableName.ToUpper().Trim();

            // Make sure not a duplicate.
            if (!_tableDataByName.ContainsKey(tableNameUpper))
            {
                throw new SimpleSqlDBException("Table '" + tableNameUpper + "' does not exist.");
            }

            // Insert a new row.
            DataTable dt = _tableDataByName[tableNameUpper];
            DataRow row = dt.NewRow();

            foreach (Tuple<string, string> nameAndValue in colNamesAndValue)
            {
                // Validate the column name.
                string colNameUpper = nameAndValue.Item1.ToUpper().Trim().Replace("[", "").Replace("]", "");

                if (!row.Table.Columns.Contains(colNameUpper))
                {
                    throw new SimpleSqlDBException("Invalid column name '" + colNameUpper + "'.");
                }
                else
                {
                    int colIdx = dt.Columns.IndexOf(dt.Columns[colNameUpper.Replace("[", "").Replace("]", "")]);
                    UpdateColumnValue(row, row, null, colIdx, nameAndValue.Item2);
                }
            }

            dt.Rows.Add(row);
            return 1;
        }

        public IDataReader ExecuteReader(string CommandText, IDataParameterCollection parms)
        {
            return ExecuteReader(CommandText, parms, CommandBehavior.Default);
        }

        public IDataReader ExecuteReader(string CommandText, IDataParameterCollection parms, CommandBehavior behavior)
        {
            CommandText = unfoldBoundParms(CommandText, parms);

            lock (_tableDataByName)
            {
                string CommandTextUpper = CommandText.ToUpper();

                int selectIdx = CommandTextUpper.IndexOf("SELECT");
                int fromIdx = CommandTextUpper.IndexOf("FROM");
                int whereIdx = CommandTextUpper.IndexOf("WHERE");
                int orderByIdx = CommandTextUpper.IndexOf("ORDER BY");
                
                if (selectIdx < 0)
                {
                    throw new SimpleSqlDBException("Invalid query (missing SELECT clause): " + CommandText);
                }

                if (fromIdx < 0)
                {
                    throw new SimpleSqlDBException("Invalid SELECT statement (missing FROM clause): " + CommandText);
                }

                if (CommandText.Contains("JOIN"))
                {
                    throw new SimpleSqlDBException("Invalid SELECT statement (JOIN not supported): " + CommandText);
                }

                // Get the name of the table.
                string tableName = CommandTextUpper.Substring(fromIdx + "FROM".Length).TrimStart() + " ";
                int firstSpaceAfterFromIdx = tableName.IndexOf(" ");

                if (firstSpaceAfterFromIdx < 0)
                {
                    throw new SimpleSqlDBException("Invalid SELECT statement: " + CommandText);
                }
                else
                {
                    tableName = tableName.Substring(0, firstSpaceAfterFromIdx).Trim();
                }

                // Make sure the table exists.
                string tableNameUpper = tableName.ToUpper();
                if (tableNameUpper.Contains(","))
                {
                    throw new SimpleSqlDBException("Invalid SELECT statement (multi-table FROM not supported): " + CommandText);
                }
                else if (!_tableDataByName.ContainsKey(tableNameUpper))
                {
                    throw new SimpleSqlDBException("Table '" + tableNameUpper + "' does not exist.");
                }

                DataTable mainTable = _tableDataByName[tableNameUpper];

                // Get the list of columns we are selecting.  Replace any '*' with a list of all columns.
                string colListStr = CommandTextUpper.Substring(selectIdx + "SELECT".Length, fromIdx - selectIdx - "SELECT".Length).Trim();
                bool isDistinctUsed = false;

                // See if distinct is used.
                int distinctIdx = colListStr.IndexOf("DISTINCT");
                if (distinctIdx >= 0)
                {
                    isDistinctUsed = true;
                    colListStr = colListStr.Replace("DISTINCT", "").Trim();
                }

                if (colListStr == "*")
                {
                    // Get a list of columns.
                    colListStr = "";
                    foreach (DataColumn col in mainTable.Columns)
                    {
                        if (!string.IsNullOrWhiteSpace(colListStr))
                        {
                            colListStr += ",";
                        }

                        colListStr += col.ColumnName.Replace("[", "").Replace("]", "");
                    }
                }

                string[] colNameList = colListStr.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);

                // Handle any 'AS' situations by creating a native-column-to-AS-name map.
                bool couldBeAliases = CommandText.Contains(" AS ");
                Dictionary<string, string> colAliasMap = new Dictionary<string, string>();
                Dictionary<int, string> returnFieldNamesByColIdx = new Dictionary<int, string>();

                List<string> colNameListNoAliases = new List<string>();
                int colNameIdx = 0;

                foreach (string colName in colNameList)
                {
                    int asIdx = colName.IndexOf(" AS");
                    if (asIdx > 0)
                    {
                        string physicalName = colName.Substring(0, asIdx).Trim().Replace("[", "").Replace("]", "");
                        string colAlias = colName.Substring(asIdx + " AS".Length).Replace("'", "").Trim();

                        colAliasMap[physicalName] = colAlias;
                        returnFieldNamesByColIdx[colNameIdx++] = physicalName;

                        colNameListNoAliases.Add(physicalName);
                    }
                    else
                    {
                        colAliasMap[colName] = colName;
                        returnFieldNamesByColIdx[colNameIdx++] = colName.Replace("[", "").Replace("]", "");

                        colNameListNoAliases.Add(colName.Replace("[", "").Replace("]", ""));
                    }
                }

                // Get the WHERE portion.
                string conditionStr = (whereIdx > 0) ? CommandText.Substring(whereIdx + "WHERE".Length) : "";

                string orderByStr = null;

                if (orderByIdx > 0)
                {
                    int orderByIdxInWhere = conditionStr.IndexOf("ORDER BY");
                    orderByStr = conditionStr.Substring(orderByIdxInWhere + "ORDER BY".Length);
                    conditionStr = conditionStr.Substring(0, orderByIdxInWhere);
                    conditionStr = escapeColumnNames(tableName, conditionStr);
                }

                if (colNameList.Length == 0)
                {
                    throw new SimpleSqlDBException("Invalid SELECT statement (return column list is missing): " + CommandText);
                }

                // Create a DataTable to hold the result set.
                DataTable dtResultSet = new DataTable();
                dtResultSet.TableName = tableNameUpper;
                colNameIdx = 0;

                foreach (string colName in colNameListNoAliases)
                {
                    bool isFunction = colName.Contains("(");

                    Type returnType;
                    if (isFunction)
                    {
                        returnType = colName.StartsWith("COUNT") ? typeof(int) : typeof(decimal);
                    }
                    else
                    {
                        returnType = mainTable.Columns[colName.Replace("[", "").Replace("]", "")].DataType;
                    }

                    DataColumn col = new DataColumn();
                    col.ColumnName = colNameIdx++.ToString();
                    col.DataType = returnType;
                    dtResultSet.Columns.Add(col);
                }

                // Perform the query.
                DataRow[] matchingRows = mainTable.Select(conditionStr, orderByStr);

                foreach (DataRow mainRow in matchingRows)
                {
                    // Copy requested columns from the main table into the result set.
                    DataRow rowResultSet = dtResultSet.NewRow();

                    for (int colIdx=0; colIdx< returnFieldNamesByColIdx.Count; colIdx++)
                    {
                        UpdateColumnValue(mainRow, rowResultSet, null, colIdx, returnFieldNamesByColIdx[colIdx]);
                    }

                    dtResultSet.Rows.Add(rowResultSet);
                }

                // Apply DISTINCT as appropriate.
                if (isDistinctUsed)
                {
                    // Make a column name list.
                    string colNames = dtResultSet.Columns[0].ColumnName;
                    for(int colIdx = 1; colIdx < dtResultSet.Columns.Count; colIdx++)
                    {
                        colNames += "," + dtResultSet.Columns[colIdx].ColumnName;
                    }

                    dtResultSet = dtResultSet.DefaultView.ToTable(true, colNames);
                }

                // Apply aliases to honor any 'AS' clause.
                colNameIdx = 0;
                foreach (string colName in colAliasMap.Keys)
                {
                    dtResultSet.Columns[colNameIdx++.ToString()].ColumnName = colAliasMap[colName];
                }

                // Return a data reader.
                return dtResultSet.CreateDataReader();
            }
        }

        private int CountChar(string text, char ch)
        {
            int result = 0;
            foreach(char textCh in text)
            {
                if (textCh == ch)
                {
                    result++;
                }
            }
            return result;
        }

        public void DumpTableContentToConsole(string tableName)
        {
            // Get the table in question.
            string tableNameUpper = tableName.ToUpper();

            if (!_tableDataByName.ContainsKey(tableNameUpper))
            {
                return; // throw new OrderDataCacheException("Table '" + tableNameUpper + "' does not exist.");
            }

            DataTable dt = _tableDataByName[tableNameUpper];
            Console.WriteLine("===[ Table Dump: " + tableNameUpper + " ]===");

            // Dump column headers.
            foreach(DataColumn col in dt.Columns)
            {
                Console.Write(col.ColumnName.PadRight(20));
            }
            Console.WriteLine();

            // Dump row data.
            foreach (DataRow row in dt.Rows)
            {
                foreach (DataColumn col in dt.Columns)
                {
                    Console.Write(row[col.ColumnName].ToString().PadRight(20));
                }
                Console.WriteLine();
            }
        }

        // Creates a clone of this database with the same schema but no data.
        public SimpleSqlDB Clone()
        {
            lock(_tableDataByName)
            {
                SimpleSqlDB clonedDB = new SimpleSqlDB();

                foreach (string tableName in _tableDataByName.Keys)
                {
                    DataTable sourceTable = _tableDataByName[tableName];
                    DataTable clonedTable = sourceTable.Clone();

                    clonedDB._tableDataByName[tableName] = clonedTable;
                }

                return clonedDB;
            }
        }

        /// <summary>
        /// Clears all data from the given table.  Sematically the same as a "DELETE MYTABLE" with no conditional.
        /// </summary>
        /// <param name="tableName"></param>
        /// <retu*rns></returns>
        public int ClearTable(string tableName)
        {
            string tableNameUpper = tableName.ToUpper();

            lock(_tableDataByName)
            {
                // Make sure the table exists.
                if (!_tableDataByName.ContainsKey(tableNameUpper))
                {
                    throw new SimpleSqlDBException("Table '" + tableNameUpper + "' does not exist.");
                }

                // Clear out the whole table.
                DataTable dt = _tableDataByName[tableNameUpper];
                int rowsAffected = dt.Rows.Count;
                dt.Rows.Clear();

                return rowsAffected;
            }
        }

        /// <summary>
        /// Inserts data into a table given a set of ColumnName/Value pairs.  Semantically same as an INSERT.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="columnValuesByName"></param>
        public void InsertRowDirectly(string tableName, Dictionary<string, object> columnValuesByName)
        {
            string tableNameUpper = tableName.ToUpper();

            lock(_tableDataByName)
            {
                // Make sure the table exists.
                if (!_tableDataByName.ContainsKey(tableNameUpper))
                {
                    throw new SimpleSqlDBException("Table '" + tableNameUpper + "' does not exist.");
                }

                // Generate a row.
                DataTable dt = _tableDataByName[tableNameUpper];
                DataRow newRow = dt.NewRow();

                // Populate it.
                foreach (string colName in columnValuesByName.Keys)
                {
                    newRow[colName.ToUpper()] = columnValuesByName[colName];
                }

                try
                {
                    dt.Rows.Add(newRow);
                }
                catch (ConstraintException)
                {
                    if (!this.IgnoreConstraintViolationsUponInsert)
                    {
                        throw;
                    }
                }
            }
        }

        /// <summary>
        /// Inner logic for a "DROP" statement.
        /// </summary>
        /// <param name="tableName"></param>
        /// <returns></returns>
        private void SqlImpl_DropTable(string tableName)
        {
            string tableNameUpper = tableName.ToUpper();

            // Make sure the table exists.
            if (!_tableDataByName.ContainsKey(tableNameUpper))
            {
                throw new SimpleSqlDBException("Table '" + tableNameUpper + "' does not exist.");
            }

            _tableDataByName.Remove(tableName);
        }

        /// <summary>
        /// Replaces bound parm symbols with literals.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static string unfoldBoundParms(string sql, IDataParameterCollection parms)
        {
            string output = "";
            bool inQuote = false;
            int parmIdx = 0;

            foreach(char ch in sql)
            {
                if (ch == '\'')
                {
                    inQuote = !inQuote;
                }

                if (inQuote)
                {
                    output += ch;
                }
                else if (ch == '?')
                {
                    if (parmIdx >= parms.Count)
                    {
                        throw new ArgumentException("Number of bound parms does not match number of markers in statement.");
                    }

                    string literalStr;
                    object thisParm = ((IDataParameter)parms[parmIdx]).Value;

                    if (thisParm == null)
                    {
                        literalStr = "NULL";
                    }
                    else if (thisParm is string)
                    {
                        literalStr = "'" + thisParm.ToString().Replace("'", "''") + "'";
                    }
                    else if (thisParm is DateTime)
                    {
                        DateTime dt = (DateTime) thisParm;

                        literalStr = "#" + dt.Month + "/" + dt.Day + "/" + dt.Year + " " + dt.Hour + ":" + dt.Minute + ":" + dt.Second + "." + dt.Millisecond + "#";
                    }
                    else 
                    {
                        literalStr = thisParm.ToString();
                    }

                    output += literalStr;

                    parmIdx++;
                }
                else
                {
                    output += ch;
                }
            }

            return output;
        }

        /// <summary>
        /// Surrounds column names with "[" / "]" to account for possible special characters.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="conditionStr"></param>
        /// <returns></returns>
        private string escapeColumnNames(string tableName, string conditionStr)
        {
            string result = conditionStr;

            if (_tableDataByName.ContainsKey(tableName))
            {
                DataTable dt = _tableDataByName[tableName];

                foreach (DataColumn col in dt.Columns)
                {
                    result = ReplaceEx(result, col.ColumnName.Trim(), "[" + col.ColumnName + "]");
                }
            }

            return result;
        }

        /// <summary>
        /// Get a connection to this database.
        /// </summary>
        /// <returns></returns>
        public IDbConnection GetConnection()
        {
            return new SimpleSqlDBConnection(this);
        }

        /// <summary>
        /// This value is for special use cases where duplicate records are okay.
        /// </summary>
        public bool IgnoreConstraintViolationsUponInsert
        {
            get;
            set;
        }

        /// <summary>
        /// Extremely fast case-insensitive string replace.  Code is from:
        /// 
        /// http://www.codeproject.com/Articles/10890/Fastest-C-Case-Insenstive-String-Replace
        /// </summary>
        /// <param name="original"></param>
        /// <param name="pattern"></param>
        /// <param name="replacement"></param>
        /// <returns></returns>
        public static string ReplaceEx(string original, string pattern, string replacement)
        {
            int count, position0, position1;
            count = position0 = position1 = 0;
            string upperString = original.ToUpper();
            string upperPattern = pattern.ToUpper();
            int inc = (original.Length / pattern.Length) *
                      (replacement.Length - pattern.Length);
            char[] chars = new char[original.Length + Math.Max(0, inc)];
            while ((position1 = upperString.IndexOf(upperPattern,
                                              position0)) != -1)
            {
                for (int i = position0; i < position1; ++i)
                    chars[count++] = original[i];
                for (int i = 0; i < replacement.Length; ++i)
                    chars[count++] = replacement[i];
                position0 = position1 + pattern.Length;
            }
            if (position0 == 0) return original;
            for (int i = position0; i < original.Length; ++i)
                chars[count++] = original[i];
            return new string(chars, 0, count);
        } 
    }
}
