using System;
using System.Data;

namespace Com.DeepBlue523.SimpleSqlMemoryDB
{
    public class SimpleSqlDBResultSet : IDisposable
    {
        private SimpleSqlDBConnection _conn = null;
        private SimpleSqlDBCommand _cmd = null;
        private IDataReader _dataReader = null;
        private DataTable _schemaTable = null;
        private Type _connType;

        public SimpleSqlDBResultSet(SimpleSqlDBConnection conn, SimpleSqlDBCommand cmd, IDataReader dataReader, Type connType)
        {
            _conn = conn;
            _cmd = cmd;
            _connType = connType;
            _dataReader = dataReader;
        }

        public bool Next()
        {
            return _dataReader.Read();
        }

        public bool NextResult()
        {
            return _dataReader.NextResult();
        }

        public void Close()
        {
            // Close the data reader.
            if (_dataReader != null)
            {
                _dataReader.Close();
                _dataReader.Dispose();
                _dataReader = null;
            }
        }

        public int GetColumnCount()
        {
            return _dataReader.FieldCount;
        }

        public String GetColumnName(int colIdx)
        {
            return _dataReader.GetName(colIdx - 1);
        }

        public Type GetColumnType(int colIdx)
        {
            return _dataReader.GetFieldType(colIdx - 1);
        }

        public int GetColumnSize(int colIdx)
        {
            DataTable dt = GetSchemaTable();
				return (Int32)dt.Rows[colIdx - 1].ItemArray[2];
        }

        public int GetPrecision(int colIdx)
        {
            DataTable dt = GetSchemaTable();
				return (Int16)dt.Rows[colIdx - 1].ItemArray[3];
        }

        public int GetScale(int colIdx)
        {
            DataTable dt = GetSchemaTable();
				return (Int16)dt.Rows[colIdx - 1].ItemArray[4];
        }

        public String GetColumnTypeName(int colIdx)
        {
            if (_dataReader.GetFieldType(colIdx - 1) == typeof(decimal))
                return "DECIMAL";
            else if (_dataReader.GetFieldType(colIdx - 1) == typeof(int))
                return "NUMERIC";
            else if (_dataReader.GetFieldType(colIdx - 1) == typeof(long))
                return "NUMERIC";
            else if (_dataReader.GetFieldType(colIdx - 1) == typeof(DateTime))
                return "DATE";
            else
                return "CHAR";
        }

        public int GetInt(String colName)
        {
            return GetInt(GetColumnIndex(colName));
        }

        public int GetInt(int colIdx)
        {
            object valObj = _dataReader.GetValue(colIdx - 1);

            if (valObj == System.DBNull.Value)
                return 0;
            else if (valObj is decimal)
                return (int)((decimal)valObj);
            else if (valObj is double)
                return (int)((double)valObj);
            else
                return (int)valObj;
        }

        public decimal GetDecimal(String colName)
        {
            return GetDecimal(GetColumnIndex(colName));
        }

        public decimal GetDecimal(int colIdx)
        {
            object valObj = _dataReader.GetValue(colIdx - 1);

            if (valObj == System.DBNull.Value || valObj is System.String)
                return 0;
            else if (valObj is int)
                return (decimal)((int)valObj);
            else if (valObj is double)
                return (decimal)((double)valObj);
            else
                return (decimal)valObj;
        }

        public String GetString(String colName)
        {
            return GetString(GetColumnIndex(colName));
        }

        public String GetString(int colIdx)
        {
            if (_dataReader.GetValue(colIdx - 1) == System.DBNull.Value)
                return null;
            else
                return _dataReader.GetString(colIdx - 1);
        }

        public Object GetObject(String colName)
        {
            return GetObject(GetColumnIndex(colName));
        }

        public Object GetObject(int colIdx)
        {
            if (_dataReader.GetValue(colIdx - 1) == System.DBNull.Value)
                return null;
            else
                return _dataReader.GetValue(colIdx - 1);
        }

        private DataTable GetSchemaTable()
        {
            if (_schemaTable == null)
            {
                _schemaTable = _dataReader.GetSchemaTable();
            }
            return _schemaTable;
        }

        public int GetColumnIndex(String colName)
        {
            // Optimize!
            for (int i = 0; i < _dataReader.FieldCount; i++)
            {
                if (_dataReader.GetName(i) == colName)
                {
                    return i + 1;
                }
            }
            return -1;
        }

        public void Dispose()
        {
            if (_dataReader != null) 
            {
                _dataReader.Dispose();
            }

            GC.SuppressFinalize(this);
        }
    }
}
