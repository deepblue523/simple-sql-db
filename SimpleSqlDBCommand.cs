using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace Com.DeepBlue523.SimpleSqlMemoryDB
{
    /// <summary>
    /// Wrapper around IDbCommand that automatically disposes any associated IDataReader 
    /// instances when this command is disposed (cascading dispose).  Apart from this functionality
    /// this class is really just a wrapper around IDbCommand.
    /// </summary>
    public class SimpleSqlDBCommand : IDbCommand, IDisposable
    {
        private SimpleSqlDB _database;
        private SimpleSqlDBConnection _conn;
        private SimpleSqlDBParameterCollection _parms;
        private List<WeakReference> openResultSets = new List<WeakReference>();

        public SimpleSqlDBCommand(SimpleSqlDB database, SimpleSqlDBConnection conn)
        {
            _database = database;
            _conn = conn;
            _parms = new SimpleSqlDBParameterCollection();
        }

        public void AssociateResultSet(IDataReader resultSet)
        {
            WeakReference weakRef = new WeakReference(resultSet);
            openResultSets.Add(weakRef);
        }

        public string CommandText
        {
            get;
            set;
        }

        public int CommandTimeout
        {
            get;
            set;
        }

        public CommandType CommandType
        {
            get
            {
                return System.Data.CommandType.Text;
            }
            set
            {
            }
        }

        public IDbConnection Connection
        {
            get { return _conn; }
            set { _conn = (SimpleSqlDBConnection)value; }
        }

        public IDataParameterCollection Parameters
        {
            get { return _parms; }
        }

        public IDbTransaction Transaction
        {
            get { return null; }
            set { }
        }

        public UpdateRowSource UpdatedRowSource
        {
            get { return UpdateRowSource.None; }
            set { }
        }

        public void Cancel()
        {
        }

        public IDbDataParameter CreateParameter()
        {
            return new SimpleSqlDBParameter();
        }

        public int ExecuteNonQuery()
        {
            try
            {
                /*if (CommandText.Contains("OEP55B(") || CommandText.Contains("OEP55B "))
                {
                    System.Console.WriteLine(SimpleSqlDB.unfoldBoundParms(CommandText, _parms));
                }*/

                //Console.WriteLine("SQL: " + CommandText);
                int rowsAffected = _database.ExecuteNonQuery(CommandText, Parameters);
                //Console.WriteLine("Rows Affected: " + rowsAffected);
                return rowsAffected;
            }
            finally
            {
                /*if (CommandText.Contains("OEP55B(") || CommandText.Contains("OEP55B "))
                {
                    _database.DumpTableContentToConsole("OEP55B");
                }*/

                //_database.DumpTableContentToConsole("OEP40_SNAP");
            }
        }

        public IDataReader ExecuteReader()
        {
            /*if (CommandText.Contains("OEP55B "))
            {
                System.Console.WriteLine(SimpleSqlDB.unfoldBoundParms(CommandText, _parms));
                _database.DumpTableContentToConsole("OEP55B");
            }*/

            //Console.WriteLine("SQL: " + CommandText);
            return _database.ExecuteReader(CommandText, Parameters);
        }

        public IDataReader ExecuteReader(CommandBehavior behavior)
        {
            //Console.WriteLine("SQL: " + CommandText);
            return _database.ExecuteReader(CommandText, Parameters, behavior);
        }

        public object ExecuteScalar()
        {
            return null;
        }

        public void Prepare()
        {
        }

        public void Dispose()
        {
            // Dispose of any result sets.
            foreach (WeakReference weakRef in openResultSets)
            {
                if (weakRef.IsAlive)
                {
                    IDataReader dataReader = (IDataReader)weakRef.Target;
                    if ((dataReader != null) && !dataReader.IsClosed)
                    {
                        try { dataReader.Dispose(); GC.SuppressFinalize(this); }
                        catch { /* Chew it up - ain't much we can do! */ }
                    }
                }
            }
            openResultSets.Clear();
        }
    }
}
