using System;
using System.Data;

namespace Com.DeepBlue523.SimpleSqlMemoryDB
{
    public class SimpleSqlDBConnection : IDbConnection, IDisposable
    {
        private SimpleSqlDB _database;

        public SimpleSqlDBConnection(SimpleSqlDB database)
        {
            _database = database;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        public string ConnectionString
        {
            get
            {
                return "OrderDataCacheConnection";
            }
            set
            {
            }
        }

        public int ConnectionTimeout
        {
            get;
            set;
        }

        public string Database
        {
            get;
            set;
        }

        public ConnectionState State
        {
            get;
            set;
        }

        public IDbTransaction BeginTransaction()
        {
            return null;
        }

        public IDbTransaction BeginTransaction(IsolationLevel il)
        {
            return null;
        }

        public void ChangeDatabase(string databaseName)
        {
        }

        public void Close()
        {
        }

        public IDbCommand CreateCommand()
        {
            return new SimpleSqlDBCommand(_database, this);
        }

        public void Open()
        {
        }
    }
}
