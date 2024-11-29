using System;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace Com.DeepBlue523.SimpleSqlMemoryDB
{
    public class SimpleSqlDBParameter : IDbDataParameter
    {
        public SimpleSqlDBParameter()
        {
        }

        public byte Precision { get; set; }

        public byte Scale { get; set; }

        public int Size { get; set; }

        public DbType DbType { get; set; }

        public ParameterDirection Direction { get; set; }

        public bool IsNullable
        {
            get;
            set;
        }

        public string ParameterName { get; set; }

        public string SourceColumn { get; set; }

        public DataRowVersion SourceVersion { get; set; }

        public object Value { get; set; }
    }
}
