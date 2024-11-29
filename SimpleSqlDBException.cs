using System;

namespace Com.DeepBlue523.SimpleSqlMemoryDB
{
    public class SimpleSqlDBException : Exception
    {
        public SimpleSqlDBException()
        {
        }

        public SimpleSqlDBException(string message)
        : base(message)
    {
        }

        public SimpleSqlDBException(string message, Exception inner)
        : base(message, inner)
    {
        }
    }
}
