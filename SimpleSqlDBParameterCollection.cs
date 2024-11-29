using System;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace Com.DeepBlue523.SimpleSqlMemoryDB
{
    public class SimpleSqlDBParameterCollection : ArrayList, IDataParameterCollection
    {
        private SimpleSqlDBConnection _conn;

        public SimpleSqlDBParameterCollection()
        {
        }

        //
        // Summary:
        //     Gets or sets the parameter at the specified index.
        //
        // Parameters:
        //   parameterName:
        //     The name of the parameter to retrieve.
        //
        // Returns:
        //     An System.Object at the specified index.
        public object this[string parameterName]
        {
            get
            {
                int idxForParm = findIndexForParm(parameterName);
                if (idxForParm >= 0)
                {
                    return ((SimpleSqlDBParameter)this[idxForParm]).Value;
                }
                else
                {
                    throw new ArgumentException();
                }
            }
            set
            {
                int idxForParm = findIndexForParm(parameterName);
                if (idxForParm >= 0)
                {
                    ((SimpleSqlDBParameter)this[idxForParm]).Value = value;
                }
                else
                {
                    throw new ArgumentException();
                }
            }
        }

        public bool Contains(string parameterName)
        {
            int idxForParm = findIndexForParm(parameterName);
            return (idxForParm >= 0);
        }

        public int IndexOf(string parameterName)
        {
            return findIndexForParm(parameterName);
        }

        public void RemoveAt(string parameterName)
        {
            int idxForParm = findIndexForParm(parameterName);
            this.RemoveAt(idxForParm);
        }

        private int findIndexForParm(string parmName)
        {
            for(int i = 0; i < this.Count; i++)
            {
                if (((SimpleSqlDBParameter)this[i]).ParameterName == parmName)
                {
                    return i;
                }
            }

            return -1;
        }
    }
}
