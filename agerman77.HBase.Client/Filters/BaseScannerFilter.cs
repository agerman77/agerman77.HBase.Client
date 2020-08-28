using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace agerman77.HBase.Client.Filters
{
    public abstract class BaseScannerFilter
    {
        protected BaseScannerFilter()
        {
        }

        protected virtual string FilterString
        {
            get
            {
                throw new Exception("FilterString property must be implemented in derived class.");
            }
        }

        public override string ToString()
        {
            throw new Exception("ToString function must be implemented in derived class.");
        }

        internal virtual Scanner GetScanner()
        {
            throw new Exception("GetScanner function must be implemented in derived class.");
        }
    }
}
