using System.Collections.Generic;

namespace agerman77.HBase.Client
{
    public class HBaseTableRow
    {
        public List<HBaseTableColumn> Columns
        {
            get;
            set;
        }
    }
}