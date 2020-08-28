using System.Collections.Generic;

namespace agerman77.HBase.Client
{
    public class HBaseTable
    {
        public List<HBaseTableRow> Rows
        {
            get;
            set;
        }

        public List<HBaseTableColumn> Columns
        {
            get;
            set;
        }
    }
}