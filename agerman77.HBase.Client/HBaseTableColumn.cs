namespace agerman77.HBase.Client
{
    public class HBaseTableColumn
    {
        public string Name
        {
            get;
            set;
        }

        public object Value
        {
            get;
            set;
        }

        public string ColumnFamily
        {
            get;
            set;
        }
    }
}