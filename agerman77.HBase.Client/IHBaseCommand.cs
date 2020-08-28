using System;
using System.Collections.Generic;
using agerman77.HBase.Client.Filters;
namespace agerman77.HBase.Client
{
    public interface IHBaseCommand
    {
        HBaseTableRow FindRowByKey(string nameSpace, string tableName, object key);
        List<HBaseTableRow> FindRowsByKeys(string nameSpace, string tableName, string[] keys);
        List<HBaseTableRow> FindRowsByScanner(string nameSpace, string tableName, BaseScannerFilter scannerFilter);
        string InsertRecord(string nameSpace, string tableName, List<HBaseTableColumn> columns);
        void UpdateRecord(string nameSpace, string tableName, object key, List<HBaseTableColumn> columns);
        void DeleteRecord(string nameSpace, string tableName, string key);
    }
}
