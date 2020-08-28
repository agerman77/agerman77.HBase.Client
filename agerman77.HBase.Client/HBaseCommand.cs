using System;
using System.Text;
using System.Collections.Generic;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System.Threading.Tasks;
using Microsoft.HBase.Client;

using agerman77.HBase.Client.Filters;

namespace agerman77.HBase.Client
{
    public class HBaseCommand: IHBaseCommand
    {
        IHBaseConnection _connection;

        protected HBaseCommand()
        {
        }

        public HBaseCommand(IHBaseConnection connection)
        {
            _connection = connection;
        }

        #region Find Rows by Keys
        
        /// <summary>
        /// Finds the first row in a table determined by a given key
        /// </summary>
        /// <param name="nameSpace">Namespace where the table is located.</param>
        /// <param name="tableName">Name of the table to find the row.</param>
        /// <param name="key">Key in the table to find.</param>
        /// <returns></returns>
        public HBaseTableRow FindRowByKey(string nameSpace, string tableName, object key)
        {
            try
            {
                string tblName = string.Format("{0}:{1}", nameSpace, tableName);
                CellSet cellSet = _connection.Client.GetCellsAsync(tblName, key.ToString()).Result;
                if (cellSet.rows.Count > 0)
                {
                    HBaseTableRow tableRow = new HBaseTableRow();
                    List<HBaseTableColumn> tableColumns = new List<HBaseTableColumn>();
                    CellSet.Row row = cellSet.rows[0];
                    foreach (Cell cell in row.values)
                    {
                        string colFullName = Encoding.UTF8.GetString(cell.column);
                        string colFamily = colFullName.Substring(0, colFullName.IndexOf(":"));
                        string colName = colFullName.Replace(string.Format("{0}:", colFamily), string.Empty);
                        HBaseTableColumn col = new HBaseTableColumn();
                        col.ColumnFamily = colFamily;
                        col.Name = colName;
                        col.Value = Encoding.UTF8.GetString(cell.data);
                        tableColumns.Add(col);
                    }
                    tableRow.Columns = tableColumns;
                    return tableRow;
                }
            }
            catch (AggregateException)
            {
                return null;
            }           
            return null;
        }

        /// <summary>
        /// Finds the rows in a table determined by their given keys
        /// </summary>
        /// <param name="nameSpace">Namespace where the table is located.</param>
        /// <param name="tableName">Name of the table to find the rows.</param>
        /// <param name="key">Keys in the table to find.</param>
        /// <returns></returns>
        public List<HBaseTableRow> FindRowsByKeys(string nameSpace, string tableName, string[] keys)
        {
            try
            {
                string tblName = string.Format("{0}:{1}", nameSpace, tableName);
                CellSet cellSet = _connection.Client.GetCellsAsync(tblName, keys).Result;
                if (cellSet.rows.Count > 0)
                {
                    List<HBaseTableRow> rowList = new List<HBaseTableRow>();
                    List<CellSet.Row> hRows = cellSet.rows; //rows found in the hbase server
                    foreach (CellSet.Row hRow in hRows)
                    {
                        HBaseTableRow row = new HBaseTableRow();
                        List<HBaseTableColumn> tableColumns = new List<HBaseTableColumn>();
                        foreach (Cell hCell in hRow.values)
                        {
                            string colFullName = Encoding.UTF8.GetString(hCell.column);
                            string colFamily = colFullName.Substring(0, colFullName.IndexOf(":"));
                            string colName = colFullName.Replace(string.Format("{0}:", colFamily), string.Empty);
                            HBaseTableColumn col = new HBaseTableColumn();
                            col.ColumnFamily = colFamily;
                            col.Name = colName;
                            col.Value = Encoding.UTF8.GetString(hCell.data);
                            tableColumns.Add(col);
                        }
                        row.Columns = tableColumns;
                        rowList.Add(row);
                    }
                    return rowList;
                }
            }
            catch (AggregateException)
            {
                return null;
            }
            return null;
        }

        #endregion

        #region Select / Find Rows

        /// <summary>
        /// Gets paged rows from a determined table
        /// </summary>
        /// <param name="nameSpace">Namespace where the table is located.</param>
        /// <param name="tableName">Name of the table to get the rows from.</param>
        /// <param name="BaseScannerFilter">Filter to be appended to the rows.</param>
        /// <returns></returns>
        public List<HBaseTableRow> FindRowsByScanner(string nameSpace, string tableName, BaseScannerFilter scannerFilter)
        {
            string tblName = string.Format("{0}:{1}", nameSpace, tableName);
            ScannerInformation scannerInfo = null;
            List<HBaseTableRow> tableRows = null;
            try
            {
                tableRows = new List<HBaseTableRow>();
                scannerInfo = _connection.Client.CreateScannerAsync(tblName, scannerFilter.GetScanner(), _connection.Options).Result;
                CellSet next = null;
                while ((next = _connection.Client.ScannerGetNextAsync(scannerInfo, _connection.Options).Result) != null)
                {
                    foreach (var row in next.rows)
                    {
                        HBaseTableRow tableRow = new HBaseTableRow();
                        List<HBaseTableColumn> columns = new List<HBaseTableColumn>();
                        foreach(Cell cell in row.values)
                        {
                            string colFullName = Encoding.UTF8.GetString(cell.column);
                            string colFamily = colFullName.Substring(0, colFullName.IndexOf(":"));
                            string colName = colFullName.Replace(string.Format("{0}:", colFamily), string.Empty);
                            HBaseTableColumn col = new HBaseTableColumn();
                            col.ColumnFamily = colFamily;
                            col.Name = colName;
                            col.Value = Encoding.UTF8.GetString(cell.data);
                            columns.Add(col);
                        }
                        tableRow.Columns = columns;
                        tableRows.Add(tableRow);
                    }
                }
                tableRows.TrimExcess();
                return tableRows;
            }
            catch(AggregateException)
            {
                return null;
            }
            finally
            {
                if (scannerInfo != null)
                {
                    _connection.Client.DeleteScannerAsync(tblName, scannerInfo, _connection.Options);
                }
            }
        }

        #endregion

        #region Insert / Delete Rows

        /// <summary>
        /// Inserts a record in a given table
        /// </summary>
        /// <param name="nameSpace">Namespace where the table is located.</param>
        /// <param name="tableName">Name of the table to insert the record.</param>
        /// <param name="columns">The columns and family column(s) to be used</param>
        public virtual string InsertRecord(string nameSpace, string tableName, List<HBaseTableColumn> columns)
        {
            string key = Guid.NewGuid().ToString();
            return InsertRecord(nameSpace, tableName, key, columns);
        }

        protected string InsertRecord(string nameSpace, string tableName, object key, List<HBaseTableColumn> columns)
        {
            string tblName = string.Format("{0}:{1}", nameSpace, tableName);
            var row = new CellSet.Row { key = Encoding.UTF8.GetBytes(key.ToString()) };
            foreach (HBaseTableColumn column in columns)
            {
                var value = new Cell
                {
                    column = Encoding.UTF8.GetBytes(string.Format("{0}:{1}", column.ColumnFamily, column.Name)),
                    data = Encoding.UTF8.GetBytes(column.Value.ToString())
                };
                row.values.Add(value);
            }
            var set = new CellSet();
            set.rows.Add(row);
            _connection.Client.StoreCellsAsync(tblName, set);
            return key.ToString();
        }

        /// <summary>
        /// Updates a record in a given table with a given key
        /// </summary>
        /// <param name="nameSpace">Namespace where the table is located.</param>
        /// <param name="tableName">Name of the table to insert the record.</param>
        /// <param name="key">The key to be used in the record</param>
        /// <param name="columns">The columns and family column(s) to be used</param>
        public void UpdateRecord(string nameSpace, string tableName, object key, List<HBaseTableColumn> columns)
        {
            HBaseTableRow row = FindRowByKey(nameSpace, tableName, key); //find the existing record by its id
            if (row == null) //if not found, throw exception
                throw new Exception($"Key {key} not found in table {nameSpace}:{tableName}");

            foreach (HBaseTableColumn column in row.Columns) //iterate thru every column in the row found in the table
            {
                //select the column (that matches the name from the columns parameter) that is present in the row
                HBaseTableColumn myCol = columns.Find(c => c.Name.Equals(column.Name)); 
                if (myCol == null) //if not found append it to the column list
                    columns.Add(column);
                    //if found, no need to append it
            }
            InsertRecord(nameSpace, tableName, key, columns);
        }

        /// <summary>
        /// Inserts a record in a given table with a given key
        /// </summary>
        /// <param name="nameSpace">Namespace where the table is located.</param>
        /// <param name="tableName">Name of the table to insert the record.</param>
        /// <param name="key">The key to be used in the record</param>
        public virtual void DeleteRecord(string nameSpace, string tableName, string key)
        {
            string tbl = string.Format("{0}:{1}", nameSpace, tableName);
            _connection.Client.DeleteCellsAsync(tbl, key);
        }

        #endregion

    }
}