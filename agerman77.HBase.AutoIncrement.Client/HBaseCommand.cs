
using agerman77.HBase.Client;
using System;
using System.Collections.Generic;

namespace agerman77.HBase.AutoIncrement.Client
{
    public class HBaseCommand: HBase.Client.HBaseCommand
    {

        protected HBaseCommand()
        {
        }

        public HBaseCommand(IHBaseConnection connection): base(connection)
        {
        }

        public override string InsertRecord(string nameSpace, string tableName, List<HBaseTableColumn> columns)
        {
            int lastRecordId = GetLastRecordId(nameSpace, tableName); //get the last Id for this table
            int newRecordId =+ (lastRecordId + Helpers.IDENTITY_INCREMENT); //we add the value of the increment to this id
            columns = GetFullTableSchema(newRecordId, columns); //get the full schema (with Id and Active column)
            base.InsertRecord(nameSpace, tableName, newRecordId, columns); //insert the record in the table with the new Id
            UpdateRecordId(nameSpace, tableName, newRecordId); //update the table that stores this table's last id
            return newRecordId.ToString();
        }

        public override void DeleteRecord(string nameSpace, string tableName, string key)
        {
            List<HBaseTableColumn> cols = new List<HBaseTableColumn>();
            cols = GetFullTableSchema(Int32.Parse(key), cols); //get the full schema (with Id and Active column)
            cols[1].Value = 0; //set the active column as 0
            base.UpdateRecord(nameSpace, tableName, key, cols);
        }

        #region AutoIncrement functions / methods

        private List<HBaseTableColumn> GetFullTableSchema(int recordId, List<HBaseTableColumn> cols)
        {
            bool hasIdColumn = false;
            bool hasActiveColumn = false;
            List<HBaseTableColumn> scCols = cols;
            foreach (HBaseTableColumn col in scCols)
            {
                if (col.Name.Equals("Id"))
                {
                    hasIdColumn = true;
                    col.Value = recordId;
                }
                if (col.Name.Equals("Active"))
                {
                    hasActiveColumn = true;
                    col.Value = 1;
                }
            }

            if (!hasIdColumn)
                scCols.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Id", Value = recordId });

            if (!hasActiveColumn)
                scCols.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Active", Value = 1 });

            return scCols;
        }

        private int GetLastRecordId(string nameSpace, string tableName)
        {
            //find the row with this key (tableName)                                      
            HBaseTableRow row = base.FindRowByKey(nameSpace, Helpers.AUTO_INCREMENT_KEYS_TABLE, tableName); //the table name is set as the key to this record
            int lastId;
            if (row == null) //if not row is found
            {
                lastId = (Helpers.IDENTITY_SEED - 1);
                //insert the first id with this table name
                InsertRecord(nameSpace, Helpers.AUTO_INCREMENT_KEYS_TABLE, tableName,
                                             new List<HBaseTableColumn>()
                                                {
                                                            new HBaseTableColumn() {ColumnFamily="T1", Name="TableName", Value=tableName},
                                                            new HBaseTableColumn() {ColumnFamily="T1", Name="LastId", Value=lastId}
                                                });
            }
            else
                lastId = Int32.Parse(row.Columns[0].Value.ToString());

            return lastId;
        }

        private void UpdateRecordId(string nameSpace, string tableName, int newRecordId)
        {
                UpdateRecord(nameSpace, Helpers.AUTO_INCREMENT_KEYS_TABLE, tableName,
                                            new List<HBaseTableColumn>()
                                                {
                                                            new HBaseTableColumn() {ColumnFamily="T1", Name="TableName", Value=tableName},
                                                            new HBaseTableColumn() {ColumnFamily="T1", Name="LastId", Value=newRecordId}
                                                }

                );
        }

        #endregion

    }
}
