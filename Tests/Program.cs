using System;

using System.Collections.Generic;
using System.Threading.Tasks;
using agerman77.HBase.Client;
using agerman77.HBase.AutoIncrement.Client;
using System.Text;
using agerman77.HBase.Client.Filters;

namespace Tests
{
    class Program
    {
        static string _server;
        static int _port;
        static IHBaseConnection _conn;
        static IHBaseCommand _command;
        
        //static string _nameSpace = "MyAppDDBB";
        //static string _table = "MyTable";
        static string _nameSpace = "MyAppAutoIncDDBB"; //for testing autoincrement functionality
        static string _table = "MyAutoIncTable"; //for testing autoincrement functionality

        static List<string> _columnFamilies = new List<string>(){ "T1", "T2" };

        static void Main(string[] args)
        {
            InitializeGlobalVariables(); //initialize global variables

            //Test_Version_and_Status(); //test the version and status of the HBase Server

            //Test_Namespaces(); //check the namespaces] and create one

            Test_Tables(); //test the table functions

            //Test_Scan_SingleColumnValueFilter(); //test searches
            //Test_Scan_ValueFilter();
            //Test_Scan_MultipleColumnValueFilter();
        }

        static void InitializeGlobalVariables()
        {
            //_server = "http://192.168.77.128:16010";
            //_server = "http://192.168.77.128:8888";
            //_server = "http://192.168.77.128";

            _server = "192.168.77.128"; //HBase Server IP
            _port = 8888; //HBase Server REST Port (Rest service must be started at HBase)

            //create the HBaseConnection object
            //_conn = agerman77.HBase.Client.HBaseConnection.CreateConnection(_server, _port); //this starts the connection with the base library
            _conn = agerman77.HBase.AutoIncrement.Client.HBaseConnection.CreateConnection(_server, _port); //this starts the connection with the autoincrement library

            //create the HBaseCommand object
            _command = _conn.CreateCommand();
        }
      
        static void Test_Version_and_Status()
        {
            var clusterVersion = _conn.GetVersion(); //version
            var clusterStatus = _conn.GetStatus(); //status
        }

        static void Test_Namespaces()
        {
            List<string> nss = _conn.GetNamespaces(); //get the namespaces
            foreach (string ns in nss)
                Console.WriteLine($"Namespace: {ns}");

            bool created = _conn.CreateNamespace(_nameSpace); //create namespace
        }

        static void Test_Tables()
        {
            if (!_conn.TableExists(_nameSpace, _table)) //Check if table exists
            {
                 _conn.CreateTable(_nameSpace, _table, _columnFamilies); //create table
            }

            //insert first row
            List<HBaseTableColumn> tbls = new List<HBaseTableColumn>(); //set the columns and values
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Name", Value = "MyName4" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Lastname", Value = "MyLastname4" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Phone", Value = "+34666666666" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Email", Value = "myemail4@domain.com" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "ProfilePic", Value = "https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcRI56sSUImES2CQGRdyThbPWpUjc2xVukhYBg&usqp=CAU" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "Address", Value = "C/ somewhere in time, 77, 7-A" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "ZipCode", Value = "28000" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "City", Value = "Zaragoza" });
            tbls.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "Country", Value = "Spain" });

            string key = _command.InsertRecord(_nameSpace, _table, tbls); //Guid string returned by the insert function

            //insert second row
            List<HBaseTableColumn> tbls2 = new List<HBaseTableColumn>(); //set the columns and values
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Name", Value = "MyName5" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Lastname", Value = "MyLastname5" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Phone", Value = "+34777777777" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Email", Value = "myemail5@domain.com" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "ProfilePic", Value = "https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcRI56sSUImES2CQGRdyThbPWpUjc2xVukhYBg&usqp=CAU" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "Address", Value = "C/ somewhere in time, 77, 7-A" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "ZipCode", Value = "28000" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "City", Value = "Sevilla" });
            tbls2.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "Country", Value = "Spain" });

            string key2 = _command.InsertRecord(_nameSpace, _table, tbls2); //Guid string returned by the insert function

            //insert second row
            List<HBaseTableColumn> tbls3 = new List<HBaseTableColumn>(); //set the columns and values
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Name", Value = "MyName6" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Lastname", Value = "MyLastname6" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Phone", Value = "+34888888888" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Email", Value = "myemail6@domain.com" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "ProfilePic", Value = "https://encrypted-tbn0.gstatic.com/images?q=tbn%3AANd9GcRI56sSUImES2CQGRdyThbPWpUjc2xVukhYBg&usqp=CAU" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "Address", Value = "C/ somewhere in time, 77, 7-A" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "ZipCode", Value = "28000" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "City", Value = "Murcia" });
            tbls3.Add(new HBaseTableColumn() { ColumnFamily = "T2", Name = "Country", Value = "Spain" });

            string key3 = _command.InsertRecord(_nameSpace, _table, tbls3); //Guid string returned by the insert function

            if (!string.IsNullOrEmpty(key))
            {
                //lets try the FindRowByKey function to check the record really exists
                HBaseTableRow row = _command.FindRowByKey(_nameSpace, _table, key);
                if (row != null)
                {
                    //update the Name and Lastname in the row
                    tbls.Clear(); //clear the list and insert the elements again
                    tbls.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Name", Value = "MyName2" });
                    tbls.Add(new HBaseTableColumn() { ColumnFamily = "T1", Name = "Lastname", Value = "MyLastname" });

                    _command.UpdateRecord(_nameSpace, _table, key, tbls); //update the row

                    row = _command.FindRowByKey(_nameSpace, _table, key); //lets check the record again after update

                    _command.DeleteRecord(_nameSpace, _table, key); //delete the row

                    row = _command.FindRowByKey(_nameSpace, _table, key); //lets check if the record exists again
                }
            }

            Task<bool> f = _conn.DeleteTable(_nameSpace, _table);
            
        }

        static void Test_Scan_ValueFilter() 
        {
            BaseScannerFilter scannerFilter = new ValueFilter("Madrid", 10);
            List<HBaseTableRow> myRows = _command.FindRowsByScanner(_nameSpace, _table, scannerFilter);
        }

        static void Test_Scan_SingleColumnValueFilter()
        {
            BaseScannerFilter scannerFilter = new SingleColumnValueFilter("T1", "Email", "myemail@domain.com", 10);
            List<HBaseTableRow> myRows = _command.FindRowsByScanner(_nameSpace, _table, scannerFilter);
        }

        static void Test_Scan_MultipleColumnValueFilter()
        {
            SingleColumnValueFilter singleColumnValueFilter1 = new SingleColumnValueFilter("T1", "Email", "myemail@domain.com");
            SingleColumnValueFilter singleColumnValueFilter2 = new SingleColumnValueFilter("T2", "City", "Zaragoza");
            SingleColumnValueFilter singleColumnValueFilter3 = new SingleColumnValueFilter("T1", "Email", "myemail4@domain.com");

            List<SingleColumnValueFilter> filterElements = new List<SingleColumnValueFilter>();
            filterElements.Add(singleColumnValueFilter1);
            filterElements.Add(singleColumnValueFilter2);
            filterElements.Add(singleColumnValueFilter3);

            FilterJoint filterJoint = new FilterJoint(filterElements, FilterType.MustPassOne);
            List<FilterJoint> filterJointList = new List<FilterJoint>();
            filterJointList.Add(filterJoint);

            BaseScannerFilter multipleColumnScannerFilter = new MultipleColumnValueFilter(filterJointList);
            List<HBaseTableRow> myRows = _command.FindRowsByScanner(_nameSpace, _table, multipleColumnScannerFilter);
        }

    }
}
