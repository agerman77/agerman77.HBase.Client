using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using org.apache.hadoop.hbase.rest.protobuf.generated;

namespace agerman77.HBase.Client.Filters
{
    //http://hbase.apache.org/book.html#thrift.filter_language
    /// This filter takes a column family, a qualifier, a compare operator and a comparator. 
    /// If the specified column is not found – all the columns of that row will be emitted. 
    /// If the column is found and the comparison with the comparator returns true, all the columns of the row will be emitted. 
    /// If the condition fails, the row will not be emitted.


    /// <summary>
    /// Allows the user to set a search criteria in a single column of a table
    /// </summary>
    public class SingleColumnValueFilter : BaseScannerFilter
    {
        private string _columnFamily;
        private string _columnName;
        private string _columnValue;
        private int? _batch;
        private int? _startIndex;
        private string _filter;

        public SingleColumnValueFilter(string columnFamily, string columnName, string columnValue, int? batch = null, int? startIndex = null)
        {
            _columnFamily = columnFamily;
            _columnName = columnName;
            _columnValue = columnValue;
            _batch = batch;
            _startIndex = startIndex;
            _filter = FilterString.Replace("COL_NAME", Convert.ToBase64String(Encoding.UTF8.GetBytes(_columnName))).Replace("COL_FAMILY", Convert.ToBase64String(Encoding.UTF8.GetBytes(_columnFamily))).Replace("COL_VALUE", Convert.ToBase64String(Encoding.UTF8.GetBytes(_columnValue)));
        }

        protected override string FilterString
        {
            get
            {
                /*  //SINGLECOLUMNVALUEFILTER - EXAMPLE
               //Returns all rows where a determined value in a specified column (columnFamily + columnName) is found
               string singleColumnValueFilter = "{\"latestVersion\":true, 
                                  \"ifMissing\":true, 
                                  \"qualifier\":\"Y29sMQ==\", 
                                  \"family\":\"ZmFtaWx5\", 
                                  \"op\":\"EQUAL\", 
                                  \"type\":\"SingleColumnValueFilter\", 
                                  \"comparator\":{ 
                                  \"value\":\"MQ==\",
                                  \"type\":\"BinaryComparator\"}}";
           */

                return "{\"latestVersion\":true, \"ifMissing\":true, \"qualifier\":\"COL_NAME\", \"family\":\"COL_FAMILY\", \"op\":\"EQUAL\", \"type\":\"SingleColumnValueFilter\", \"comparator\":{ \"value\":\"COL_VALUE\",\"type\":\"BinaryComparator\"}}";
            }
        }


        internal override Scanner GetScanner()
        {
            Scanner scanner = new Scanner();
            if (_batch != null)
            {
                //scanner.batch = (int)_batch; 
                //batch, en el lenguaje HBase, indica el numero de celdas a devolver, no de filas...
                //es decir, que si le indicamos un batch de 10 y la tabla tiene 9 columnas solo nos devolvera
                //la primera fila completa y solo la primera celda de la segunda (9+1)
                if (_startIndex != null)
                {
                    scanner.startRow = BitConverter.GetBytes((int)_startIndex);
                    scanner.endRow = BitConverter.GetBytes(((int)_startIndex + (int)_batch));
                }
            }

            scanner.filter = _filter;
            return scanner;
        }

        public override string ToString()
        {
            return _filter;
        }
    }
}
