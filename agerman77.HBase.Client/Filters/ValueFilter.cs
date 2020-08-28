using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using org.apache.hadoop.hbase.rest.protobuf.generated;

namespace agerman77.HBase.Client.Filters
{
    //http://hbase.apache.org/book.html#thrift.filter_language
    /// This filter takes a compare operator and a comparator. 
    /// It compares each value with the comparator using the compare operator and if the comparison returns true, 
    /// it returns that key-value.
 
    /// <summary>
    /// Allows the user to set a search criteria thru all the columns of a table
    /// </summary>
    public class ValueFilter : BaseScannerFilter
    {
        private string _value;
        private int? _batch;
        private int? _startIndex;
        private string _filter;

        public ValueFilter(string value, int? batch = null, int? startIndex = null)
        {
            _value = value;
            _batch = batch;
            _startIndex = startIndex;
            _filter = FilterString.Replace("COL_VALUE", Convert.ToBase64String(Encoding.UTF8.GetBytes(_value)));
        }


        protected override string FilterString
        {
            get
            {
                /*  //VALUEFILTER
                    //Returns all rows where a determined value is found (searches all the columns for that value)
                    string valueFilter = "{\"latestVersion\":true, 
                        \"ifMissing\":true, 
                        \"op\":\"EQUAL\", 
                        \"type\":\"ValueFilter\", 
                        \"comparator\":{ 
                        \"value\":\"MQ==\",
                        \"type\":\"BinaryComparator\"}}";
                */

                return "{\"latestVersion\":true, \"ifMissing\":true, \"op\":\"EQUAL\", \"type\":\"ValueFilter\", \"comparator\":{ \"value\":\"COL_VALUE\",\"type\":\"BinaryComparator\"}}";
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
