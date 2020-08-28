using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using org.apache.hadoop.hbase.rest.protobuf.generated;

namespace agerman77.HBase.Client.Filters
{
    /// <summary>
    /// Allows the user to set different search criterias thru different columns of a table
    /// </summary>
    public class MultipleColumnValueFilter : BaseScannerFilter
    {
        private int? _batch;
        private int? _startIndex;
        List<FilterJoint> _filterJoints;
        private string _filter;

        public MultipleColumnValueFilter(List<FilterJoint> filterJoints, int? batch = null, int? startIndex = null)
        {
            _filterJoints = filterJoints;
            _batch = batch;
            _startIndex = startIndex;
            SetFilter();
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

        private void SetFilter()
        {
            StringBuilder stringBuilder = new StringBuilder();
            
            foreach (FilterJoint joint in _filterJoints)
                stringBuilder.Append(joint.ToString());

            string filterMustPassAll = string.Empty;
            int filterJointCount = _filterJoints.Count;
            if (filterJointCount > 1)
            {
                filterMustPassAll = FilterTypeList.MUST_PASS_ALL;
                filterMustPassAll = filterMustPassAll.Replace("FILTERING_COLUMNS", stringBuilder.ToString());
                _filter = filterMustPassAll;
                return;
            }

            _filter = stringBuilder.ToString();
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
