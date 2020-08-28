using Microsoft.HBase.Client.Filters;
using System;
using System.Collections.Generic;
using System.Text;

namespace agerman77.HBase.Client.Filters
{

    /// <summary>
    /// Allows the user to join different search criterias and indicate the type of join
    /// </summary>
    public class FilterJoint
    {
        List<SingleColumnValueFilter> _filterElements;
        string _filter;
        FilterType _filterType;

        public FilterJoint(List<SingleColumnValueFilter> filterElements, FilterType filterType)
        {
            _filterElements = filterElements;
            _filterType = filterType;

            SetFilter();
        }

        public List<SingleColumnValueFilter> FilterElements
        {
            get
            {
                return _filterElements;
            }
        }

        private void SetFilter()
        {
            StringBuilder stringBuilder = new StringBuilder();
            for(int i = 0; i < _filterElements.Count; i++)
            {
                stringBuilder.Append(_filterElements[i].ToString());
                if (i < (_filterElements.Count - 1))
                    stringBuilder.Append(",");
            }
            string filterPass = _filterType == FilterType.MustPassOne? FilterTypeList.MUST_PASS_ONE : FilterTypeList.MUST_PASS_ALL;
            filterPass = filterPass.Replace("FILTERING_COLUMNS", stringBuilder.ToString());
            _filter = filterPass;
        }

        public override string ToString()
        {
            return _filter;
        }

    }
}
