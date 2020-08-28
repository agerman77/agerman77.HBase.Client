
namespace agerman77.HBase.Client.Filters
{
    public enum FilterType
    {
        MustPassOne, //Indicates that only one of the search criterias must be met
        MustPassAll //Indicates that all the search criterias must be met
    }

    internal static class FilterTypeList
    {
        internal static string MUST_PASS_ONE = "{\"type\": \"FilterList\",\"op\": \"MUST_PASS_ONE\",\"filters\": [ FILTERING_COLUMNS ] }";
        internal static string MUST_PASS_ALL = "{\"type\": \"FilterList\",\"op\": \"MUST_PASS_ALL\",\"filters\": [ FILTERING_COLUMNS ]}";
    }
}
