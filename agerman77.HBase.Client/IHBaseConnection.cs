using Microsoft.HBase.Client;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace agerman77.HBase.Client
{
    public interface IHBaseConnection
    {
        bool CreateNamespace(string ns);
        List<string> GetNamespaces();
        IHBaseCommand CreateCommand();
        org.apache.hadoop.hbase.rest.protobuf.generated.Version GetVersion();
        StorageClusterStatus GetStatus();
        bool TableExists(string nameSpace, string tableName);
        bool CreateTable(string nameSpace, string tableName, List<string> columnFamilies);
        Task<bool> DeleteTable(string nameSpace, string tableName);
        HBaseClient Client { get; }
        RequestOptions Options { get; }
    }   
}
