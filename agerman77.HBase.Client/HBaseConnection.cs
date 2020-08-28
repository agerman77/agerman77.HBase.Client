using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.HBase.Client;
using Microsoft.HBase.Client.Requester;
using Microsoft.HBase.Client.LoadBalancing;
using org.apache.hadoop.hbase.rest.protobuf.generated;
using ProtoBuf;
using agerman77.HBase.Client.Requester;

namespace agerman77.HBase.Client
{
    public class HBaseConnection: IHBaseConnection
    {
        private HBaseConnection connection;
        private string _serverURL;
        private string _username;
        private string _password;
        private int _port;
        private HBaseClient _client;
        private RequestOptions _requestOptions;
        private ClusterCredentials _credentials;
        private LoadBalancerRoundRobin _loadBalancerRoundRobin;

        #region Private methods / functions / properties / constructors

        protected HBaseConnection()
        {
        }

        /// <summary>
        /// HBaseConnection used by the HBaseCommand
        /// </summary>
        public HBaseClient Client
        {
            get
            {
                return _client;
            }
        }

        /// <summary>
        /// Request options for the REST connection to the server
        /// </summary>
        public RequestOptions Options
        {
            get
            {
                return _requestOptions;
            }
        }

        /// <summary>
        /// Creates a HBaseConnection to the server
        /// </summary>
        /// <param name="serverURL">URL to the server.</param>
        protected HBaseConnection(string serverURL, int? port)
        {
            connection = new HBaseConnection();
            _serverURL = serverURL; 
            _requestOptions = RequestOptions.GetDefaultOptions();
            _requestOptions.AlternativeEndpoint = string.Empty;
            if (port != null)
            {
                _requestOptions.Port = (int)port;// port 'hbase rest service' was started at;
                _port = _requestOptions.Port;
            }

            if (serverURL.StartsWith("http"))
                _credentials = new ClusterCredentials(new Uri(serverURL), "nouser", "nopass");

            _loadBalancerRoundRobin = new LoadBalancerRoundRobin(new List<string> { _serverURL });
            _client = new HBaseClient(_credentials, _requestOptions, _loadBalancerRoundRobin);
        }

        /// <summary>
        /// Creates a HBaseConnection to the server
        /// </summary>
        /// <param name="serverURL">URL to the server.</param>
        /// <param name="userName">Username to the server.</param>
        /// <param name="password">Password to the server.</param>
        protected HBaseConnection (string serverURL, string userName, string password, int? port)
        {
            connection = new HBaseConnection();
            _serverURL = serverURL;
            _username = userName;
            _password = password;
            _requestOptions = RequestOptions.GetDefaultOptions();
            _requestOptions.AlternativeEndpoint = string.Empty;
            if (port != null)
            {
                _requestOptions.Port = (int)port;// port 'hbase rest service' was started at;
                _port = _requestOptions.Port;
            }

            if (serverURL.StartsWith("http"))
                _credentials = new ClusterCredentials(new Uri(serverURL), _username, _password);

            _loadBalancerRoundRobin = new LoadBalancerRoundRobin(new List<string> { _serverURL });
            _client = new HBaseClient(_credentials, _requestOptions, _loadBalancerRoundRobin);
        }

        #endregion

        #region Public constructors

        public static IHBaseConnection CreateConnection(string serverURL, int? port)
        {
            return new HBaseConnection(serverURL, port);
        }

        public static IHBaseConnection CreateConnection(string serverURL, string userName, string password, int? port)
        {
            return new HBaseConnection(serverURL, userName, password, port);
        }

        #endregion

        /// <summary>
        /// Returns a command object to be used over this HBaseConnection
        /// </summary>
        /// <returns></returns>
        public virtual IHBaseCommand CreateCommand()
        {
            IHBaseCommand command = new HBaseCommand(this);
            return command;
        }

        #region Table Commands 

        /// <summary>
        /// Lists all the tables in a determined namespace
        /// </summary>
        /// <param name="nameSpace"></param>
        public List<string> GetTables(string nameSpace)
        {
            return ExecuteQuery<List<string>>(string.Format("namespaces/{0}/tables", nameSpace));
        }

        /// <summary>
        /// Verifies if a given table exists in a determined namespace in the HBase Server 
        /// </summary>
        /// <param name="nameSpace">Name of the namespace where search will be executed.</param>
        /// <param name="tableName">Name of the table to verify existence.</param>
        /// <returns></returns>
        public bool TableExists(string nameSpace, string tableName)
        {
            List<string> tables = GetTables(nameSpace);
            if (tables != null && tables.Count > 0)
                return tables.Contains(tableName);

            return false;
        }

        /// <summary>
        /// Creates a table in a determined namespace in the HBase Server
        /// </summary>
        /// <param name="nameSpace">Name of the namespace where to create the table.</param>
        /// <param name="tableName">Name of the table to be created.</param>
        /// <param name="columns">Columns in the table.</param>
        /// <returns></returns>
        public bool CreateTable(string nameSpace, string tableName, List<string> columnFamilies)
        {
            bool exists = TableExists(nameSpace, tableName);
            if (!exists)
            {
                if (columnFamilies != null && columnFamilies.Count > 0)
                {
                    // Create the table
                    var newTableSchema = new TableSchema { name = string.Format("{0}:{1}", nameSpace, tableName) };
                    //add the columns
                    foreach (string columnFamily in columnFamilies)
                        newTableSchema.columns.Add(new ColumnSchema() { name = columnFamily });

                    return _client.CreateTableAsync(newTableSchema).Result;
                }
            }
            throw new Exception($"Table {nameSpace}:{tableName} exists.");
        }

        /// <summary>
        /// Deletes a table from a determined namespace in the HBase Server
        /// </summary>
        /// <param name="nameSpace">Name of the namespace where the table will be deleted from.</param>
        /// <param name="tableName">Name of the table to be deleted.</param>
        /// <returns></returns>
        public async Task<bool> DeleteTable(string nameSpace, string tableName)
        {
            bool exists = TableExists(nameSpace, tableName);

            if (!exists)
                throw new Exception($"Table {tableName} not found in namespace {nameSpace}.");
            try
            {
                await _client.DeleteTableAsync(string.Format("{0}:{1}", nameSpace, tableName));
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Namespaces Commands

        /// <summary>
        /// Returns a list of existing namespaces in the HBase Server
        /// </summary>
        /// <returns></returns>
        public List<string> GetNamespaces()
        {
            return ExecuteQuery<List<string>>("namespaces");
        }

        /// <summary>
        /// Determines whether a namespace exists in the HBase Server
        /// </summary>
        /// <param name="ns"></param>
        /// <returns></returns>
        public bool NamespaceExists(string ns)
        {
            List<string> nss = GetNamespaces(); //returns the list of existing namespaces
            if (nss != null)
                if (nss.Contains(ns))
                    return true;

            return false;
        }

        /// <summary>
        /// Creates a namespace into the HBase Server
        /// </summary>
        /// <param name="ns"></param>
        public virtual bool CreateNamespace(string ns)
        {
            if (NamespaceExists(ns)) //verify whether the namespace already exists
                throw new Exception(string.Format("{0} already exists in the HBase Server.", ns));

            ExecuteQuery<string>(string.Format("namespaces/{0}", ns), "POST"); //create the namespace
            return NamespaceExists(ns); //return whether the namespace exists
        }

        /// <summary>
        /// Deletes a namespace from the HBase Server (It should be empty first)
        /// </summary>
        /// <param name="ns"></param>
        /// <returns></returns>
        public bool DeleteNamespace(string ns)
        {
            if (!NamespaceExists(ns)) //verify whether the namespace exists
                throw new Exception(string.Format("{0} does not exist in the HBase Server.", ns));

            ExecuteQuery<string>(string.Format("namespaces/{0}", ns), "DELETE");
            return !NamespaceExists(ns);
        }

        //TODO: Create method DropNamespace (First tables must be deleted and second it's the namespace)

        #endregion

        #region deprecated
        //not working (deprecated)
        //public void AlterNamespace(string ns)
        //{
        //    if (!NamespaceExists(ns)) //verify whether the namespace already exists
        //        throw new Exception(string.Format("{0} does not exist in the HBase Server.", ns));

        //     string st = ExecuteQuery<string>(string.Format("namespaces/{0}", ns), "PUT");
        //}

        //not working
        //public string GetNamespaceInfo(string ns)
        //{
        //    var j = ExecuteQuery<string>(string.Format("namespaces/{0}", ns), "GET");
        //    return j;// ExecuteQuery<List<string>>(string.Format("namespaces/{0}", ns), "GET");
        //}

        #endregion

        #region Version / Status Commands

        /// <summary>
        /// Gets the version of the HBase Server
        /// </summary>
        /// <returns></returns>
        public org.apache.hadoop.hbase.rest.protobuf.generated.Version GetVersion()
        {
            return _client.GetVersionAsync().Result;
        }

        /// <summary>
        /// Gets the Status of the HBase Server
        /// </summary>
        /// <returns></returns>
        public StorageClusterStatus GetStatus()
        {
            return _client.GetStorageClusterStatusAsync().Result;
        }

        #endregion

        #region Execute commands

        private T ExecuteQuery<T>(string query, string method = "GET")
        {
            string m = method != "GET" ? method : "GET";
            Stream stream = new MemoryStream();
            HBaseWebRequester req = new HBaseWebRequester(_loadBalancerRoundRobin);
            Response response = req.IssueWebRequest(string.Empty, query, m, null, _requestOptions, true);
            using (Stream responseStream = response.WebResponse.GetResponseStream())
            {
                var b = Serializer.Deserialize<T>(responseStream);
                responseStream.Close();
                return b;
            }
        }

        private T ExecuteTypedQuery<T>(string query, string method = "GET")
        {
            string m = method != "GET" ? method : "GET";
            Stream stream = new MemoryStream();
            HBaseWebRequester req = new HBaseWebRequester(_loadBalancerRoundRobin, "text/xml");
            Response response = req.IssueWebRequest(string.Empty, query, m, null, _requestOptions, true);
            using (Stream responseStream = response.WebResponse.GetResponseStream())
            {
                using (StreamReader reader = new StreamReader(responseStream))
                {
                    //string str = reader.ReadToEnd(); //for testing purposes only - delete on production
                    var serializer = new System.Xml.Serialization.XmlSerializer(typeof(T));
                    var b = (T)serializer.Deserialize(reader);
                    reader.Close();
                    responseStream.Close();
                    return b;
                }
            }
        }

        #endregion

        #region No Namespace commands   
        
        //Uncomment at your own desire - 
        //Not recommended as namespaces allow developers to group tables as in a database environment

/*
        /// <summary>
        /// Verifies if a given table exists in the HBase Server
        /// </summary>
        /// <param name="tableName">Name of the table to verify existence.</param>
        /// <returns></returns>
        public bool TableExists(string tableName)
        {
            return _client.ListTablesAsync().Result.name.Contains(tableName);
        }

        /// <summary>
        /// Creates a table in the HBase Server
        /// </summary>
        /// <param name="tableName">Name of the table to be created.</param>
        /// <param name="columns">Columns in the table.</param>
        /// <returns></returns>
        public bool CreateTable(string tableName, List<string> columnFamilies)
        {
            bool exists = TableExists(tableName);
            if (!exists)
            {
                if (columnFamilies != null && columnFamilies.Count > 0)
                {
                    // Create the table
                    var newTableSchema = new TableSchema { name = tableName };
                    //add the columns
                    foreach (string columnFamily in columnFamilies)
                        newTableSchema.columns.Add(new ColumnSchema() { name = columnFamily });

                    return _client.CreateTableAsync(newTableSchema).Result;
                }
            }
            throw new Exception($"Table {tableName} exists.");
        }

        /// <summary>
        /// Deletes a table from the HBase Server
        /// </summary>
        /// <param name="tableName">Name of the table to be deleted.</param>
        /// <returns></returns>
        public bool DeleteTable(string tableName)
        {
            bool exists = client.ListTablesAsync().Result.name.Contains(tableName);

            if (!exists)
                throw new Exception($"Table {tableName} not found.");
            try
            {
                _client.DeleteTableAsync(tableName);
                return true;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
*/
        #endregion

    }
}