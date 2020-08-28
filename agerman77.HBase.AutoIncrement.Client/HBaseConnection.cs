using System;
using System.Collections.Generic;
using agerman77.HBase.Client;

namespace agerman77.HBase.AutoIncrement.Client
{
    public class HBaseConnection: HBase.Client.HBaseConnection
    {
        protected HBaseConnection(string serverURL, int? port): base(serverURL, port)
        {
        }

        protected HBaseConnection(string serverURL, string userName, string password, int? port): base(serverURL, userName, password, port)
        {
        }

        public override bool CreateNamespace(string ns)
        {
            if (!base.CreateNamespace(ns))  //create the namespace
                throw new Exception($"Namespace {ns} not created.");
            
            if (!base.CreateTable(ns, Helpers.AUTO_INCREMENT_KEYS_TABLE, new List<string>() { "T1" })) //create a table for the autoincrement keys
                throw new Exception("AutoIncrement table not created.");

            return true;
        }

        public new static IHBaseConnection CreateConnection(string serverURL, int? port)
        {
            return new HBaseConnection(serverURL, port);
        }

        public new static IHBaseConnection CreateConnection(string serverURL, string userName, string password, int? port)
        {
            return new HBaseConnection(serverURL, userName, password, port); ;
        }

        public override IHBaseCommand CreateCommand()
        {
            IHBaseCommand command = new HBaseCommand(this);
            return command;
        }

    }
}
