using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace agerman77.HBase.AutoIncrement.Client
{
    public static class Helpers
    {
        public static string AUTO_INCREMENT_KEYS_TABLE = "AUTO_INCREMENT_KEYS_TABLE"; //here's where the autoincrement keys will be stored
        public static int IDENTITY_SEED = 1; // specifies the column's first value
        public static int IDENTITY_INCREMENT = 1; //value to sum to the last record id to generate the next id
    }
}
