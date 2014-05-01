using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using System.IO;
using MapReduce.NET.Serializer;

namespace MapReduce.NET.Output
{
    public class CSVJSONOutput : OutputPlugin
    {
        StreamWriter sw;

        public CSVJSONOutput(string fileLocation) : base(fileLocation, new JsonSerializer()) { }

        protected override void SaveItem<K,V>(K key, V value)
        {
            string keyString = serializer.Serialize(key) as string;
            string valueString = serializer.Serialize(value) as string;
            sw.WriteLine(String.Format("{0}\t{1}", keyString, valueString));
        }

        protected override void Open()
        {
            sw = new StreamWriter((string)Location);
        }

        protected override void Close()
        {
            sw.Close();
        }
    }
}
