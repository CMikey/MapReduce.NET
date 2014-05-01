using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ProtoBuf;
using System.IO;
using System.Data.SQLite;
using MapReduce.NET.Serializer;

namespace MapReduce.NET.Output
{
    public class SQLiteProtoBufOutput : SQLiteOutput
    {
        public SQLiteProtoBufOutput(string dbFileLocation) : base(dbFileLocation, new ProtobufSerializer()) { }

        protected override void Open()
        {
            base.Open();

            paramKey = cmd.Parameters.Add("@key", System.Data.DbType.Binary);
            paramValue = cmd.Parameters.Add("@value", System.Data.DbType.Binary);
        }

        protected override void CreateTables()
        {
            SQLiteCommand createTable = new SQLiteCommand("CREATE TABLE Reduce (Key BLOB, Value BLOB)", conn);
            SQLiteCommand createIndex = new SQLiteCommand("CREATE INDEX ix_Key ON Reduce(Key ASC)", conn);

            createTable.ExecuteNonQuery();
            createIndex.ExecuteNonQuery();
        }
    }
}
