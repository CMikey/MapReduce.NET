using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using MapReduce.NET.Serializer;
using System.IO;

namespace MapReduce.NET.Output
{
    public class SQLiteOutput : OutputPlugin
    {
        protected SQLiteConnection conn;
        protected SQLiteCommand cmd;
        protected SQLiteParameter paramKey;
        protected SQLiteParameter paramValue;
        protected SQLiteTransaction tran;

        public SQLiteOutput(string dbFileLocation) : base(dbFileLocation, new JsonSerializer()) { }
        protected SQLiteOutput(string dbFileLocation, ISerializer serializer) : base(dbFileLocation, serializer) { }

        protected int recordCount;

        protected override void SaveItem<K,V>(K key, V value)
        {
            paramKey.Value = serializer.Serialize(key);
            paramValue.Value = serializer.Serialize(value);
            cmd.ExecuteNonQuery();

            if (recordCount++ % 1000 == 0 && tran != null)
            {
                tran.Commit();
                tran = conn.BeginTransaction();
            }
        }

        protected override void Open()
        {
            bool createTables = false;

            if (!File.Exists((string)Location))
            {
                Create();
                createTables = true;
            }

            string connString = String.Format("Data Source={0};Journal Mode=Off;Synchronous=Off;", Location);

            conn = new SQLiteConnection(connString);
            conn.Open();

            if (createTables)
                CreateTables();

            cmd = new SQLiteCommand();
            cmd.Connection = conn;
            cmd.CommandText = "INSERT INTO Reduce(key, value) VALUES (@key,@value)";

            paramKey = cmd.Parameters.Add("@key", System.Data.DbType.AnsiString);
            paramValue = cmd.Parameters.Add("@value", System.Data.DbType.AnsiString);

            tran = conn.BeginTransaction();
        }

        protected virtual void CreateTables()
        {
            SQLiteCommand createTable = new SQLiteCommand("CREATE TABLE Reduce (Key TEXT, Value TEXT)", conn);
            SQLiteCommand createIndex = new SQLiteCommand("CREATE INDEX ix_Key ON Reduce(Key ASC)", conn);

            createTable.ExecuteNonQuery();
            createIndex.ExecuteNonQuery();
        }

        private void Create()
        {
            SQLiteConnection.CreateFile((string)Location);
        }

        protected override void Close()
        {
            if (tran != null)
            {
                tran.Commit();
            }

            conn.Close();
        }
    }
}
