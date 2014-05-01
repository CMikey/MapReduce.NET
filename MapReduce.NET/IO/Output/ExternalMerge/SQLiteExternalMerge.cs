using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using System.IO;
using MapReduce.NET.Serializer;

namespace MapReduce.NET.Output.ExternalMerge
{
    public class SQLiteExternalMerge
    {
        protected class SourceItem<K,V> //: IComparable where K : IComparable
        {
            public K Key { get; set; }
            public V Value { get; set; }
            public int SourceIndex { get; set; }

            //public int CompareTo(object obj)
            //{
            //    return ((SourceItem<K, V>)obj).Key.CompareTo(Key);
            //}
        }

        public static void Merge<KEY, MAPVALUE, REDUCEDVALUE>(Reducer<KEY, MAPVALUE, REDUCEDVALUE> reducer, ISerializer serializer, string outputFile, params string[] inputs) where KEY : IComparable
        {
            string connTemplate = "Data Source={0};Journal Mode=Off;Synchronous=Off;";

            SQLiteConnection connOut = new SQLiteConnection(String.Format(connTemplate, outputFile));
            connOut.Open();

            SQLiteConnection[] connInputs = new SQLiteConnection[inputs.Length];
            SQLiteDataReader[] readerInputs = new SQLiteDataReader[inputs.Length];

            for (int i = 0; i < connInputs.Length; i++)
            {
                connInputs[i] = new SQLiteConnection(String.Format(connTemplate, inputs[i]));
                connInputs[i].Open();

                SQLiteCommand cmdInput = new SQLiteCommand();
                cmdInput.Connection = connInputs[i];
                cmdInput.CommandText = "SELECT Key,Value FROM Reduce ORDER BY Key";

                readerInputs[i] = cmdInput.ExecuteReader();
            }

            SQLiteCommand cmdInsert = new SQLiteCommand();
            cmdInsert.Connection = connOut;
            cmdInsert.CommandText = "INSERT INTO Reduce(key, value) VALUES (@key,@value)";

            SQLiteParameter paramKey = cmdInsert.Parameters.Add("@key", System.Data.DbType.AnsiString);
            SQLiteParameter paramValue = cmdInsert.Parameters.Add("@value", System.Data.DbType.AnsiString);

            SourceItem<KEY, REDUCEDVALUE>[] readValues = new SourceItem<KEY, REDUCEDVALUE>[inputs.Length];

            int liveReaders = readerInputs.Length;

            while (liveReaders > 0)
            {
                KEY minvalue = default(KEY);
                bool minvalueInited = false;

                for (int i = 0; i < readerInputs.Length; i++)
                {
                    if (readValues[i] != null || readerInputs[i].IsClosed)
                        continue;

                    var item = ReadNextFromReader<KEY, REDUCEDVALUE>(readerInputs, i, serializer);

                    readValues[i] = item;

                    if (item == null)
                    {
                        liveReaders--;
                        readerInputs[i].Close();
                        connInputs[i].Clone();
                        continue;
                    }

                    if (item.Key.CompareTo(minvalue) < 0 || !minvalueInited)
                    {
                        minvalueInited = true;
                        minvalue = item.Key;
                    }
                }

                SourceItem<KEY, REDUCEDVALUE> buff = null;

                for (int i = 0; i < readerInputs.Length; i++)
                {
                    if (readValues[i] == null)
                        continue;
                    
                    if (readValues[i].Key.CompareTo(minvalue) == 0)
                    {
                        if (buff == null)
                        {
                            buff = readValues[i];
                        }
                        else
                        {
                            reducer.Merge(buff.Value, readValues[i].Value);
                        }

                        readValues[i] = null;
                    }
                }

                if (buff != null)
                {
                    // write out buff
                    paramKey.Value = serializer.Serialize(buff.Key);
                    paramValue.Value = serializer.Serialize(buff.Value);

                    cmdInsert.ExecuteNonQuery();
                }
            }

            connOut.Clone();
        }

        private static SourceItem<KEY,REDUCEDVALUE> ReadNextFromReader<KEY, REDUCEDVALUE>(SQLiteDataReader[] readerIns, int i, ISerializer serializer) where KEY : IComparable
        {
            SQLiteDataReader reader = readerIns[i];

            if (reader.Read())
            {
                KEY k = serializer.Deserialize<KEY>(reader["Key"]);

                REDUCEDVALUE v = serializer.Deserialize<REDUCEDVALUE>(reader["Value"]);


                return new SourceItem<KEY, REDUCEDVALUE> { Key = k, Value = v, SourceIndex = i };
            }

            return null;
        }
    }
}
