using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using MapReduce.NET;
using System.Collections;
using System.Reflection.Emit;
using System.Runtime.InteropServices;
using System.Linq.Expressions;
using System.Reflection;
using System.Diagnostics;
using MapReduce.NET.Output;
using MapReduce.NET.Output.ExternalMerge;
using MapReduce.NET.Serializer;
using MapReduce.NET.Collections.System.IO;
using MapReduce.NET.Service;

namespace TestProj
{
    class Program
    {
        static void Main(string[] args)
        {
            #region Remote MR test
            ServiceTest.Test(); 
            #endregion

            #region Simple product recommendation sample
            //MapReduceDriver prodrec = new MapReduceDriver("configProductRecommendSimple.json");

            //IDictionary sessprod = prodrec.Start(); 
            #endregion

            #region Simple GREP sample
            //MapReduceDriver grep = new MapReduceDriver("configGrep.json");

            //IDictionary grepresult = grep.Start(); 
            #endregion

            #region Word count to SQLite JSON
            //MapReduceDriver wordcount = new MapReduceDriver("configWordCountSQLiteJSONoutput.json");

            //IDictionary wordcountresult = wordcount.Start(); 
            #endregion

            #region Inverted Index - Shakespeare
            //MapReduceDriver dwordcountInv2 = new MapReduceDriver("configInvertedIndex.json");

            //dwordcountInv2.Tasks[0].Output.Location = Guid.NewGuid() + ".db3";

            //IDictionary resultInv2 = dwordcountInv2.Start(); 
            #endregion

            #region Large product recommendation - depends on external log
            //MapReduceDriver prodRecommDriver = new MapReduceDriver("configProductRecommendLargeLog.json");

            //prodRecommDriver.Start(); 
            #endregion

            #region Binary inverted index data - depends on external data
            //MapReduceDriver dwordcountInv2 = new MapReduceDriver("configInvertedIndexSQLiteBinaryOutput.json");

            //dwordcountInv2.Tasks[0].Output.Location = "gutenberg.db3";

            //IDictionary resultInv2 = dwordcountInv2.Start(); 
            #endregion

            #region External merger sample
            //SQLiteExternalMerge.Merge(new InvertedIndexReducer(), new ProtobufSerializer(), "mout.db3", "m1bin.db3", "m2bin.db3"); 
            #endregion
        }
    }
}
