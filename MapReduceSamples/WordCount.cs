using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET;
using System.Text.RegularExpressions;
using MapReduce.NET.Collections;

namespace MapReduceSamples
{
    public class WordCountMap : Mapper<string, string, string, int>
    {
        string[] split = new string[100];

        public override void Map(string key, string value, IQueue<string, int> result)
        {
            // key is null, coming from txt file

            unsafe
            {
                fixed (char* val = value)
                {
                    for (int i = 0; i < value.Length; i++)
                    {
                        // be careful with unicode input - a character with value of 60000+ is still a valid character!
                        if (val[i] < 'A' || (val[i] < 'a' && val[i] > 'Z') || val[i] > 1000)
                            val[i] = ' ';
                    }
                }
            }

            string[] cleanarr = value.ToLower().Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);

            foreach (var item in cleanarr)
            {
                result.Push(item, 1);
            }
        }
    }

    public class WordCountReduce : Reducer<string, int, int>
    {
        public WordCountReduce()
        {
            //this.Comparer = new FastStringComparer();
        }

        public override int Reduce(string key, int value, int result)
        {
            return result + value;
        }
    }

    public class FastStringComparer : IEqualityComparer<string>
    {
        public bool Equals(string x, string y)
        {
            return x.Equals(y);
        }

        public int GetHashCode(string obj)
        {
            if (string.IsNullOrEmpty(obj))
                return 0;

            int retval = 0;

            for (int i = 0; i < obj.Length; i++)
            {
                retval ^= obj[i] % 16;
                retval <<= 4;
                retval ^= obj[i] / 16;
            }

            return retval;
        }
    }
}
