using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using MapReduce.NET;
using MapReduce.NET.Collections;


namespace MapReduceSamples
{
    public class GrepMapper : Mapper<string, string, string, string>
    {
        public bool IgnoreCase { get; set; }
        public string Search { get; set; }

        public override void Map(string nullkey, string line, IQueue<string, string> result)
        {
            if (
                (IgnoreCase && line.ToLower().IndexOf(Search) > -1) ||
                (!IgnoreCase && line.IndexOf(Search) > -1)
            )
                    result.Push((string)Context.Location, String.Format("{0} - {1}", (int)Context.Position, line));
        }
    }

    public class GrepReducer : Reducer<string, string, List<string>>
    {
        public override List<string> Reduce(string key, string value, List<string> result)
        {
            if (result == null)
                result = new List<string>();

            result.Add(value);

            return result;
        }
    }
}
