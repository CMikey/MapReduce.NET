using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET;
using MapReduce.NET.Collections;

namespace MapReduceSamples
{
    public class LogMapper : Mapper<string, string, string, string>
    {
        override public void Map(string key, string value, IQueue<string,string> result)
        {
            // key is always null as it is file sourced

            // "session product1"
            string[] sp = value.Split('\t');

            key = sp[0];
            value = sp[1];

            result.Push(key, value);
        }
    }

    public class Reducer1 : Reducer<string, string, List<string>>
    {
        public override List<string> Reduce(string key, string value, List<string> buff)
        {
            if(buff == null)
                buff = new List<string>();
                
            buff.Add(value);

            return buff;
        }
    }

    public class MapToTransition : Mapper<string, List<string>, Transition, int>
    {
        public override void Map(string key, List<string> value, IQueue<Transition, int> result)
        {
            var sorted = new SortedSet<string>(value);

            var uniqueValues = sorted.ToArray();

            for (int i = 0; i < uniqueValues.Length; i++)
            {
                for (int j = i + 1; j < uniqueValues.Length; j++)
                {
                    var tr1 = new Transition { From = value[i], To = value[j] };
                    var tr2 = new Transition { From = value[j], To = value[i] };

                    result.Push(tr1, 1);

                    result.Push(tr2, 1);
                }
            }
        }
    }

    public class Reducer2 : Reducer<Transition, int, int>
    {
        public override int Reduce(Transition key, int value, int buff)
        {
            buff += value;
            return buff;
        }
    }
}
