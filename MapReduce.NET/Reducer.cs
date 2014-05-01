using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace MapReduce.NET
{
    public abstract class Reducer<TReducekey, TReducevalue, TReducednewvalue> : MapReduceBase
    {
        public virtual void Merge(TReducednewvalue to, TReducednewvalue from){}

        public virtual void BeforeSave(IDictionary<TReducekey, TReducednewvalue> dict) { }

        public abstract TReducednewvalue Reduce(TReducekey key, TReducevalue value, TReducednewvalue result);
    }
}
