using MapReduce.NET.Collections;

namespace MapReduce.NET
{
    public abstract class Mapper<TKey, TValue, TReducekey, TReducevalue> : MapReduceBase
    {
        private MapReduceContext _context = new MapReduceContext();

        public MapReduceContext Context
        {
            get { return _context; }
            set { _context = value; }
        }

        public abstract void Map(TKey key, TValue value, IQueue<TReducekey, TReducevalue> result);
    }
}