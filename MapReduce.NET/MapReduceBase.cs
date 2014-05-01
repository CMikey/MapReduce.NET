using System.Collections.Generic;

namespace MapReduce.NET
{
    public abstract class MapReduceBase
    {
        private IDictionary<string, string> _parameters;

        internal IDictionary<string, string> Parameters
        {
            get
            {
                return _parameters;
            }
            set
            {
                _parameters = value;
                TypeFinder.MapDictionary(this, _parameters);
            }
        }
    }
}