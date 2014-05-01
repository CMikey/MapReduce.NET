using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET.Output;
using MapReduce.NET.Serializer;
using System.Collections;

namespace MapReduce.NET.Output
{
    public class ConsoleOutput : OutputPlugin
    {
        public ConsoleOutput(string location) : base(null, new JsonSerializer())
        {}

        protected override void SaveItem<K, V>(K key, V value)
        {
            Console.Write(key);

            if (value is IDictionary)
            {
                Console.WriteLine();

                foreach (object k in ((IDictionary)value).Keys)
                {
                    Console.WriteLine("{0} - {1}", k, ((IDictionary)value)[k]);
                }
            }
            else if (value is ICollection)
            {
                Console.WriteLine();

                foreach (object k in ((ICollection)value))
                {
                    Console.WriteLine("{0}", k);
                }
            }
            else
            {
                Console.WriteLine(" - {0}", value);
            }
        }

        protected override void Open()
        {
            Console.WriteLine("-- BEGIN --");
        }

        protected override void Close()
        {
            Console.WriteLine("-- END --");
        }
    }
}
