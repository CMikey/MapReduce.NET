using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Threading;

namespace MapReduce.NET.Collections
{
    internal class CircularArray<K,V> : IQueue<K,V>
    {
        const uint buffSize = 10 * 1000;
        K[] buffKey = new K[buffSize];
        V[] buffValue = new V[buffSize];
        volatile uint ptrWrite;
        volatile uint ptrRead;

        public uint Length { get { return buffSize; } }

        internal int DelayedWriteCount { get; set; }
 
        private bool mapInProgress = true;

        internal bool MapInProgress
        {
            get { return mapInProgress; }
            set { mapInProgress = value; }
        }

        internal bool HasNext
        {
            get
            {
                return ptrWrite > ptrRead;
            }
        }
        
        public void Push(K key, V value)
        {
            if (key == null)
                throw new ArgumentNullException("key");

            uint ptrWriteNext = ptrWrite + 1;

            while (ptrWriteNext == ptrRead + buffSize)
            {
                Thread.Sleep(0);
                //Console.WriteLine("Mapper was too fast");
                DelayedWriteCount++;
            }

            uint buffptr = ptrWrite % buffSize;

            buffKey[buffptr] = key;
            buffValue[buffptr] = value;

            ptrWrite = ptrWriteNext;
        }

        internal bool Pop(out K key, out V value)
        {
            uint ptrReadNext = ptrRead + 1;

            key = default(K);
            value = default(V);

            if (ptrReadNext > ptrWrite)
            {
                return false;
            }

            uint buffptr = ptrRead % buffSize;

            key = buffKey[buffptr];
            value = buffValue[buffptr];

            ptrRead = ptrReadNext;


            return true;
        }
    }
}
