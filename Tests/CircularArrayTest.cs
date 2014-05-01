using MapReduce.NET.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Threading;

namespace Tests
{
    [TestClass()]
    public class CircularArrayTest
    {
        private TestContext testContextInstance;

        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        public void CircularArrayConstructorTestHelper<K, V>()
        {
            CircularArray<K, V> target = new CircularArray<K, V>();
            Assert.IsFalse(target.HasNext);
            Assert.IsTrue(target.MapInProgress);
        }

        [TestMethod()]
        public void CircularArrayConstructorTest()
        {
            CircularArrayConstructorTestHelper<GenericParameterHelper, GenericParameterHelper>();
        }
        
        public void PopTestHelper<K, V>(K key, V value)
        {
            CircularArray<K, V> target = new CircularArray<K, V>();
            target.Push(key, value);

            Assert.IsTrue(target.HasNext);
            Assert.IsTrue( target.Pop(out key, out value));

            Assert.IsFalse(target.HasNext);
            Assert.IsFalse(target.Pop(out key, out value));
        }

        [TestMethod()]
        public void PopTest()
        {
            PopTestHelper<string, string>("L","L");
        }

        public void DelayedWriteCountTestHelper(object o)
        {
            CircularArray<string, string> target = (CircularArray<string, string>)o;

            for (int i = 0; i < target.Length + 1; i++)
            {
                target.Push("L", "L");
            }
        }

        [TestMethod()]
        public void DelayedWriteCountTest()
        {
            CircularArray<string, string> target = new CircularArray<string, string>();
            new Thread(new ParameterizedThreadStart(DelayedWriteCountTestHelper)).Start(target);
            Thread.Sleep(300);
            Assert.AreNotEqual(0, target.DelayedWriteCount);
        }
    }
}
