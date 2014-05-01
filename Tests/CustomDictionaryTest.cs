using MapReduce.NET.Collections;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Collections;

namespace Tests
{
    [TestClass()]
    public class CustomDictionaryTest
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

        public void CustomDictionaryConstructorTestHelper<K, V>()
        {
            CustomDictionary<K, V> target = new CustomDictionary<K, V>();
            Assert.AreEqual(0, target.Count);
        }

        [TestMethod()]
        public void CustomDictionaryConstructorTest()
        {
            CustomDictionaryConstructorTestHelper<GenericParameterHelper, GenericParameterHelper>();
        }


        public void AddTest1Helper<K, V>(K key,V value, int expected, CustomDictionary<K,V> target)
        {
            bool overwrite = false; 
            int actual;
            actual = target.Add(key, value, overwrite);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod()]
        public void AddTest1()
        {
            CustomDictionary<string, string> target = new CustomDictionary<string, string>(); 

            AddTest1Helper("L","",0, target);
            AddTest1Helper("L2", "", 1, target);
        }

        public void ClearTestHelper<K, V>()
        {
            CustomDictionary<K, V> target = new CustomDictionary<K, V>(); 
            target.Clear();
            Assert.AreEqual(0, target.Count);
        }

        [TestMethod()]
        public void ClearTest()
        {
            ClearTestHelper<GenericParameterHelper, GenericParameterHelper>();
        }

        public void ContainsTestHelper<K, V>()
        {
            CustomDictionary<K, V> target = new CustomDictionary<K, V>(); 
            KeyValuePair<K, V> item = new KeyValuePair<K, V>(); 
            bool expected = false; 
            bool actual;
            actual = target.Contains(item);
            Assert.AreEqual(expected, actual);

            target.Add(item);
            expected = true;
            actual = target.Contains(item);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod()]
        public void ContainsTest()
        {
            ContainsTestHelper<int, int>();
        }


        public void GetTestHelper<K, V>()
        {
            CustomDictionary<K, V> target = new CustomDictionary<K, V>(); 
            K key = default(K); 
            V expected = default(V);
            target.Add(key, expected, false);
            V actual;
            actual = target.Get(key);
            Assert.AreEqual(expected, actual);
        }

        [TestMethod()]
        public void GetTest()
        {
            GetTestHelper<int, int>();
        }

        public void GetAtPositionTestHelper()
        {
            CustomDictionary<string, int> target = new CustomDictionary<string, int>();

            int expceted = 10;
            int actual;

            for (int i = 0; i < 1000; i++)
            {
                target.Add(i.ToString(), i, false);
            }

            int pos = target.Add("L", expceted, false);

            actual = target.GetAtPosition(pos);
            Assert.AreEqual(expceted, actual);
        }

        [TestMethod()]
        public void GetAtPositionTest()
        {
            GetAtPositionTestHelper();
        }

        public void GetPositionTestHelper()
        {
            CustomDictionary<string, int> target = new CustomDictionary<string, int>();

            int expceted = 10;
            int actual;

            for (int i = 0; i < 1000; i++)
            {
                target.Add(i.ToString(), i, false);
            }

            int pos = target.Add("L", expceted, false);

            pos = target.GetPosition("L");

            actual = target.GetAtPosition(pos);
            Assert.AreEqual(expceted, actual);
        }

        [TestMethod()]
        public void GetPositionTest()
        {
            GetPositionTestHelper();
        }

       
        public void InitOrGetPositionTestHelper()
        {
            CustomDictionary<string, int> target = new CustomDictionary<string, int>();

            for (int i = 0; i < 1000; i++)
            {
                target.Add(i.ToString(), i, false);
            }

            int pos = target.InitOrGetPosition("10");
            Assert.AreEqual(10, pos);

            Assert.AreEqual(1000, target.Count);

            pos = target.InitOrGetPosition("L");
            Assert.AreEqual(1000, pos);

            Assert.AreEqual(1001, target.Count);
        }

        [TestMethod()]
        public void InitOrGetPositionTest()
        {
            InitOrGetPositionTestHelper();
        }

        public void StoreAtLocationTestHelper()
        {
            CustomDictionary<string, int> target = new CustomDictionary<string, int>();

            for (int i = 0; i < 1000; i++)
            {
                target.Add(i.ToString(), i, false);
            }

            target.StoreAtPosition(10, 999);

            Assert.AreEqual(999, target["10"]);
        }

        [TestMethod()]
        public void StoreAtLocationTest()
        {
            StoreAtLocationTestHelper();
        }

        public void TryGetValueTestHelper()
        {
            CustomDictionary<string, int> target = new CustomDictionary<string, int>();

            for (int i = 0; i < 1000; i++)
            {
                target.Add(i.ToString(), i, false);
            }

            int val;
            bool exists = target.TryGetValue("10", out val);

            Assert.IsTrue(exists);
            Assert.AreEqual(10, val);

            exists = target.TryGetValue("1a0", out val);

            Assert.IsFalse(exists);
        }

        [TestMethod()]
        public void TryGetValueTest()
        {
            TryGetValueTestHelper();
        }
    }
}
