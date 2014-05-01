using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using System.Reflection;
using MapReduce.NET.Serializer;

namespace MapReduce.NET.Output
{
    public abstract class OutputPlugin : IOPlugin
    {
        protected ISerializer serializer;

        public OutputPlugin(string outputLocation, ISerializer serializer)
        {
            this.Location = outputLocation;
            this.serializer = serializer;
        }

        public void Save(IDictionary output)
        {
            var args = output.GetType().GetGenericArguments();
            Type key = args[0];
            Type value = args[1];

            MethodInfo mi = GetType().GetMethod("SaveGeneric");
            MethodInfo miSaveGeneric = mi.MakeGenericMethod(key, value);
            miSaveGeneric.Invoke(this, new object[] { output });
        }

        public void SaveGeneric<K, V>(IDictionary<K, V> output)
        {
            uint counter = 0;

            Open();

            foreach (var item in output)
            {
                SaveItem(item.Key, item.Value);

                if (++counter % ReportEveryNth == 0)
                    RaiseStatusUpdate(UpdateType.Output, counter);
            }

            Close();

            RaiseStatusUpdate(UpdateType.Output, counter);
        }

        protected abstract void SaveItem<K,V>(K key, V value);

        protected abstract void Open();

        protected abstract void Close();
    }
}
