using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Reflection;

namespace MapReduce.NET
{
    public static class TypeFinder
    {
        public static Type FindType(string toFind)
        {
            if (toFind == null)
                return null;

            Type type = null;

            var assemblies = AppDomain.CurrentDomain.GetAssemblies();

            foreach (var assembly in assemblies)
            {
                type = assembly.GetType(toFind);

                if (type != null)
                    break;
            }

            if (type == null)
            {
                var assemblyname = toFind.Substring(0, toFind.IndexOf('.'));
                var asm = Assembly.Load(assemblyname);
                type = asm.GetType(toFind);
            }

            return type;
        }

        public static void MapDictionary(object mapTo, IDictionary<string, string> dict)
        {
            if (mapTo == null)
                return;

            if (dict == null)
                return;

            var t = mapTo.GetType();

            foreach (var key in dict.Keys)
            {
                var pi = t.GetProperty(key, BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.FlattenHierarchy | BindingFlags.Instance);

                if (pi == null)
                    continue;

                try
                {
                    var val = Convert.ChangeType(dict[key], pi.PropertyType);
                    pi.SetValue(mapTo, val, null);

                }
                catch (Exception)
                {
                }
            }
        }
    }
}
