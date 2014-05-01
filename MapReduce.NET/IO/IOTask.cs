using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET.Output;

namespace MapReduce.NET
{
    public class IOTask
    {
        private IOPlugin plugin;

        public string Location { get; set; }
        public string PluginType { get; set; }

        private IDictionary<string, string> parameters;

        public IDictionary<string, string> Parameters
        {
            get
            {
                return parameters;
            }
            set
            {
                parameters = value;
            }
        }

        public IOPlugin GetPlugin()
        {
            Type type = TypeFinder.FindType(PluginType);
            
            if (type == null)
                return null;

            plugin = Activator.CreateInstance(type, Location) as IOPlugin;

            TypeFinder.MapDictionary(plugin, parameters);

            return plugin;
        }
    }
}
