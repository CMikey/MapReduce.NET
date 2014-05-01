using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using MapReduce.NET.Service;
using System.IO;
using System.IO.Compression;

namespace MapReduce.NET.Service
{
    public class ServiceTest
    {
        public static void Test()
        {
            var client = new MapReduceServiceClient<string,List<string>>();
            client.Endpoint.Address = new System.ServiceModel.EndpointAddress("http://localhost:1060/MapReduceService.svc");

            Dictionary<string, string> dict = new Dictionary<string, string>();
            dict.Add("search", "hamlet");

            var a = client.Start("configGrep", dict);

            var status = client.Status();

            client.WaitToFinish();

            var gd = client.GetMemoryResultDictionary(false);
        }
    }
}
