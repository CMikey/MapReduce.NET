using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Activation;
using System.ServiceModel.Web;
using System.Text;
using System.Collections;
using System.Threading;
using MapReduce.NET;
using System.Web.Hosting;
using System.IO;
using System.Reflection;
using System.IO.Compression;
using MapReduce.NET.Serializer;
using MapReduce.NET.Service;

namespace MapReduce.Web
{
    [ServiceContract]
    public interface IMapReduceService
    {
        [OperationContract]
        [WebInvoke(Method = "GET")]
        StatusMessage Status();

        [OperationContract]
        [WebInvoke(Method = "POST")]
        StatusMessage Start(string config, IDictionary<string, string> parameters);

        [OperationContract]
        [WebInvoke(Method = "GET")]
        void AbortMapReduce();

        [OperationContract]
        [WebInvoke(Method = "GET")]
        void WaitToFinish();

        [OperationContract]
        [WebInvoke(Method = "GET")]
        Guid[] GetResultSetList();

        [OperationContract]
        [WebInvoke(Method = "GET")]
        string[] GetFileList(Guid resultSetId);

        [OperationContract]
        [WebInvoke(Method = "GET")]
        string GetMemoryResult(bool purgeData);

        [OperationContract]
        [WebInvoke(Method = "GET")]
        void RemoveResultSet(Guid resuletSetId);
        
        // GetFile() implemented in standard ASP.NET
        
    }

    [ServiceBehavior(InstanceContextMode = InstanceContextMode.Single)]
    public class MapReduceService : IMapReduceService   
    {
        Thread worker;
        StatusMessage status = new StatusMessage();
        MapReduceDriver driver;

        public MapReduceService()
        {
            string rootDir = HostingEnvironment.MapPath("/App_Data/");
            Environment.CurrentDirectory = rootDir;
        }

        public StatusMessage Status()
        {
            return status;
        }

        public StatusMessage Start(string config, IDictionary<string, string> parameters)
        {
            var statuslocal = new StatusMessage();

            if (string.IsNullOrEmpty(config))
            {
                statuslocal.Type = StatusType.Error;
                statuslocal.Message = "Invalid config name";
            }
            else if (worker != null)
            {
                statuslocal.Type = StatusType.Error;
                statuslocal.Message = "A MapReduce is already running";
            }
            else
            {
                driver = new MapReduceDriver(Path.Combine("configs",config));
                SetParameters(driver.Tasks, parameters);

                worker = new Thread(new ThreadStart(MapReduceThread));
                worker.Start();

                status.ResultSetId = statuslocal.ResultSetId = Guid.NewGuid();
            }

            return statuslocal;
        }

        private void SetParameters(IList<MapReduceTask> mrtasks, IDictionary<string, string> parameters)
        {
            if (parameters == null || parameters.Count == 0)
                return;

            foreach (var task in mrtasks)
            {
                foreach (var parameter in parameters)
                {
                    task.Parameters[parameter.Key] = parameter.Value;
                }
            }
        }

        public void AbortMapReduce()
        {
            worker.Abort();
            status.Type = StatusType.Stopped;
        }

        public void WaitToFinish()
        {
            worker.Join();
        }

        public Guid[] GetResultSetList()
        {
            throw new NotImplementedException();
        }

        public string[] GetFileList(Guid resultSetId)
        {
            throw new NotImplementedException();
        }

        public void RemoveResultSet(Guid resuletSetId)
        {
            throw new NotImplementedException();
        }

        public string GetMemoryResult(bool purgeData)
        {
            MemoryStream ms = new MemoryStream();
            GZipStream gz = new GZipStream(ms, CompressionMode.Compress, true);
            BufferedStream bs = new BufferedStream(gz, 64 * 1024); // 64KByte buffer

            ProtoBuf.Serializer.Serialize(bs, driver.Tasks.Last().ReduceResult);
            
            bs.Flush();
            gz.Close();

            return Convert.ToBase64String(ms.ToArray());
        }

        private void MapReduceThread()
        {
            driver.Progress += new ProgressDetails(RefreshStatus);
            driver.Start();

            foreach (var task in driver.Tasks)
            {
                status.OutputFiles.Add(task.Output.Location);
            }

            status.Type = StatusType.Stopped;
        }

        void RefreshStatus(UpdateType type, uint processedItems, double elapsedSeconds, uint itemsPerSecond)
        {
            status.Type = MapType(type);
            status.ProcessedItems = processedItems;
            status.ElapsedSeconds = elapsedSeconds; 
            status.ItemsPerSeconds = itemsPerSecond;
            status.Created = DateTime.Now;
        }

        private StatusType MapType(UpdateType type)
        {
            switch (type)
            {
                case UpdateType.None:
                    return StatusType.Stopped;
                case UpdateType.Map:
                    return StatusType.RunningMapReduce;
                case UpdateType.Reduce:
                    return StatusType.RunningMapReduce;
                case UpdateType.Input:
                    return StatusType.RunningMapReduce;
                case UpdateType.Output:
                    return StatusType.RunningSave;
                default:
                    return StatusType.Stopped;
            }
        }
    }
}
