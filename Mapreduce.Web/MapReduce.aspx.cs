using System;
using System.IO;
using System.IO.Compression;
using System.Threading;
using System.Web.Hosting;
using System.Web.UI;
using MapReduce.NET;
using MapReduce.NET.Service;
using Newtonsoft.Json;

namespace MapReduce.Web
{
    public partial class MapReduce : Page
    {
        private const string CommandKey = "Command";
        private const string ConfigNameKey = "ConfigName";
        private const string WorkerThreadKey = "WorkerThread";
        private const string StatusKey = "Status";
        private const int SleepTime = 1*1000; // 30 sec
        private StatusMessage status;
        private Thread worker;

        protected void Page_Load(object sender, EventArgs e)
        {
            worker = Session[WorkerThreadKey] as Thread;
            status = Session[StatusKey] as StatusMessage;


            string rootDir = HostingEnvironment.MapPath("/App_Data/");
            Environment.CurrentDirectory = rootDir;

            string cmd = Request[CommandKey];
            string configName = Request[ConfigNameKey];

            if (string.IsNullOrEmpty(cmd))
                return;

            cmd = cmd.ToLower();

            if (cmd == "status")
            {
                if (status == null)
                    RefreshStatus(UpdateType.None, 0, 0, 0);

                Status();
                return;
            }

            if (cmd == "start")
            {
                Start(configName);
                return;
            }

            if (cmd == "getresult")
            {
                GetResult(configName);
                return;
            }

            JsonMessage("No command");
        }

        private void GetResult(string configFile, int taskNumber = 0)
        {
            int sleptSoFar = 0;

            while (worker != null && worker.IsAlive)
            {
                Thread.Sleep(sleptSoFar += 500);

                if (sleptSoFar > SleepTime)
                    Response.Redirect(Request.RawUrl);
            }

            var driver = new MapReduceDriver(configFile);
            var fi = new FileInfo(driver.Tasks[0].Output.Location);

            if (worker == null && fi.LastWriteTime.AddSeconds(5) < DateTime.Now)
            {
                Start(configFile);
                Response.Redirect(Request.RawUrl);
            }

            SendFile(fi);
        }

        private void SendFile(FileInfo fi)
        {
            Response.Clear();
            Response.ContentType = "application/binary";
            Response.AppendHeader("content-disposition", String.Format("attachment; filename={0}", fi.Name));

            Response.Headers.Remove("Content-Encoding");
            Response.AppendHeader("Content-Encoding", "gzip");

            FileStream fs = fi.OpenRead();

            Response.Filter = new GZipStream(Response.Filter, CompressionMode.Compress);

            Response.WriteFile(fi.FullName);
            Response.Flush();
        }

        private void Start(string configFile)
        {
            if (string.IsNullOrEmpty(configFile))
            {
                JsonMessage("config file name not provided.");
                return;
            }

            if (worker != null)
                return;

            worker = new Thread(MapReduceThread);
            var dr = new MapReduceDriver(configFile);
            worker.Start(dr);
            Session[WorkerThreadKey] = worker;

            JsonMessage("Started");
        }

        private void Status()
        {
            Response.Write(JsonConvert.SerializeObject(status));
        }

        private void MapReduceThread(object driver)
        {
            var driverTyped = driver as MapReduceDriver;
            driverTyped.Progress += RefreshStatus;
            driverTyped.Start();

            foreach (var task in driverTyped.Tasks)
            {
                status.OutputFiles.Add(task.Output.Location);
            }

            //status.Type = UpdateType.None;
            //status.Created = DateTime.Now;
            //Session[StatusKey] = status;
        }

        private void RefreshStatus(UpdateType type, uint processedItems, double elapsedSeconds, uint itemsPerSecond)
        {
            status = new StatusMessage
            {
                Type = StatusType.Stopped,
                ProcessedItems = processedItems,
                ElapsedSeconds = elapsedSeconds,
                ItemsPerSeconds = itemsPerSecond,
                Created = DateTime.Now
            };
            Session[StatusKey] = status;
        }

        private void JsonMessage(string msg)
        {
            Response.Write(JsonConvert.SerializeObject(msg));
        }
    }
}