using System;

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Newtonsoft.Json;
using System.Reflection;
using System.Collections;
using System.Data.SQLite;
using System.Reflection.Emit;
using System.Linq.Expressions;
using System.Threading;
using MapReduce.NET.Output;
using System.Diagnostics;

namespace MapReduce.NET
{
    public class MapReduceDriver
    {
        public IList<MapReduceTask> Tasks { get; set; }

        public event ProgressDetails Progress;

        private object _reducer;
        private object _mapper;
        private MethodInfo _miMap;
        private MethodInfo _miRed;

        private Type _mapkey;
        private Type _mapvalue;
        private Type _reducekey;
        private Type _reducevalue;
        private Type _reducednewvalue;

        Stopwatch _stopwatch;
        long _lastStatusUpdate;
        long _outPutstartTime;
        private bool _completed;

        public MapReduceDriver(string configFileName)
        {
            if (string.IsNullOrEmpty(Path.GetExtension(configFileName)))
                configFileName += ".json";

            var config = File.ReadAllText(configFileName);

            Tasks = JsonConvert.DeserializeObject<List<MapReduceTask>>(config);
        }

        public void ExecuteStep(MapReduceTask task, IEnumerable input = null)
        {
            object dictMap;
            var miExecute = InitTaskExecute(task, out dictMap);

            task.MapThread = new Thread(() => 
                miExecute.Invoke(task, new[] { input, _mapper, _reducer, dictMap, task.ReduceResult, true, false }));

            task.ReduceThread = new Thread(() => 
                miExecute.Invoke(task, new[] { input, _mapper, _reducer, dictMap, task.ReduceResult, false, true }));
            
            task.MapThread.Start();
            task.ReduceThread.Start();
        }

        private MethodInfo InitTaskExecute(MapReduceTask task, out object dictMap)
        {
            IDictionary tmpdict;

            task.FindTypesMap(out _miMap, out dictMap, out _mapper);
            task.FindTypesReduce(out _miRed, out tmpdict, out _reducer);
            task.ReduceResult = tmpdict;

            MethodInfo miExecuteGeneric = task.GetType().GetMethod("ExecuteMapReduce", BindingFlags.NonPublic | BindingFlags.Instance);

            ParameterInfo[] mapParams = _mapper.GetType().GetMethod("Map").GetParameters();
            Type[] collectorParams = mapParams[2].ParameterType.GetGenericArguments();

            ParameterInfo[] reduceParams = _reducer.GetType().GetMethod("Reduce").GetParameters();


            _mapkey = mapParams[0].ParameterType;
            _mapvalue = mapParams[1].ParameterType;
            _reducekey = collectorParams[0];
            _reducevalue = collectorParams[1];
            _reducednewvalue = reduceParams[2].ParameterType;

            MethodInfo miExecute = miExecuteGeneric.MakeGenericMethod(new[] { _mapkey, _mapvalue, _reducekey, _reducevalue, _reducednewvalue });
            return miExecute;
        }

        private MethodInfo InitMergeDictionaries(MapReduceTask task)
        {
            MethodInfo miMergeGeneric = task.GetType().GetMethod("MergeDictionaries", BindingFlags.NonPublic | BindingFlags.Instance);

            MethodInfo miExecute = miMergeGeneric.MakeGenericMethod(new[] { _reducekey, _reducevalue, _reducednewvalue });
            return miExecute;
        }

        public void WaitForComplete()
        {
            foreach (var task in Tasks)
            {
                task.WaitForComplete();
            }

            _completed = true;
        }

        public IDictionary Start()
        {
            _stopwatch = Stopwatch.StartNew();

            _completed = false;

            ThreadPool.QueueUserWorkItem(WatchDog);

            //execute all tasks
            for (var i = 0; i < Tasks.Count; i++)
            {
                var task = Tasks[i];
                task.SetStopWatch(_stopwatch);
                task.StatusUpdate += outp_StatusUpdate;

                // process the result of previous MapReduce tasks
                if (task.Input == null)
                {
                    var inputTask = Tasks[i - 1];

                    for (var j = 0; j < i - 1; j++)
                    {
                        var mergeTask = Tasks[j];
                        MergeDictionaries(inputTask, mergeTask.ReduceResult);
                    }

                    ExecuteStep(task, inputTask.ReduceResult);
                }
                else
                {
                    ExecuteStep(task);
                }

                // if not parallel execution, wait for complete and merge with previous results, then save 
                if (!task.Parallel)
                {
                    task.WaitForComplete();

                    if (task.Output != null && task.Output.PluginType != null)
                    {
                        for (var j = 0; j < i; j++)
                        {
                            var prevTask = Tasks[j];
                            MergeDictionaries(task, prevTask.ReduceResult);
                        }

                        var outp = task.Output.GetPlugin() as OutputPlugin;

                        if (_stopwatch != null)
                            _outPutstartTime = _stopwatch.ElapsedMilliseconds;

                        if (outp != null)
                        {
                            outp.StatusUpdate += outp_StatusUpdate;

                            outp.Save(task.ReduceResult);
                        }

                        task.ReduceResult = null;  // clear memory after saving
                    }
                }
            }

            // wait for all pending tasks
            WaitForComplete();

            var lastTask = Tasks.Last();

            var mergeCount = 0;

            // if there were parallel tasks, merge all of them then save
            for (var j = 0; j < Tasks.Count - 1; j++)
            {
                var prevTask = Tasks[j];

                if (!prevTask.Parallel)
                    continue;

                mergeCount++;

                MergeDictionaries(lastTask, prevTask.ReduceResult);
            }

            if (mergeCount > 0 && lastTask.Output != null)
            {
                var outp = lastTask.Output.GetPlugin() as OutputPlugin;
                if (outp != null) outp.Save(lastTask.ReduceResult);
            }

            return lastTask.ReduceResult;
        }

        private void WatchDog(object o)
        {
            while (!_completed)
            {
                Thread.Sleep(5000);
                //PartialSave();
#warning Check memory limit and invoke save if running out of memory
            }
        }

        public void PartialSave()
        {
            foreach (var task in Tasks.Where(task => task != null && task.IsRunning && task.Output != null))
            {
                task.PartialSaveInProgress = true;

                var outp = task.Output.GetPlugin() as OutputPlugin;

                if (_stopwatch != null)
                    _outPutstartTime = _stopwatch.ElapsedMilliseconds;

                var status = new StatusDelegate(outp_StatusUpdate);

                if (outp != null)
                {
                    outp.StatusUpdate += status;

                    outp.Save(task.ReduceResult);

                    task.Output.Location = Guid.NewGuid() + ".db3";

                    outp.StatusUpdate -= status;
                }

                task.ReduceResult.Clear();  // clear memory after saving

                GC.Collect();

                task.PartialSaveInProgress = false;
            }
        }

        void outp_StatusUpdate(UpdateType type, IUpdateSource source, uint processedItems)
        {
            // if the update is too frequent, slow it down
            if (_lastStatusUpdate == 0)
            {
                _lastStatusUpdate = _stopwatch.ElapsedMilliseconds;
            }
            else if (_stopwatch.ElapsedMilliseconds - _lastStatusUpdate < 800)
            {
                source.ReportEveryNth *= 2;
            }
            else if (_stopwatch.ElapsedMilliseconds - _lastStatusUpdate > 1200)
            {
                source.ReportEveryNth = (uint)(source.ReportEveryNth / 1.5f);
            }

            _lastStatusUpdate = _stopwatch.ElapsedMilliseconds;

            var itemsPerSec = (uint)(processedItems / (_stopwatch.ElapsedMilliseconds / 1000f - _outPutstartTime / 1000f));

            var currentAction = "Processing";

            switch (type)
            {
                case UpdateType.Map:
                    break;
                case UpdateType.Reduce:
                    break;
                case UpdateType.Input:
                    return; // currently not interesed in Load events
                    //currentAction = "Loading";
                    //break;
                case UpdateType.Output:
                    currentAction = "Saving";
                    break;
            }


            Console.WriteLine(string.Format("[{1,2}.{2:000}] {4} item #{0:0,0} (avg: {3:0,0}/sec)", 
                processedItems,
                (int)_stopwatch.Elapsed.TotalSeconds, 
                _stopwatch.Elapsed.Milliseconds, 
                itemsPerSec,
                currentAction
                )
                );

            if (Progress != null)
                Progress(type, processedItems, _stopwatch.Elapsed.TotalSeconds, itemsPerSec);
        }

        private void MergeDictionaries(MapReduceTask task, IEnumerable from)
        {
            if (task == null || from == null)
                return;

            var miMerge = InitMergeDictionaries(task);

            miMerge.Invoke(task, new[] { _reducer, from });
        }
    }
}
