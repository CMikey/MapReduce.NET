using System;
using System.Collections.Generic;
using System.Reflection;
using System.Collections;
using System.Threading;
using MapReduce.NET.Input;
using System.Diagnostics;
using MapReduce.NET.Collections;

namespace MapReduce.NET
{
    public class MapReduceTask : IUpdateSource
    {
        private Stopwatch _stopwatch;
        private long _lastStatusUpdate;
        private uint _reportEveryNth = 100;
        internal event StatusDelegate StatusUpdate;

        public string MapName { get; set; }

        public string ReduceName { get; set; }

        public IOTask Input { get; set; }

        public IOTask Output { get; set; }

        public bool Parallel { get; set; }

        public IDictionary ReduceResult { get; set; }

        internal Thread MapThread { get; set; }
        internal Thread ReduceThread { get; set; }

        internal bool PartialSaveInProgress { get; set; }

        public IDictionary<string,string> Parameters { get; set; }
        
        public bool IsRunning
        {
            get
            {
                return MapThread != null && ReduceThread != null;
            }
        }
        
        public uint ReportEveryNth
        {
            get { return _reportEveryNth; }
            set
            {
                if (value <= 0)
                    return;

                _reportEveryNth = value;
            }
        }
        
        public void WaitForComplete()
        {
            // start a partialsave from here when the memory pressure is high

            if (MapThread != null)
                MapThread.Join();

            if (ReduceThread != null)
                ReduceThread.Join();

            MapThread = null;
            ReduceThread = null;
        }

        internal void FindTypesMap(out MethodInfo miMap, out object dictMap, out object mapper)
        {
            var tmap = TypeFinder.FindType(MapName);

            miMap = null;
            dictMap = null;
            mapper = null;

            if (tmap != null)
            {
                miMap = tmap.GetMethod("Map");
                var pisMap = miMap.GetParameters();
                var dictionaryKeyValue = pisMap[2].ParameterType.GetGenericArguments();

                mapper = Activator.CreateInstance(tmap);

                var dictTypeGeneric = typeof(CircularArray<,>);

                var dictType = dictTypeGeneric.MakeGenericType(dictionaryKeyValue[0], dictionaryKeyValue[1]);

                dictMap = Activator.CreateInstance(dictType);
            }
        }

        internal void FindTypesReduce(out MethodInfo miRed, out IDictionary dictRed, out object reducer)
        {
            var tred = TypeFinder.FindType(ReduceName);

            miRed = null;
            dictRed = null;
            reducer = null;

            if (tred != null)
            {
                miRed = tred.GetMethod("Reduce");
                var pisRed = miRed.GetParameters();
                var dictionaryKeyValue = pisRed[2].ParameterType.GetGenericArguments();

                var dictTypeGeneric = typeof(CustomDictionary<,>);

                var dictType = dictTypeGeneric.MakeGenericType(pisRed[0].ParameterType, pisRed[2].ParameterType);

                reducer = Activator.CreateInstance(tred);

                dictRed = (IDictionary)Activator.CreateInstance(dictType);

                //PropertyInfo piComparer = reducer.GetType().GetProperty("Comparer");

                //object comparer = piComparer.GetValue(reducer, null);

                //if (comparer != null)
                //    dictRed = (IDictionary)Activator.CreateInstance(dictType, comparer);
                //else
                //    dictRed = (IDictionary)Activator.CreateInstance(dictType);
            }
        }

        internal IDictionary ExecuteMapReduce<TMk,TMv,TNk,TNv,TRnv>(
            IEnumerable input, Mapper<TMk,TMv,TNk,TNv> mapper, 
            Reducer<TNk, TNv, TRnv> reducer, 
            CircularArray<TNk,TNv> dictMap, 
            IDictionary<TNk,TRnv> dictRed, 
            bool executeMap, 
            bool executeReduce) where TMk : class
        {
            if(executeMap && mapper != null)
                Map(input, mapper, dictMap);

            if(executeReduce && reducer != null)
                Reduce(reducer, dictMap, dictRed);

            return dictRed as IDictionary;
        }

        private void Map<TMk, TMv, TNk, TNv>(
            IEnumerable input, 
            Mapper<TMk, TMv, TNk, TNv> mapper, 
            CircularArray<TNk, TNv> dictMap) where TMk : class
        {
            mapper.Parameters = this.Parameters;
            InputPlugin<TMv> inputPlugin = null;

            if (Input != null) // read from external source
            {
                inputPlugin = Input.GetPlugin() as InputPlugin<TMv>;
                if (inputPlugin != null)
                {
                    inputPlugin.Open();

                    input = inputPlugin.Read();

                    inputPlugin.StatusUpdate += RaiseStatusUpdate;
                }
            }

            if (input == null)
            {
                dictMap.MapInProgress = false;
                throw new ArgumentException("Empty data source");
            }

            var inputDict = input is IDictionary;

            uint counter = 0;

            foreach (var item in input)
            {
                while (PartialSaveInProgress) // after a short save we continue
                    Thread.Sleep(100);

                if (++counter % ReportEveryNth == 0)
                    RaiseStatusUpdate(UpdateType.Map, this, counter);

                TMk key;
                TMv val;

                if (!inputDict) // flat input, no keys
                {
                    key = null;
                    val = (TMv)item;
                }
                else
                {
                    var dictItem = (KeyValuePair<TMk, TMv>)item;
                    key = dictItem.Key;
                    val = dictItem.Value;
                }

                if (inputPlugin != null)
                {
                    mapper.Context.Position = inputPlugin.Position;
                    mapper.Context.Location = inputPlugin.Location;
                }

                mapper.Map(key, val, dictMap);
            }

            dictMap.MapInProgress = false;

            if (inputPlugin != null)
                inputPlugin.Close();
        }

        private void Reduce<TNk, TNv, TRnv>(
            Reducer<TNk, TNv, TRnv> reducer, 
            CircularArray<TNk, TNv> dictMap, 
            IDictionary<TNk, TRnv> dictRed)
        {
            reducer.Parameters = this.Parameters;

            while (dictMap.MapInProgress || dictMap.HasNext)
            {
                while (PartialSaveInProgress) // a short save and we continue
                    Thread.Sleep(100);

                TNk key;
                TNv val;
                if (!dictMap.Pop(out key, out val))
                {
                    Thread.Sleep(1);
                    continue;
                }

                var pos = ((CustomDictionary<TNk, TRnv>)dictRed).InitOrGetPosition(key);

                var reducedValue = ((CustomDictionary<TNk, TRnv>)dictRed).GetAtPosition(pos);

                var newReducedValue = reducer.Reduce(key, val, reducedValue);

                ((CustomDictionary<TNk, TRnv>)dictRed).StoreAtPosition(pos, newReducedValue);
            }

            reducer.BeforeSave(dictRed);
        }
        
        internal void SetStopWatch(Stopwatch sw)
        {
            this._stopwatch = sw;
        }

        internal void MergeDictionaries<TK,TV,TNv>(Reducer<TK,TV,TNv> reducer, IDictionary from)
        {
            if (from.GetType() != ReduceResult.GetType())
                return;

            var fromtyped = ((IDictionary<TK, TNv>)from);

            foreach (var kv in fromtyped)
            {
                if (kv.Value is IEnumerable)
                {
                    foreach (var subitem in kv.Value as IEnumerable)
                    {
                        var pos = ((CustomDictionary<TK, TNv>)ReduceResult).InitOrGetPosition((TK)kv.Key);
                        var value = ((CustomDictionary<TK, TNv>)ReduceResult).GetAtPosition(pos);
                        var newvalue = reducer.Reduce((TK)kv.Key, (TV)subitem, value);
                        ((CustomDictionary<TK, TNv>)ReduceResult).StoreAtPosition(pos, newvalue);
                    }
                }
                else
                {
                    var pos = ((CustomDictionary<TK, TNv>)ReduceResult).InitOrGetPosition((TK)kv.Key);
                    var value = ((CustomDictionary<TK, TNv>)ReduceResult).GetAtPosition(pos);
                    var newvalue = reducer.Reduce(kv.Key, (TV)(object)kv.Value, value);
                    ((CustomDictionary<TK, TNv>)ReduceResult).StoreAtPosition(pos, newvalue);
                }
            }

            from.Clear();
        }

        void RaiseStatusUpdate(UpdateType type, IUpdateSource source, uint processedItems)
        {
            if (_stopwatch == null)
                return;

            // if the update is too frequent, slow it down to around 1/sec
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

            if (StatusUpdate != null)
                StatusUpdate(type, source, processedItems);
        }
    }
}