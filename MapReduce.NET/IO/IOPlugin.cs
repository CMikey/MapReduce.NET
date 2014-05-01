using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET.Output;

namespace MapReduce.NET
{
    public class IOPlugin : IUpdateSource
    {
        internal event StatusDelegate StatusUpdate;

        public object Location { get; protected set; }

        private uint reportEveryNth = 100;

        public uint ReportEveryNth
        {
            get { return reportEveryNth; }
            set
            {
                if (value <= 0)
                    return;

                reportEveryNth = value;
            }
        }

        internal void RaiseStatusUpdate(UpdateType type, uint processedItems)
        {
            if (StatusUpdate == null)
                return;

            if(type == UpdateType.Output)
                StatusUpdate(UpdateType.Output, this, processedItems);

            if (type == UpdateType.Input)
                StatusUpdate(UpdateType.Input, this, processedItems);
        }

    }
}
