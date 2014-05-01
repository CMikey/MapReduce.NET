using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections;
using MapReduce.NET.Output;

namespace MapReduce.NET.Input
{
    public abstract class InputPlugin<T> : IOPlugin
    {
        public object Position { get; set; }
        
        private uint counter = 0;

        public InputPlugin(object inputLocation)
        {
            this.Location = inputLocation;
        }

       

        internal void Close()
        {
            RaiseStatusUpdate(UpdateType.Input, counter);

            CloseInput();
        }

        public IEnumerable<T> Read()
        {
            T data;
            object index;
            counter = 0;

            while (ReadItem(out data, out index))
            {
                Position = index;

                if (++counter % ReportEveryNth == 0)
                    RaiseStatusUpdate( UpdateType.Input, counter);

                yield return data;
            }
        }

        protected internal abstract void Open();

        protected abstract void CloseInput();

        protected abstract bool ReadItem(out T data, out object index);
    }
}
