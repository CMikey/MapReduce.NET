using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using MapReduce.NET;
using System.Runtime.Serialization;

namespace MapReduce.NET.Service
{
    [DataContract]
    public enum StatusType
    {
        Error, Started, RunningMapReduce, RunningSave, Stopped
    }

    [DataContract]
    public class StatusMessage
    {
        [DataMember]
        public Guid ResultSetId { get; set; }

        [DataMember]
        public string Message { get; set; }

        [DataMember]
        public StatusType Type { get; set; }

        [DataMember]
        public uint ProcessedItems { get; set; }

        [DataMember]
        public double ElapsedSeconds { get; set; }

        [DataMember]
        public uint ItemsPerSeconds { get; set; }

        public List<string> OutputFiles { get; private set; }

        [DataMember]
        public DateTime Created { get; set; }

        public StatusMessage()
        {
            OutputFiles = new List<string>();
            Created = DateTime.Now;
            Type = StatusType.Stopped;
        }
    }
}