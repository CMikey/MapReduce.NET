using System;
using System.Collections.Generic;
using System.ServiceModel.Web;
using System.IO;
using System.IO.Compression;

namespace MapReduce.NET.Service
{

    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    [System.ServiceModel.ServiceContractAttribute(ConfigurationName = "IMapReduceService")]
    public interface IMapReduceService<TK,TV>
    {

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/Status", ReplyAction = "http://tempuri.org/IMapReduceService/StatusResponse")]
        StatusMessage Status();

        [WebInvoke(Method = "POST")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/Start", ReplyAction = "http://tempuri.org/IMapReduceService/StartResponse")]
        StatusMessage Start(string config, Dictionary<string, string> parameters);

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/Abort", ReplyAction = "http://tempuri.org/IMapReduceService/AbortResponse")]
        void AbortMapReduce();

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/WaitToFinish", ReplyAction = "http://tempuri.org/IMapReduceService/WaitToFinishResponse")]
        void WaitToFinish();

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/GetResultSetList", ReplyAction = "http://tempuri.org/IMapReduceService/GetResultSetListResponse")]
        Guid[] GetResultSetList();

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/GetFileList", ReplyAction = "http://tempuri.org/IMapReduceService/GetFileListResponse")]
        string[] GetFileList(Guid resultSetId);

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/RemoveResultSet", ReplyAction = "http://tempuri.org/IMapReduceService/RemoveResultSetResponse")]
        void RemoveResultSet(Guid resuletSetId);

        [WebInvoke(Method = "GET")]
        [System.ServiceModel.OperationContractAttribute(Action = "http://tempuri.org/IMapReduceService/GetMemoryResult", ReplyAction = "http://tempuri.org/IMapReduceService/GetMemoryResultResponse")]
        string GetMemoryResult(bool purgeData);
    }

    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    public interface IMapReduceServiceChannel<TK,TV> : IMapReduceService<TK,TV>, System.ServiceModel.IClientChannel
    {
    }

    [System.Diagnostics.DebuggerStepThroughAttribute()]
    [System.CodeDom.Compiler.GeneratedCodeAttribute("System.ServiceModel", "4.0.0.0")]
    public partial class MapReduceServiceClient<TK, TV> : System.ServiceModel.ClientBase<IMapReduceService<TK, TV>>, IMapReduceService<TK, TV>
    {

        public MapReduceServiceClient()
        {
        }

        public MapReduceServiceClient(string endpointConfigurationName) :
            base(endpointConfigurationName)
        {
        }

        public MapReduceServiceClient(string endpointConfigurationName, string remoteAddress) :
            base(endpointConfigurationName, remoteAddress)
        {
        }

        public MapReduceServiceClient(string endpointConfigurationName, System.ServiceModel.EndpointAddress remoteAddress) :
            base(endpointConfigurationName, remoteAddress)
        {
        }

        public MapReduceServiceClient(System.ServiceModel.Channels.Binding binding, System.ServiceModel.EndpointAddress remoteAddress) :
            base(binding, remoteAddress)
        {
        }

        public StatusMessage Status()
        {
            return base.Channel.Status();
        }

        public StatusMessage Start(string config, Dictionary<string, string> parameters)
        {
            return base.Channel.Start(config, parameters);
        }

        public void AbortMapReduce()
        {
            base.Channel.AbortMapReduce();
        }

        public void WaitToFinish()
        {
            base.Channel.WaitToFinish();
        }

        public Guid[] GetResultSetList()
        {
            return base.Channel.GetResultSetList();
        }

        public string[] GetFileList(Guid resultSetId)
        {
            return base.Channel.GetFileList(resultSetId);
        }

        public void RemoveResultSet(Guid resuletSetId)
        {
            base.Channel.RemoveResultSet(resuletSetId);
        }

        public string GetMemoryResult(bool purgeData)
        {
            return base.Channel.GetMemoryResult(purgeData);
        }

        public IDictionary<TK, TV> GetMemoryResultDictionary(bool purgeData)
        {
            var result = base.Channel.GetMemoryResult(purgeData);

            var resultByteArr = Convert.FromBase64String(result);

            var ms = new MemoryStream(resultByteArr);

            var gz = new GZipStream(ms, CompressionMode.Decompress);

            var dict = ProtoBuf.Serializer.Deserialize<IDictionary<TK,TV>>(gz);

            return dict;
        }

    }
}
