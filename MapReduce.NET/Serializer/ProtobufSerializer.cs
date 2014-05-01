using System.IO;

namespace MapReduce.NET.Serializer
{
    public class ProtobufSerializer : ISerializer
    {
        readonly MemoryStream _memoryStream = new MemoryStream();

        public object Serialize<T>(T item) 
        {
            ProtoBuf.Serializer.Serialize(_memoryStream, item);

            var retval = _memoryStream.ToArray();

            _memoryStream.SetLength(0);

            return retval;
        }

        public T Deserialize<T>(object source)
        {
            var buff = source as byte[];

            if (buff != null) _memoryStream.Write(buff, 0, buff.Length);

            var retval = ProtoBuf.Serializer.Deserialize<T>(_memoryStream);

            _memoryStream.Position = 0;

            _memoryStream.SetLength(0);

            return retval;
        }
    }
}
