
namespace MapReduce.NET
{
    public enum UpdateType { None, Map, Reduce, Input, Output };

    internal delegate void StatusDelegate(UpdateType type, IUpdateSource source, uint processedItems);

    public delegate void ProgressDetails(UpdateType type, uint processedItems, double elapsedSeconds, uint itemsPerSecond);
}
