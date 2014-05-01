//using System;
//using System.Collections.Generic;
//using System.Linq;
//using System.Text;
//using System.Diagnostics;

//namespace MapReduce.NET.Monitor
//{
//    public class Performance
//    {
//        public long CPU { get; set; }
//        //public int TotalMemory { get; set; }
//        public long TotalNetwork { get; set; }
//        public long TotalCommitedMemory { get; set; }

//        static PerformanceCounter pcNetwork = new PerformanceCounter("Network Interface", "Bytes Total/sec");
//        //static PerformanceCounter pcMemory = new PerformanceCounter(".NET CLR Memory", "# Total committed Bytes", Process.GetCurrentProcess().ProcessName, true);
//        static PerformanceCounter pcProcessor = new PerformanceCounter("Processor", "% Processor Time", "_Total");

//        public static Performance Read()
//        {
//            return new Performance
//            {
//                CPU = pcProcessor.RawValue,
//                TotalNetwork = pcNetwork.RawValue,
//                //TotalCommitedMemory = pcMemory.RawValue
//            };
//        }
//    }
//}
