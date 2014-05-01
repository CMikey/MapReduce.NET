using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET.Collections.System.IO;
using System.IO;

namespace MapReduce.NET.Input
{
    public class TextfileLineInput : InputPlugin<string>
    {
        StreamReaderAdvanced sr;
        int currentPosition;
        string[] files;
        int currentFileIndex;

        public DateTime? CreationDateAtLeast  { get; set; }
        public DateTime? CreationDateAtMost { get; set; }
        //public bool ArchiveAttributeOnly { get; set; }


        public TextfileLineInput(string path) : base(path) { }

        internal protected override void Open()
        {
            string dir = Path.GetDirectoryName(Location.ToString());
            string pattern = Path.GetFileName(Location.ToString());

            if (string.IsNullOrEmpty(dir))
                dir = ".";

            files = Directory.GetFiles(dir, pattern);
        }

        protected override void CloseInput()
        {
        }

        protected override bool ReadItem(out string data, out object index)
        {
            if (sr == null)
            {
                Location = files[currentFileIndex++];
                sr = new StreamReaderAdvanced(Location.ToString());
            }

            string line = sr.ReadLine();

            if (line == null)
            {
                if (currentFileIndex == files.Length)
                {
                    data = null;
                    index = -1;
                    return false;
                }

                index = 0;
                sr = null;
                return ReadItem(out data, out index);
            }

            index = currentPosition;

            currentPosition = (int)sr.Position;

            data = line;

            return true;
        }
    }
}
