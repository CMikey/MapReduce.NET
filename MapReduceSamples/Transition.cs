using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using ProtoBuf;

namespace MapReduceSamples
{
    [ProtoContract]
    public class Transition : IComparable
    {
        [ProtoMember(1)]
        public string From { get; set; }
        [ProtoMember(2)]
        public string To { get; set; }

        public override int GetHashCode()
        {
            return From.GetHashCode() + To.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            if (obj.GetType() != typeof(Transition))
                return false;

            var other = (Transition)obj;

            if (this.From != other.From)
                return false;

            if (this.To != other.To)
                return false;

            return true;
        }

        public override string ToString()
        {
            return From + " -> " + To;
        }

        public int CompareTo(object obj)
        {
            if (obj.GetType() != typeof(Transition))
                return -1;

            Transition other = obj as Transition;

            int c1 = From.CompareTo(other.From);
            int c2 = To.CompareTo(other.To);

            if (c1 == c2 && c2 == 0)
                return 0;

            if (c1 < 0 || c1 > 0)
                return c1;

            if (c2 < 0 || c2 > 0)
                return c2;

            return 0;
        }
    }
}
