using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MapReduce.NET;
using ProtoBuf;
using MapReduce.NET.Collections;

namespace MapReduceSamples
{
    public class ProductRecommendationMapper : Mapper<string, string, string, Visit>
    {
        public override void Map(string key, string value, IQueue<string, Visit> result)
        {
            try
            {
                //key is null, file input

                string[] line = value.Split(',');

                string sessionid = line[5];

                if (line[3] != "3") // visiting a product page
                    return;

                string productidstring = line[4];

                int productid = Int32.Parse(productidstring);

                DateTime dt = DateTime.Parse(line[2]);

                result.Push(sessionid, new Visit { ProductId = productid, Date = dt });
            }
            catch (Exception)
            {
            }
        }
    }

    public class ProductRecommendationReducer : Reducer<string, Visit, List<Visit>>
    {
        public override List<Visit> Reduce(string key, Visit value, List<Visit> result)
        {
            if (result == null)
                result = new List<Visit>();

            if (!result.Exists(v=>v.ProductId == value.ProductId))
                result.Add(value);

            return result;
        }
    }

    public class ProductRecommendationMapperStep2 : Mapper<string, List<Visit>, int, ProductTransition>
    {

        public override void Map(string key, List<Visit> value, IQueue<int, ProductTransition> result)
        {
            if (value.Count > 20) // too many visits, not relevant
                return;

            for (int i = 0; i < value.Count; i++)
            {
                for (int j = i + 1; j < value.Count; j++)
                {
                    float transitionValue = (float)(1 / Math.Pow(1.01, DateTime.Now.Subtract(value[i].Date).Days));

                    if (transitionValue == 0.0)
                        continue;

                    result.Push(value[i].ProductId, new ProductTransition{ To = value[j].ProductId, Value = transitionValue} );
                    result.Push(value[j].ProductId, new ProductTransition { To = value[i].ProductId, Value = transitionValue });
                }
            }
        }
    }

    public class ProductRecommendationReducerStep2 : Reducer<int, ProductTransition, Dictionary<int,float>>
    {
        public override Dictionary<int, float> Reduce(int key, ProductTransition value, Dictionary<int, float> result)
        {
            if (result == null)
                result = new Dictionary<int, float>();

            if (!result.ContainsKey(value.To))
            {
                result.Add(value.To, value.Value);

            }
            else
            {
                result[value.To] += value.Value;
            }



            return result;
        }

        public override void BeforeSave(IDictionary<int, Dictionary<int, float>> dict)
        {
            foreach (var productRecommendation in dict)
            {
                foreach (var item in productRecommendation.Value.Where(p => p.Value < 0.01).ToList())
                {
                    productRecommendation.Value.Remove(item.Key);
                }
            }
        }
    }

    public struct Visit
    {
        public DateTime Date;
        public int ProductId;
    }

    [ProtoContract]
    public struct ProductTransition : IComparable, IComparable<ProductTransition>, IEquatable<ProductTransition>
    {
        [ProtoMember(1)]
        public int To { get; set; }

        [ProtoMember(2)]
        public float Value { get; set; }

        public override int GetHashCode()
        {
            return To;
        }

        public override bool Equals(object obj)
        {
            var other = (ProductTransition)obj;

            return Equals(other);
        }

        public override string ToString()
        {
            return To + " ( " + Value + " ) ";
        }

        public int CompareTo(object obj)
        {
            ProductTransition other = (ProductTransition)obj;

            return CompareTo(other);
        }

        public int CompareTo(ProductTransition other)
        {
            return To.CompareTo(other.To);
        }

        public bool Equals(ProductTransition other)
        {
            return this.To == other.To;
        }
    }
}
