using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;
using System.Text.Json.Serialization;
using Nest;

namespace LinkyCmd
{
    public class Frame
    {
        public DateTime TimeStamp { get; private set; } = DateTime.UtcNow;
        public int InstantaneousCurrent  { get; private set; }
        public int ApparentPower { get; private set; }
        public int Index { get; private set; }

        [Ignore, JsonIgnore]
        public Dictionary<string, string> Values { get; private set; } = new Dictionary<string, string>();

        public Frame(MemoryStream stream)
        {
            StreamReader reader = new StreamReader(stream, Encoding.ASCII);
            while (!reader.EndOfStream)
            {
                string nextLine = reader.ReadLine();
                string[] elements = nextLine.Split(' ');

                if (elements.Length > 1 && !Values.ContainsKey(elements[0]))
                    Values.Add(elements[0], elements[1]);
            }

            InstantaneousCurrent = -1;
            ApparentPower = -1;
            Index = -1;

            string strValue = null;
            int intValue = -1;

            if (Values.TryGetValue("IINST", out strValue))
                if (int.TryParse(strValue, out intValue))
                    InstantaneousCurrent = intValue;
            
            if (Values.TryGetValue("PAPP", out strValue))
                if (int.TryParse(strValue, out intValue))
                    ApparentPower = intValue;

            if (Values.TryGetValue("BASE", out strValue))
                if (int.TryParse(strValue, out intValue))
                    Index = intValue;
        }
        private PropertyInfo[] _PropertyInfos = null;

        public override string ToString()
        {
            if(_PropertyInfos == null)
                _PropertyInfos = this.GetType().GetProperties();

            var sb = new StringBuilder();

            foreach (var info in _PropertyInfos)
            {
                var value = info.GetValue(this, null) ?? "(null)";
                sb.AppendLine(info.Name + ": " + value.ToString());
            }

            return sb.ToString();
        }    
    } 
}