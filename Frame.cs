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

                if (elements.Length > 2)
                {
                    string id = elements[0];
                    string value = elements[1];
                    char receivedChecksum = nextLine[nextLine.Length - 1]; // don't use elements[2] as the checksum character may be a space

                    // Compute the checksum. Contrary to what the documentation says,
                    // the last separator is not to be included in the checked data
                    byte s1 = 0;
                    string checkedData = nextLine.Substring(0, nextLine.Length - 2);
                    foreach (char c in checkedData)
                        s1 += (byte)c;
                    s1 &= 0x3F;
                    s1 += 0x20;

                    // Only add valid data. We may get duplicates when transmission errors occur.
                    if (s1 == receivedChecksum && !Values.ContainsKey(id))
                        Values.Add(id, value);
                }
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

        [Ignore, JsonIgnore]
        public bool IsValid { 
            get {
                return ApparentPower >= 0 && InstantaneousCurrent >= 0 && Index >= 0 && !IsEmpty;
            }   
        }

        [Ignore, JsonIgnore]
        public bool IsEmpty {
            get {
                return Values.Count == 0;
            }
        }
    } 
}