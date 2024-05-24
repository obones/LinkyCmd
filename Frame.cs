/*
 Linky Cmd - The command line program to be used with LinkyPIC

 The contents of this file are subject to the Mozilla Public License Version 1.1 (the "License");
 you may not use this file except in compliance with the License. You may obtain a copy of the
 License at http://www.mozilla.org/MPL/

 Software distributed under the License is distributed on an "AS IS" basis, WITHOUT WARRANTY OF
 ANY KIND, either express or implied. See the License for the specific language governing rights
 and limitations under the License.

 The Original Code is Frame.cs.

 The Initial Developer of the Original Code is Olivier Sannier.
 Portions created by Olivier Sannier are Copyright (C) of Olivier Sannier. All rights reserved.
*/
using System;
using System.Collections;
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

        /// <summary>
        /// The meter index, with the following meaning depending on the contract:
        ///     BASE  -> The only index available
        ///     HC    -> Full hours (HCHP tag)
        /// </summary>
        public int Index { get; private set; }

        /// <summary>
        /// The available indexes, which may be just one
        /// The key is the value tag
        /// </summary>
        public Dictionary<string, int> Indexes { get; private set; } = new();

        /// <summary>
        /// The contract identifier, valid values include BASE, HC
        /// </summary>
        public string? Contract { get; private set; }
        
        /// <summary>
        /// The current tariff period, directly from the data frame itself.
        /// Known values include :
        ///    TH  -> static value for BASE contract
        ///    HC  -> 'shallow' hours in an HC contract
        ///    HP  -> 'full' hours in an HC contract
        /// </summary>
        public string? Period { get; private set; }

        [Ignore, JsonIgnore]
        public Dictionary<string, string> Values { get; private set; } = new Dictionary<string, string>();

        public Frame(MemoryStream stream)
        {
            StreamReader reader = new StreamReader(stream, Encoding.ASCII);
            while (!reader.EndOfStream)
            {
                string? nextLine = reader.ReadLine();
                string[] elements = nextLine!.Split(' ');

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

                    // If data is invalid, add a prefix to the key indicate it, this way we still have values and are not an empty frame
                    if (s1 != receivedChecksum) 
                        id = "!" + id;

                    // We may get duplicates when transmission errors occur.
                    if (!Values.ContainsKey(id))
                        Values.Add(id, value);
                }
            }

            InstantaneousCurrent = -1;
            ApparentPower = -1;
            Index = -1;

            InstantaneousCurrent = GetIntValue("IINST");
            
            ApparentPower = GetIntValue("PAPP");

            Contract = GetStrippedStringValue("OPTARIF");
            Period = GetStrippedStringValue("PTEC");

            StoreIndexValue("BASE", true);
            StoreIndexValue("HCHC", true);
            StoreIndexValue("HCHP", true);
        }

        private PropertyInfo[]? _PropertyInfos = null;

        private int GetIntValue(string tag)
        {
            if (Values.TryGetValue(tag, out string? strValue))
                if (int.TryParse(strValue, out int intValue))
                    return intValue;

            return -1;
        }

        private string? GetStrippedStringValue(string tag)
        {
            if (Values.TryGetValue(tag, out string? strValue))
                return strValue.TrimEnd('.');

            return null;
        }

        private void StoreIndexValue(string tag, bool asDefaultIndex)
        {
            var value = GetIntValue(tag);
            if (value >= 0)
            {
                Indexes.Add(tag, value);
                if (asDefaultIndex)
                    Index = value;
            }
        }

        public override string ToString()
        {
            if(_PropertyInfos == null)
                _PropertyInfos = this.GetType().GetProperties();

            var sb = new StringBuilder();

            foreach (var info in _PropertyInfos)
            {
                var value = info.GetValue(this, null) ?? "(null)";
                if (typeof(IDictionary).IsAssignableFrom(info.PropertyType))
                {
                    var dict = (IDictionary)value;
                    sb.Append(info.Name + ": [");
                    foreach (DictionaryEntry item in dict)
                        sb.Append("{ " + item.Key + " ; " + item.Value + " }");
                    sb.AppendLine("]");
                }
                else
                {
                    sb.AppendLine(info.Name + ": " + value.ToString());
                }
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