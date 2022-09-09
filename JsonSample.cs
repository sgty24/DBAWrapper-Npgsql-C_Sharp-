using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBAWrapperCSharp {
    [Serializable]
    internal class JsonSample {
        public Int32 x { get; set; }
        public string y { get; set; }
        public DateTime z { get; set; }

        public JsonSample() {
            x = 100;
            y = "test";
            z = DateTime.Now;
        }
    }
}
