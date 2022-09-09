using System;
using System.Data;
using System.Net;
using System.Net.NetworkInformation;
using System.Reflection.PortableExecutable;
using System.Text.Json;
using System.Text.Json.Serialization;
using NpgsqlTypes;
using DBAWrapperCSharp;

try {
    using (DBAWrapper dba = new DBAWrapper(DBAWrapperCSharp.Properties.Resources.connect_str)) {
        // DBオープン
        dba.OpenConnect();

        // テーブル生成文
        /*
           CREATE TABLE IF NOT EXISTS public.sample_table(
                   col_010 json,
                   col_020 smallint,
                   col_030 integer,
                   col_040 bigint,
                   col_050 real,
                   col_060 double precision,
                   col_070 character varying(10),
                   col_080 date,
                   col_090 timestamp without time zone,
                   col_100 interval,
                   col_110 boolean,
                   col_120 numeric,
                   col_130 uuid,
                   col_140 inet,
                   col_150 macaddr,
                   col_160 point,
                   col_170 line,
                   col_180 lseg,
                   col_190 path,
                   col_200 polygon,
                   col_210 circle,
                   col_220 box,
                   col_230 tsquery,
                   col_240 tsvector)
         */

        // INSERT
        var json_data = new JsonSample();
        dba.ParamAddFirstJson<JsonSample>("@v_col_010", json_data);                     //json
        dba.ParamAddNext("@v_col_020", Int16.MaxValue);                             //smallint
        dba.ParamAddNext("@v_col_030", Int32.MaxValue);                             //integer
        dba.ParamAddNext("@v_col_040", Int64.MaxValue);                             //bigint
        dba.ParamAddNext("@v_col_050", Single.MinValue);                            //real
        dba.ParamAddNext("@v_col_060", Double.MaxValue);                            //double precision
        dba.ParamAddNext("@v_col_070", "test");                                     //character varying(10)
        dba.ParamAddNext("@v_col_080", new DateOnly(DateTime.Today.Year, 
                                                    DateTime.Today.Month, 
                                                    DateTime.Today.Day));           //date
        dba.ParamAddNext("@v_col_090", DateTime.MaxValue);                          //timestamp without time zone
        dba.ParamAddNext("@v_col_100", (DateTime.MaxValue - DateTime.Now));         //interval
        dba.ParamAddNext("@v_col_110", true);                                       //boolean
        dba.ParamAddNext("@v_col_120", Decimal.MaxValue);                           //numeric
        dba.ParamAddNext("@v_col_130", Guid.NewGuid());                             //uuid
        dba.ParamAddNext("@v_col_140", new IPAddress(new byte[]
                                                        { 192, 168, 1, 1 }));       //inet
        dba.ParamAddNext("@v_col_150", new PhysicalAddress(new byte[]
                                                        { 0x01, 0x02, 
                                                          0x03, 0x04, 
                                                          0x05, 0x06 }));           //macaddr
        dba.ParamAddNext("@v_col_160", new NpgsqlPoint(1,2));                       //point
        dba.ParamAddNext("@v_col_170", new NpgsqlLine(4,5,6));                      //line
        dba.ParamAddNext("@v_col_180", new NpgsqlLSeg(7,8,9,10));                   //lseg
        dba.ParamAddNext("@v_col_190", new NpgsqlPath(new NpgsqlPoint(11, 12),      
                                                      new NpgsqlPoint(13, 14),      
                                                      new NpgsqlPoint(15, 16),      
                                                      new NpgsqlPoint(17, 18)));    //path
        dba.ParamAddNext("@v_col_200", new NpgsqlPolygon(new NpgsqlPoint(21, 12),   
                                                         new NpgsqlPoint(23, 14),   
                                                         new NpgsqlPoint(25, 16),   
                                                         new NpgsqlPoint(27, 18))); //polygon
        dba.ParamAddNext("@v_col_210", new NpgsqlCircle(new NpgsqlPoint(30,1),30)); //circle
        dba.ParamAddNext("@v_col_220", new NpgsqlBox(31,32,33,34));                 //box
        dba.ParamAddNext("@v_col_230", (NpgsqlTsQuery?)null);                       //tsquery
        dba.ParamAddNext("@v_col_240", (NpgsqlTsVector?)null);                      //tsvector
        dba.Exec("INSERT INTO public.sample_table(" +
                   "col_010," +
                   "col_020," +
                   "col_030," +
                   "col_040," +
                   "col_050," +
                   "col_060," +
                   "col_070," +
                   "col_080," +
                   "col_090," +
                   "col_100," +
                   "col_110," +
                   "col_120," +
                   "col_130," +
                   "col_140," +
                   "col_150," +
                   "col_160," +
                   "col_170," +
                   "col_180," +
                   "col_190," +
                   "col_200," +
                   "col_210," +
                   "col_220," +
                   "col_230," +
                   "col_240" +
                  ")VALUES(" +
                   "@v_col_010," +
                   "@v_col_020," +
                   "@v_col_030," +
                   "@v_col_040," +
                   "@v_col_050," +
                   "@v_col_060," +
                   "@v_col_070," +
                   "@v_col_080," +
                   "@v_col_090," +
                   "@v_col_100," +
                   "@v_col_110," +
                   "@v_col_120," +
                   "@v_col_130," +
                   "@v_col_140," +
                   "@v_col_150," +
                   "@v_col_160," +
                   "@v_col_170," +
                   "@v_col_180," +
                   "@v_col_190," +
                   "@v_col_200," +
                   "@v_col_210," +
                   "@v_col_220," +
                   "@v_col_230," +
                   "@v_col_240" +
                  ");");

        // SELECT
        dba.ParamAddFirst("@v_col_070", "test");
        using (var dr_ref = dba.ExecSelect("SELECT * FROM sample_table WHERE col_070=@v_col_070;")) {
            // スキーマ取得
            var column_schema = dr_ref.GetColumnSchema();
            if (column_schema is null) {
                throw new Exception("ColumnSchema取得エラー");
            }

            // 列名表示
            foreach (var col in column_schema) {
                Console.Write(String.Format("{0},", col.ColumnName));
            }
            Console.Write("\r\n");

            //データ表示
            Int64 rec_num = 0;
            while(dr_ref.Read()) {
                foreach (var col in column_schema) {
                    Console.Write(String.Format("{0},", dr_ref[col.ColumnName]));
                }
                Console.Write("\r\n");
                rec_num++;
            }
            Console.WriteLine($"該当件数:{rec_num}");
        }
    }
} catch(Exception _ex) {
    // エラーハンドリング
    Console.WriteLine(_ex.Message);
}
