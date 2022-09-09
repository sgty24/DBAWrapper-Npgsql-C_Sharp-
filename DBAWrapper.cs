using System;
using System.Data;
using System.Net.NetworkInformation;
using System.Net;
using System.Text.Json;
using Npgsql;
using NpgsqlTypes;
using System.Runtime.Serialization;

/// ''' <summary>DB接続管理クラス</summary>
public class DBAWrapper : IDisposable {
    // *******************************************************************************************
    // ** メンバ変数 *****************************************************************************
    /// <summary>PostgreSQL接続オブジェクト</summary>
    private NpgsqlConnection m_cn = new NpgsqlConnection();

    /// <summary>PostgreSQLコマンドオブジェクト</summary>
    private NpgsqlCommand? m_cmd = null;

    /// <summary>PostgreSQLレコードオブジェクト</summary>
    private NpgsqlDataReader? m_dr = null;

    /// <summary>パラメーターリスト</summary>
    private List<Npgsql.NpgsqlParameter> m_param_list = new List<Npgsql.NpgsqlParameter>();

    /// <summary>パラメーターリスト</summary>
    private bool m_is_begin_tran = false;

    // *******************************************************************************************
    // ** メンバ関数 *****************************************************************************
    /// ----------------------------------------------------------------------------------------
    /// <summary>デフォルトコンストラクタ(呼び出し不可)</summary>
    private DBAWrapper() {
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>コンストラクタ</summary>
    public DBAWrapper(string _con_str) {
        m_cn.ConnectionString = _con_str;
    }

    /// ----------------------------------------------------------------------------------------
    ///'''<summary>DB接続オープン</summary>
    public void OpenConnect() {
        this.m_cn.Open();
    }

    /// ----------------------------------------------------------------------------------------
    ///'''<summary>パラメーター付SQL実行(結果セットなし)</summary>
    ///'''<param name="_sql">実行SQL</param>
    public void Exec(string _sql) {
        CloseDataReader();
        this.m_cmd = new NpgsqlCommand();
        this.m_cmd.Connection = this.m_cn;
        this.m_cmd.CommandText = _sql;
        foreach (Npgsql.NpgsqlParameter param in this.m_param_list) {
            this.m_cmd.Parameters.Add(param);
        }
        this.m_cmd.ExecuteNonQuery();
        this.m_param_list.Clear();
    }

    /// ----------------------------------------------------------------------------------------
    ///'''<summary>パラメーター付SQL実行(結果セットあり)</summary>
    ///'''<param name="_sql">実行SQL</param>
    public Npgsql.NpgsqlDataReader ExecSelect(string _sql) {
        CloseDataReader();
        this.m_cmd = new NpgsqlCommand();
        this.m_cmd.Connection = this.m_cn;
        this.m_cmd.CommandText = _sql;
        foreach (Npgsql.NpgsqlParameter param in this.m_param_list) {
            this.m_cmd.Parameters.Add(param);
        }
        this.m_dr = this.m_cmd.ExecuteReader();
        this.m_param_list.Clear();
        return this.m_dr;
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>データセットを取得する</summary>
    /// <param name="_sql">>実行SQL</param>
    /// <returns>データセット</returns>
    public System.Data.DataSet Fill(string _sql) {
        CloseDataReader();
        System.Data.DataSet ds = new System.Data.DataSet();
        using (NpgsqlDataAdapter da = new NpgsqlDataAdapter()) {
            this.m_cmd = new NpgsqlCommand(_sql, m_cn);

            foreach (Npgsql.NpgsqlParameter param in this.m_param_list) {
                this.m_cmd.Parameters.Add(param);
            }

            da.SelectCommand = this.m_cmd;
            da.Fill(ds);
        }

        this.m_param_list.Clear();

        return ds;
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(共通処理)</summary>
    /// <param name="_param">パラメーター</param>
    /// <param name="_value_is_null">値がNULLか？</param>
    public void ParamAdd(NpgsqlParameter _param, bool _value_is_null) {
        // NULL指定なら値上書き
        if (_value_is_null) {
            _param.Value = System.DBNull.Value;
        }

        //パラメータ追加
        this.m_param_list.Add(_param);
    }

    #region 非配列ParamAddFirst
    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Json)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirstJson<T>(string _param_name, T? _value) where T : new() {
        this.m_param_list.Clear();
        this.ParamAddNextJson(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Int16)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Int16? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Int32)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Int32? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Int64)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Int64? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Single)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Single? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Single)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Double? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/String)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, string? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/DateOnly)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, DateOnly? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/DateTime)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, DateTime? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Boolean)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, bool? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Decimal)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, decimal? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Guid)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Guid? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/IPAddress)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, IPAddress? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/PhysicalAddress)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, PhysicalAddress? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlPoint)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlPoint? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlLine)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlLine? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlLSeg)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlLSeg? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlPath)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlPath? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlPolygon)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlPolygon? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlCircle)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlCircle? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlBox)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlBox? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlTsQuery)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlTsQuery? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlTsVector)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlTsVector? _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }
    #endregion

    #region 非配列ParamAddNext
    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Json)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNextJson<T>(string _param_name, T? _value) where T : new() {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Json);
        if (_value is null) {
            db_val.Value = "";
        } else {
            db_val.Value = JsonSerializer.Serialize(_value);
        }
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int16)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Int16? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Smallint);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int32)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Int32? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Integer);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int64)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Int64? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Bigint);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Single)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Single? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Real);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int64)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Double? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Double);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/String)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, string? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Varchar);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/DateOnly)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, DateOnly? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Date);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/DateTime)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, DateTime? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Timestamp);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/TimeSpan)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, TimeSpan? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Interval);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Boolean)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, bool? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Boolean);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Decimal)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Decimal? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Numeric);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Guid)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Guid? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Uuid);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/IPAddress)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, IPAddress? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Inet);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/PhysicalAddress)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, PhysicalAddress? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.MacAddr);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlPoint)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlPoint? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Point);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlLine)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlLine? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Line);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlLSeg)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlLSeg? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.LSeg);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlPath? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Path);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlPolygon)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlPolygon? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Polygon);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlCircle)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlCircle? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Circle);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlBox)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlBox? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Box);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlTsQuery)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlTsQuery? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.TsQuery);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlTsVector)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlTsVector? _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.TsVector);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }
    #endregion

    #region 配列ParamAddFirst
    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Json配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirstJson<T>(string _param_name, T?[] _value) where T : new() {
        this.m_param_list.Clear();
        this.ParamAddNextJson(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Int16配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Int16?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Int32配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Int32?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Int64配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Int64?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Single配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Single?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Single配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Double?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/String配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, string?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/DateOnly配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, DateOnly?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/DateTime配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, DateTime?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Boolean配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, bool?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Decimal配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, decimal?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/Guid配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, Guid?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/IPAddress配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, IPAddress?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/PhysicalAddress配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, PhysicalAddress?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlPoint配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlPoint?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlLine配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlLine?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlLSeg配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlLSeg?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlPath配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlPath?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlPolygon配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlPolygon?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlCircle配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlCircle?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlBox配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlBox?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlTsQuery配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlTsQuery?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回/NpgsqlTsVector配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddFirst(string _param_name, NpgsqlTsVector?[] _value) {
        this.m_param_list.Clear();
        this.ParamAddNext(_param_name, _value);
    }
    #endregion

    #region 配列ParamAddNext
    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Json配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNextJson<T>(string _param_name, T?[] _value) where T : new() {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Json | NpgsqlDbType.Array);
        if (_value is null) {
            db_val.Value = "";
        } else {
            db_val.Value = JsonSerializer.Serialize(_value);
        }
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int16配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Int16?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Smallint | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int32配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Int32?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Integer | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int64配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Int64?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Bigint | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Single配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Single?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Real | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Int64配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Double?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Double | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/String配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, string?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Varchar | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/DateOnly配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, DateOnly?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Date | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/DateTime配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, DateTime?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Timestamp | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/TimeSpan配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, TimeSpan?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Interval | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Boolean配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, bool?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Boolean | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Decimal配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Decimal?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Numeric | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/Guid配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, Guid?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Uuid | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/IPAddress配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, IPAddress?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Inet | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/PhysicalAddress配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, PhysicalAddress?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.MacAddr | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlPoint配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlPoint?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Point | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlLine配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlLine?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Line | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlLSeg配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlLSeg?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.LSeg | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlPath?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Path | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlPolygon配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlPolygon?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Polygon | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlCircle配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlCircle?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Circle | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlBox配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlBox?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.Box | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlTsQuery配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlTsQuery?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.TsQuery | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>パラメーター追加(初回以降/NpgsqlTsVector配列)</summary>
    /// <param name="_param_name">パラメーター名</param>
    /// <param name="_value">値</param>
    public void ParamAddNext(string _param_name, NpgsqlTsVector?[] _value) {
        var db_val = new NpgsqlParameter(_param_name, NpgsqlDbType.TsVector | NpgsqlDbType.Array);
        db_val.Value = _value;
        this.ParamAdd(db_val, _value is null);
    }
    #endregion

    /// ----------------------------------------------------------------------------------------
    /// <summary>結果セットを閉じる</summary>
    private void CloseDataReader() {
        if (this.m_dr is not null) {
            this.m_dr.Close();
            this.m_cmd = null;
            this.m_dr = null;
        }
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>トランザクション開始</summary>
    public void BeginTran() {
        this.Exec("begin transaction;");
        m_is_begin_tran = true;
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>トランザクションコミット</summary>
    public void CommitTran() {
        this.Exec("commit;");
        m_is_begin_tran = false;
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>トランザクションロールバック</summary>
    public void RollbackTran() {
        if (this.m_cn.State == ConnectionState.Open) {
            m_is_begin_tran = false;
            this.Exec("rollback;");
        }
    }

    /// ----------------------------------------------------------------------------------------
    /// <summary>オブジェクト廃棄処理</summary>
    public void Dispose() {
        if (m_is_begin_tran) {
            this.RollbackTran();
        }
            
        CloseDataReader();
        this.m_cmd = null;
        if (this.m_cn.State == ConnectionState.Open) {
            this.m_cn.Close();
        }   

        GC.SuppressFinalize(this);
    }
}
