# DBAWrapper-Npgsql-C_Sharp-
C#で書かれたNpgsqlのラッパーです。
CallSample.cs の様にパラメーター指定を簡素に書けるようにしています。
接続文字列はコンストラクタにて指定します。

# 対応パラメーター
現在以下のパラメーターに対応しています。
- 任意のクラス(JSONにシリアライズしてDBで処理されます)
- Int16
- Int32
- Int64
- Single
- Double
- string
- DateOnly
- DateTime
- bool
- decimal
- Guid
- IPAddress
- PhysicalAddress
- NpgsqlPoint
- NpgsqlLine
- NpgsqlLSeg
- NpgsqlPath
- NpgsqlPolygon
- NpgsqlCircle
- NpgsqlBox
- NpgsqlTsQuery
- NpgsqlTsVector
- Int16配列
- Int32配列
- Int64配列
- Single配列
- Double配列
- string配列
- DateOnly配列
- DateTime配列
- bool配列
- decimal配列
- Guid配列
- IPAddress配列
- PhysicalAddress配列
- NpgsqlPoint配列
- NpgsqlLine配列
- NpgsqlLSeg配列
- NpgsqlPath配列
- NpgsqlPolygon配列
- NpgsqlCircle配列
- NpgsqlBox配列
- NpgsqlTsQuery配列
- NpgsqlTsVector配列

# 動作環境
以下環境で動作確認しています。
- .NET Framework 6.0
- Npgsql 6.0.6
- PostgreSQL 14.5
