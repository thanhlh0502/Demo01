using System;
using System.Data.SqlClient;
using System.Text;

namespace CloneStructure
{
    class Program
    {
        static void Main(string[] args)
        {
            string sourceConnectionString = "Server=10.217.4.12;Database=Direplacement;User Id=diconnect;Password=diconnect;";
            string targetDbName = "TL_Direplacement";
            string targetConnectionString = $"Server=10.217.3.120;Database={targetDbName};User Id=diconnect;Password=diconnect;";

            // Đã có sẵn database đích, bỏ qua bước tạo database

            // 2. Lấy danh sách các bảng dbo
            var tableNames = new System.Collections.Generic.List<string>();
            using (var conn = new SqlConnection(sourceConnectionString))
            {
                conn.Open();
                using (var cmd = new SqlCommand("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE' AND TABLE_SCHEMA = 'dbo'", conn))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string schema = reader.GetString(0);
                        string table = reader.GetString(1);
                        tableNames.Add($"[{schema}].[{table}]");
                    }
                }
            }

            // 3. Lấy script tạo bảng và tạo bảng ở DB mới
            using (var sourceConn = new SqlConnection(sourceConnectionString))
            using (var targetConn = new SqlConnection(targetConnectionString))
            {
                sourceConn.Open();
                targetConn.Open();
                foreach (var table in tableNames)
                {
                    try
                    {
                        string createScript = GetCreateTableScript(sourceConn, table);
                        if (!string.IsNullOrEmpty(createScript))
                        {
                            using (var cmd = new SqlCommand(createScript, targetConn))
                            {
                                cmd.ExecuteNonQuery();
                            }
                            Console.WriteLine($"Cloned: {table}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Skipped: {table} - {ex.Message}");
                    }
                }
            }

            Console.WriteLine("Done!");
        }

        // Hàm lấy script tạo bảng (chỉ cấu trúc)
        static string GetCreateTableScript(SqlConnection conn, string tableName)
        {
            // Sử dụng sp_helptext để lấy script, hoặc tự build script
            // Ở đây dùng cách đơn giản: lấy từ sys.columns, sys.types, sys.objects
            // Để đơn giản, chỉ lấy các cột, không lấy index, khóa ngoại, v.v.
            var sb = new StringBuilder();
            string schema = tableName.Split('.')[0].Trim('[', ']');
            string table = tableName.Split('.')[1].Trim('[', ']');
            sb.AppendLine($"CREATE TABLE {tableName} (");
            using (var cmd = new SqlCommand(@"SELECT c.name, t.name, c.max_length, c.is_nullable, c.is_identity FROM sys.columns c JOIN sys.types t ON c.user_type_id = t.user_type_id WHERE c.object_id = OBJECT_ID(@tbl)", conn))
            {
                cmd.Parameters.AddWithValue("@tbl", $"{schema}.{table}");
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string colName = reader.GetString(0);
                        string typeName = reader.GetString(1);
                        short maxLength = reader.GetInt16(2);
                        bool isNullable = reader.GetBoolean(3);
                        bool isIdentity = reader.GetBoolean(4);
                        sb.Append($"    [{colName}] {typeName}");
                        if (typeName == "nvarchar" || typeName == "varchar" || typeName == "char" || typeName == "nchar")
                        {
                            sb.Append($"({(maxLength == -1 ? "MAX" : (maxLength / (typeName.StartsWith("n") ? 2 : 1)).ToString())})");
                        }
                        if (isIdentity)
                            sb.Append(" IDENTITY(1,1)");
                        sb.Append(isNullable ? " NULL" : " NOT NULL");
                        sb.AppendLine(",");
                    }
                }
            }
            if (sb[sb.Length - 2] == ',') sb.Length -= 2; // Xóa dấu phẩy cuối
            sb.AppendLine(")");
            return sb.ToString();
        }
    }
}
