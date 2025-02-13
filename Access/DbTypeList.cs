namespace Public.DB
{
    /// <summary>
    /// 数据类型
    /// </summary>
    public class DbTypeList
    {
        /// <summary>
        /// 数据类型集合
        /// </summary>
        public static Dictionary<string, DbType> DbTypes { get; } = new Dictionary<string, DbType>(StringComparer.OrdinalIgnoreCase) {
            { "AnsiString",DbType.AnsiString },
            { "Binary", DbType.Binary },
            { "Byte[]", DbType.Binary },
            { "Byte",DbType.Byte },
            { "Boolean", DbType.Boolean },
            { "Currency",DbType.Currency },
            { "Date", DbType.Date },
            { "DateTime",DbType.DateTime },
            { "Decimal", DbType.Decimal },
            { "Double",DbType.Double },
            { "Guid", DbType.Guid },
            { "Int16",DbType.Int16 },
            { "Int32", DbType.Int32 },
            { "Int64",DbType.Int64 },
            { "Object", DbType.Object },
            { "SByte",DbType.SByte },
            { "Single", DbType.Single },
            { "String",DbType.String },
            { "Time", DbType.Time },
            { "UInt16",DbType.UInt16 },
            { "UInt32", DbType.UInt32 },
            { "UInt64",DbType.UInt64 },
            { "VarNumeric", DbType.VarNumeric },
            { "AnsiStringFixedLength",DbType.AnsiStringFixedLength },
            { "StringFixedLength", DbType.StringFixedLength },
            { "Xml",DbType.Xml },
            { "DateTime2", DbType.DateTime2 },
            { "DateTimeOffset",DbType.DateTimeOffset }
        };

        /// <summary>
        /// 根据数据类型名称获取类型
        /// </summary>
        /// <param name="dbTypeName"></param>
        /// <returns></returns>
        public static DbType GetDbType(string dbTypeName)
        {
            if (string.IsNullOrEmpty(dbTypeName))
                return DbType.String;

            if (0 <= dbTypeName.IndexOf('.'))
                dbTypeName = dbTypeName.Substring(dbTypeName.LastIndexOf('.') + 1);

            if (!DbTypes.ContainsKey(dbTypeName))
                return DbType.String;

            return DbTypes[dbTypeName.Trim()];
        }

        /// <summary>
        /// 根据数据类型得到转换后的值
        /// </summary>
        /// <param name="dbType"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static object GetConvertValue(DbType dbType, object value)
        {
            object oValue = null;

            if (Equals(null, dbType) || Equals(null, value))
                return oValue;

            if (value is IConvertible)
                return value;

            if (dbType != DbType.Binary)
                value = value.ToString();

            switch (dbType)
            {
                case DbType.Binary:
                    oValue = value as byte[];
                    break;
                case DbType.Byte:
                    oValue = Convert.ToByte(value);
                    break;
                case DbType.Boolean:
                    oValue = Convert.ToBoolean(value);
                    break;
                case DbType.Currency:
                    oValue = Convert.ToDouble(value);
                    break;
                case DbType.Date:
                    oValue = Convert.ToDateTime(value);
                    break;
                case DbType.DateTime:
                    oValue = Convert.ToDateTime(value);
                    break;
                case DbType.Decimal:
                    oValue = Convert.ToDouble(value);
                    break;
                case DbType.Double:
                    oValue = Convert.ToDouble(value);
                    break;
                case DbType.Int16:
                    oValue = Convert.ToInt16(value);
                    break;
                case DbType.Int32:
                    oValue = Convert.ToInt32(value);
                    break;
                case DbType.Int64:
                    oValue = Convert.ToInt64(value);
                    break;
                case DbType.SByte:
                    oValue = Convert.ToInt16(value);
                    break;
                case DbType.Single:
                    oValue = Convert.ToDouble(value);
                    break;
                case DbType.Time:
                    oValue = Convert.ToDateTime(value);
                    break;
                case DbType.UInt16:
                    oValue = Convert.ToUInt16(value);
                    break;
                case DbType.UInt32:
                    oValue = Convert.ToUInt32(value);
                    break;
                case DbType.UInt64:
                    oValue = Convert.ToUInt64(value);
                    break;
                case DbType.VarNumeric:
                    oValue = Convert.ToDouble(value);
                    break;
                case DbType.DateTime2:
                    oValue = Convert.ToDateTime(value);
                    break;
                default:
                    oValue = value;
                    break;
            }

            return oValue;
        }

    }
}
