namespace Public.DB
{
    /// <summary>
    /// 查询参数
    /// </summary>
    [Serializable]
    public class QueryParameter : MarshalByRefObject, IDbDataParameter, ICloneable
    {
        private bool m_forceSize = false;
        private int m_size;
        private string m_sourceColumn = "";
        private object m_value;

        #region 属性
        /// <summary>
        /// 最大位数
        /// </summary>
        public byte Precision { get; set; }
        /// <summary>
        /// 小数位数
        /// </summary>
        public byte Scale { get; set; }
        /// <summary>
        /// 数据类型宽度
        /// </summary>
        public int Size
        {
            get
            {
                if (m_forceSize)
                    return m_size;
                return 0;
            }
            set
            {
                if (value < 0)
                {
                    throw new Exception(value.ToString());
                }
                if (value != 0)
                {
                    m_forceSize = true;
                    m_size = value;
                    return;
                }
                m_forceSize = false;
                m_size = -1;
            }
        }
        public DbType DbType { get; set; }
        public ParameterDirection Direction { get; set; }

        public bool IsNullable { get; }

        public string ParameterName { get; set; } = string.Empty;
        public string SourceColumn
        {
            get
            {
                if (string.IsNullOrEmpty(m_sourceColumn))
                    return string.Empty;
                return m_sourceColumn;

            }
            set
            {
                m_sourceColumn = value;
            }
        }
        public DataRowVersion SourceVersion { get; set; }
        public object Value
        {
            get
            {
                if (Equals(m_value, null))
                    return DBNull.Value;
                else
                    return m_value;
            }
            set
            {
                m_value = DbTypeList.GetConvertValue(DbType, value);
            }
        }
        #endregion

        #region 构造方法
        public QueryParameter()
        {
            Value = null;
            Direction = ParameterDirection.Input;
            SourceVersion = DataRowVersion.Current;
            m_forceSize = false;
            m_size = -1;
        }
        public QueryParameter(string parameterName, object value) : this()
        {
            ParameterName = parameterName;
            Value = value;
        }
        public QueryParameter(string parameterName, object value, DbType dbType) : this()
        {
            ParameterName = parameterName;
            DbType = dbType;
            Value = value;
        }

        public QueryParameter(string parameterName, DbType dbType) : this()
        {
            ParameterName = parameterName;
            DbType = dbType;
        }

        public QueryParameter(string parameterName, DbType dbType, int size) : this()
        {
            ParameterName = parameterName;
            DbType = dbType;
            Size = size;
        }

        public QueryParameter(string parameterName, DbType dbType, int size, string sourceColumn) : this()
        {
            ParameterName = parameterName;
            DbType = dbType;
            Size = size;
            SourceColumn = sourceColumn;
        }

        public QueryParameter(string parameterName, DbType dbType, int size, ParameterDirection direction, bool isNullable, byte precision, byte scale, string sourceColumn, DataRowVersion sourceVersion, object value) : this()
        {
            ParameterName = parameterName;
            DbType = dbType;
            Size = size;
            Direction = direction;
            IsNullable = isNullable;
            Precision = precision;
            Scale = scale;
            SourceColumn = sourceColumn;
            SourceVersion = sourceVersion;
            Value = value;
        }
        #endregion

        #region 方法
        public override string ToString()
        {
            return ParameterName;
        }

        public object Clone()
        {
            QueryParameter parameter1;
            parameter1 = new QueryParameter(ParameterName, DbType, Size, Direction, IsNullable, Precision, Scale, SourceColumn, SourceVersion, Value);
            if (Value as ICloneable != null)
            {
                parameter1.Value = ((ICloneable)Value).Clone();
            }
            return parameter1;
        }
        #endregion
    }
}
