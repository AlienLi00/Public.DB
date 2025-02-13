namespace Public.DB
{
    /// <summary>
    /// 数据库属性
    /// </summary>
    public class DatabaseProperty
    {
        /// <summary>
        /// 数据连接字符串
        /// </summary>
        public string ConnectionString { get; set; } = "";
        /// <summary>
        /// 数据库类型
        /// </summary>
        public DatabaseType DatabaseType { get; set; }
        /// <summary>
        /// 令牌
        /// </summary>
        public string Token { get; set; } = "";
    }
}