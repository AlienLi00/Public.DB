using Oracle.ManagedDataAccess.Client;

namespace Public.DB
{
    /// <summary>
    /// OracleDAO类
    /// </summary>
    public class OracleDAO : DAO
    {
        #region 构造方法
        public OracleDAO(String connString)
        {
            this.DbConnection = new OracleConnection(connString);
            this.ConnString = connString;
        }
        #endregion

        #region 属性
        public override IDbConnection DbConnection { get; }

        public override DatabaseType DatabaseType => DatabaseType.Oracle;

        public override string ConnString { get; }
        #endregion

        #region NewDbConnection
        public override IDbConnection NewDbConnection(string connString)
        {
            return new OracleConnection(connString);
        }
        #endregion

        #region BuildDataAdapter
        public override IDataAdapter BuildDataAdapter(IDbCommand selectCmd)
        {
            var adapter = new OracleDataAdapter(selectCmd as OracleCommand);
            if (Equals(null, adapter.SelectCommand.Connection))
                adapter.SelectCommand.Connection = this.DbConnection as OracleConnection;
            return adapter;
        }
        public override IDataAdapter BuildDataAdapter(string selectCmd)
        {
            var adapter = new OracleDataAdapter(selectCmd, this.DbConnection as OracleConnection);
            adapter.SelectCommand.CommandTimeout = 3600;//设置超时为1小时
            return adapter;
        }

        public override IDataAdapter BuildDataAdapter(IDbCommand insertCommand, IDbCommand updateCommand, IDbCommand deleteCommand)
        {
            var adapter = new OracleDataAdapter();
            adapter.InsertCommand = insertCommand as OracleCommand;
            if (Equals(null, adapter.InsertCommand.Connection))
                adapter.InsertCommand.Connection = this.DbConnection as OracleConnection;

            adapter.UpdateCommand = updateCommand as OracleCommand;
            if (Equals(null, adapter.UpdateCommand.Connection))
                adapter.UpdateCommand.Connection = this.DbConnection as OracleConnection;

            adapter.DeleteCommand = deleteCommand as OracleCommand;
            if (Equals(null, adapter.DeleteCommand.Connection))
                adapter.DeleteCommand.Connection = this.DbConnection as OracleConnection;

            return adapter;
        }
        #endregion

        #region ExecuteXmlReader
        public override XmlReader ExecuteXmlReader(CommandType commandType, string commandText, QueryParameterCollection commandParameters, string transId)
        {
            XmlReader reader;
            //避免并行错误，如果没有事务，用using的方式
            if (Equals(null, GetTransaction(transId)))
            {
                using var conn = NewDbConnection(this.ConnString);
                using var cmd = conn.CreateCommand() as OracleCommand;
                PrepareCommand(conn, cmd, commandType, commandText, commandParameters);
                reader = cmd.ExecuteXmlReader();
            }
            else
            {
                var cmd = this.DbConnection.CreateCommand() as OracleCommand;
                PrepareCommand(DbConnection, cmd, commandType, commandText, commandParameters);
                reader = cmd.ExecuteXmlReader();
            }
            return reader;
        }
        #endregion
    }
}
