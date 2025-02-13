using System.Data.SqlClient;

namespace Public.DB
{
    /// <summary>
    /// DAO上层抽象类
    /// </summary>
    public class MSSqlDAO : DAO
    {
        #region 构造方法
        public MSSqlDAO(String connString)
        {
            this.DbConnection = new SqlConnection(connString);
            this.ConnString = connString;
        }
        #endregion

        #region 属性
        public override IDbConnection DbConnection { get; }

        public override DatabaseType DatabaseType => DatabaseType.MSSQLServer;

        public override string ConnString { get; }
        #endregion

        #region NewDbConnection
        public override IDbConnection NewDbConnection(string connString)
        {
            return new SqlConnection(connString);
        }
        #endregion

        #region BuildDataAdapter
        public override IDataAdapter BuildDataAdapter(IDbCommand selectCmd)
        {
            var adapter = new SqlDataAdapter(selectCmd as SqlCommand);
            if (Equals(null, adapter.SelectCommand.Connection))
                adapter.SelectCommand.Connection = this.DbConnection as SqlConnection;
            return adapter;
        }

        public override IDataAdapter BuildDataAdapter(string selectCmd)
        {
            var adapter = new SqlDataAdapter(selectCmd, this.DbConnection as SqlConnection);
            adapter.SelectCommand.CommandTimeout = 3600;//设置超时为1小时
            return adapter;
        }

        public override IDataAdapter BuildDataAdapter(IDbCommand insertCommand, IDbCommand updateCommand, IDbCommand deleteCommand)
        {
            var adapter = new SqlDataAdapter();
            adapter.InsertCommand = insertCommand as SqlCommand;
            if (Equals(null, adapter.InsertCommand.Connection))
                adapter.InsertCommand.Connection = this.DbConnection as SqlConnection;

            adapter.UpdateCommand = updateCommand as SqlCommand;
            if (Equals(null, adapter.UpdateCommand.Connection))
                adapter.UpdateCommand.Connection = this.DbConnection as SqlConnection;

            adapter.DeleteCommand = deleteCommand as SqlCommand;
            if (Equals(null, adapter.DeleteCommand.Connection))
                adapter.DeleteCommand.Connection = this.DbConnection as SqlConnection;

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
                using var cmd = conn.CreateCommand() as SqlCommand;
                PrepareCommand(conn, cmd, commandType, commandText, commandParameters);
                reader = cmd.ExecuteXmlReader();
            }
            else
            {
                var cmd = this.DbConnection.CreateCommand() as SqlCommand;
                PrepareCommand(DbConnection, cmd, commandType, commandText, commandParameters);
                reader = cmd.ExecuteXmlReader();
            }
            return reader;
        }
        #endregion
    }
}
