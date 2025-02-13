using Npgsql;

namespace Public.DB
{
    public class PostgreDAO : DAO
    {
        #region 构造方法
        public PostgreDAO(String connString)
        {
            this.DbConnection = new NpgsqlConnection(connString);
            this.ConnString = connString;
        }
        #endregion

        #region 属性
        public override IDbConnection DbConnection { get; }

        public override DatabaseType DatabaseType => DatabaseType.PostgreSql;

        public override string ConnString { get; }
        #endregion

        #region NewDbConnection
        public override IDbConnection NewDbConnection(string connString)
        {
            return new NpgsqlConnection(connString);
        }
        #endregion

        #region BuildDataAdapter
        public override IDataAdapter BuildDataAdapter(IDbCommand selectCmd)
        {
            var adapter = new NpgsqlDataAdapter(selectCmd as NpgsqlCommand);
            if (Equals(null, adapter.SelectCommand.Connection))
                adapter.SelectCommand.Connection = this.DbConnection as NpgsqlConnection;
            return adapter;
        }
        public override IDataAdapter BuildDataAdapter(string selectCmd)
        {
            var adapter = new NpgsqlDataAdapter(selectCmd, this.DbConnection as NpgsqlConnection);
            adapter.SelectCommand.CommandTimeout = 3600;//设置超时为1小时
            return adapter;
        }

        public override IDataAdapter BuildDataAdapter(IDbCommand insertCommand, IDbCommand updateCommand, IDbCommand deleteCommand)
        {
            var adapter = new NpgsqlDataAdapter();
            adapter.InsertCommand = insertCommand as NpgsqlCommand;
            if (Equals(null, adapter.InsertCommand.Connection))
                adapter.InsertCommand.Connection = this.DbConnection as NpgsqlConnection;

            adapter.UpdateCommand = updateCommand as NpgsqlCommand;
            if (Equals(null, adapter.UpdateCommand.Connection))
                adapter.UpdateCommand.Connection = this.DbConnection as NpgsqlConnection;

            adapter.DeleteCommand = deleteCommand as NpgsqlCommand;
            if (Equals(null, adapter.DeleteCommand.Connection))
                adapter.DeleteCommand.Connection = this.DbConnection as NpgsqlConnection;

            return adapter;
        }
        #endregion

        #region ExecuteXmlReader
        public override XmlReader ExecuteXmlReader(CommandType commandType, string commandText, QueryParameterCollection commandParameters, string transId)
        {
            throw new NotImplementedException();
        }
        #endregion
    }
}
