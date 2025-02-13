using MySql.Data.MySqlClient;

namespace Public.DB
{
    public class MySqlDAO : DAO
    {
        #region 构造方法
        public MySqlDAO(String connString)
        {
            this.DbConnection = new MySqlConnection(connString);
            this.ConnString = connString;
        }
        #endregion

        #region 属性
        public override IDbConnection DbConnection { get; }

        public override DatabaseType DatabaseType => DatabaseType.MySql;

        public override string ConnString { get; }
        #endregion

        #region NewDbConnection
        public override IDbConnection NewDbConnection(string connString)
        {
            return new MySqlConnection(connString);
        }
        #endregion

        #region BuildDataAdapter
        public override IDataAdapter BuildDataAdapter(IDbCommand selectCmd)
        {
            var adapter = new MySqlDataAdapter(selectCmd as MySqlCommand);
            if (Equals(null, adapter.SelectCommand.Connection))
                adapter.SelectCommand.Connection = this.DbConnection as MySqlConnection;
            return adapter;
        }
        public override IDataAdapter BuildDataAdapter(string selectCmd)
        {
            var adapter = new MySqlDataAdapter(selectCmd, this.DbConnection as MySqlConnection);
            adapter.SelectCommand.CommandTimeout = 3600;//设置超时为1小时
            return adapter;
        }

        public override IDataAdapter BuildDataAdapter(IDbCommand insertCommand, IDbCommand updateCommand, IDbCommand deleteCommand)
        {
            var adapter = new MySqlDataAdapter();
            adapter.InsertCommand = insertCommand as MySqlCommand;
            if (Equals(null, adapter.InsertCommand.Connection))
                adapter.InsertCommand.Connection = this.DbConnection as MySqlConnection;

            adapter.UpdateCommand = updateCommand as MySqlCommand;
            if (Equals(null, adapter.UpdateCommand.Connection))
                adapter.UpdateCommand.Connection = this.DbConnection as MySqlConnection;

            adapter.DeleteCommand = deleteCommand as MySqlCommand;
            if (Equals(null, adapter.DeleteCommand.Connection))
                adapter.DeleteCommand.Connection = this.DbConnection as MySqlConnection;

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
