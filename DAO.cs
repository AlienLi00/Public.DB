global using System.Data;
global using System.Transactions;
global using System.Xml;
using System.Data.SqlClient;

namespace Public.DB
{
    /// <summary>
    /// DAO上层抽象类
    /// </summary>
    public abstract class DAO
    {
        #region 获取DAO
        private static Dictionary<String, DAO> daos = new Dictionary<string, DAO>(StringComparer.OrdinalIgnoreCase);
        public static DAO GetDAO(DatabaseProperty dp)
        {
            if (String.IsNullOrEmpty(dp.Token))
                dp.Token = "sys";

            if (!daos.ContainsKey(dp.Token))
                daos[dp.Token] = DAOFactory.CreateDAO(dp);

            return daos[dp.Token];
        }

        public static DAO GetDAO(String token)
        {
            if (String.IsNullOrEmpty(token))
                token = "sys";
            if (!daos.ContainsKey(token))
                return null;

            return daos[token];
        }
        #endregion

        #region 属性
        public abstract IDbConnection DbConnection { get; }
        public abstract String ConnString { get; }

        public abstract DatabaseType DatabaseType { get; }
        public bool IsClosed
        {
            get
            {
                return DbConnection.State.Equals(ConnectionState.Closed);
            }
        }
        #endregion

        #region 事务
        //事务集合
        private static Dictionary<String, TransactionScope> transScopes = new Dictionary<String, TransactionScope>(StringComparer.OrdinalIgnoreCase);
        /// <summary>
        /// 获取事务片
        /// </summary>
        /// <returns></returns>
        public TransactionScope GetTransaction(string transId)
        {
            if (String.IsNullOrEmpty(transId))
                transId = "";
            if (transScopes.ContainsKey(transId))
                return transScopes[transId];
            return null;
        }
        public void BeginTransaction(string transId)
        {
            if (String.IsNullOrEmpty(transId))
                return;

            transScopes[transId] = new TransactionScope(TransactionScopeOption.Required, new TransactionOptions() { IsolationLevel = System.Transactions.IsolationLevel.ReadUncommitted });
        }

        public void TransCommit(string transId)
        {
            TransactionScope ts = GetTransaction(transId);
            if (Equals(null, ts))
            {
                this.Close();
                return;
            }
            try
            {
                ts.Complete();
                ts.Dispose();
                transScopes.Remove(transId);
            }
            catch (Exception ex)
            {

            }
            finally
            {
                ts.Dispose();
                this.Close();
            }
        }
        public void TransRollback(string transId)
        {
            TransactionScope ts = GetTransaction(transId);
            if (Equals(null, ts))
            {
                this.Close();
                return;
            }
            try
            {
                ts.Dispose();
                transScopes.Remove(transId);
            }
            catch (Exception ex)
            {

            }
            finally
            {
                this.Close();
            }
        }
        #endregion

        #region 数据连接

        /// <summary>
        /// 获取新的数据连接
        /// </summary>
        /// <param name="connString"></param>
        /// <returns></returns>
        public abstract IDbConnection NewDbConnection(String connString);
        public void Open()
        {
            if (this.DbConnection.State.Equals(ConnectionState.Closed))
                this.DbConnection.Open();
        }
        public void Close()
        {
            try
            {
                this.DbConnection.Close();
            }
            catch (Exception e)
            {

            }
        }
        #endregion

        #region 数据适配器
        public abstract IDataAdapter BuildDataAdapter(string selectCmd);

        public abstract IDataAdapter BuildDataAdapter(IDbCommand selectCmd);

        public abstract IDataAdapter BuildDataAdapter(IDbCommand insertCommand, IDbCommand updateCommand, IDbCommand deleteCommand);
        #endregion

        #region SaveTable
        public void SaveTable(DataTable table, string commandText)
        {
            SaveTable(table, commandText, "");
        }

        public void SaveTable(DataTable dt, string commandText, string transId)
        {
            //避免并行错误，如果没有事务，用using的方式
            if (Equals(null, GetTransaction(transId)))
            {
                using var conn = NewDbConnection(this.ConnString);
                using var cmd = conn.CreateCommand();
                PrepareCommand(conn, cmd, CommandType.Text, commandText, null);
                SqlDataAdapter adapter = new SqlDataAdapter((SqlCommand)cmd);
                SqlCommandBuilder builder = new SqlCommandBuilder(adapter);
                int num = adapter.Update(dt);
            }
            else
            {
                using var cmd = DbConnection.CreateCommand();
                PrepareCommand(DbConnection, cmd, CommandType.Text, commandText, null);
                SqlDataAdapter adapter = new SqlDataAdapter((SqlCommand)cmd);
                SqlCommandBuilder builder = new SqlCommandBuilder(adapter);
                int num = adapter.Update(dt);
            }
        }
        #endregion

        #region ExecuteDataset
        public DataSet ExecuteDataset(string commandText)
        {
            return ExecuteDataset(CommandType.Text, commandText, null, null, null, "");
        }
        public DataSet ExecuteDataset(string commandText, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, null, null, null, transId);
        }
        public DataSet ExecuteDataset(CommandType commandType, string commandText, String transId)
        {
            return ExecuteDataset(commandType, commandText, null, null, null, transId);
        }

        public DataSet ExecuteDataset(string commandText, QueryParameterCollection commandParameters)
        {
            return ExecuteDataset(CommandType.Text, commandText, commandParameters, null, null, null);
        }

        public DataSet ExecuteDataset(string commandText, QueryParameterCollection commandParameters, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, commandParameters, null, null, transId);
        }

        public DataSet ExecuteDataset(CommandType commandType, string commandText, QueryParameterCollection commandParameters, String transId)
        {
            return ExecuteDataset(commandType, commandText, commandParameters, null, null, transId);
        }

        public DataSet ExecuteDataset(string commandText, DataSet ds, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, null, ds, null, transId);
        }
        public DataSet ExecuteDataset(CommandType commandType, string commandText, DataSet ds, String transId)
        {
            return ExecuteDataset(commandType, commandText, null, ds, null, transId);
        }

        public DataSet ExecuteDataset(string commandText, QueryParameterCollection commandParameters, DataSet ds, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, commandParameters, ds, null, transId);
        }
        public DataSet ExecuteDataset(CommandType commandType, string commandText, QueryParameterCollection commandParameters, DataSet ds, String transId)
        {
            return ExecuteDataset(commandType, commandText, commandParameters, ds, null, transId);
        }

        public DataSet ExecuteDataset(string commandText, string tableName, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, null, null, tableName, transId);
        }
        public DataSet ExecuteDataset(CommandType commandType, string commandText, string tableName, String transId)
        {
            return ExecuteDataset(commandType, commandText, null, null, tableName, transId);
        }

        public DataSet ExecuteDataset(string commandText, QueryParameterCollection commandParameters, string tableName, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, commandParameters, null, tableName, transId);
        }
        public DataSet ExecuteDataset(CommandType commandType, string commandText, QueryParameterCollection commandParameters, string tableName, String transId)
        {
            return ExecuteDataset(commandType, commandText, commandParameters, null, tableName, transId);
        }

        public DataSet ExecuteDataset(string commandText, DataSet ds, string tableName, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, null, ds, tableName, transId);
        }
        public DataSet ExecuteDataset(CommandType commandType, string commandText, DataSet ds, string tableName, String transId)
        {
            return ExecuteDataset(commandType, commandText, null, ds, tableName, transId);
        }

        public DataSet ExecuteDataset(string commandText, QueryParameterCollection commandParameters, DataSet ds, string tableName, String transId)
        {
            return ExecuteDataset(CommandType.Text, commandText, commandParameters, ds, tableName, transId);
        }
        public DataSet ExecuteDataset(CommandType commandType, string commandText, QueryParameterCollection commandParameters, DataSet ds, string tableName, string transId)
        {
            if (object.Equals(ds, null))
                ds = new DataSet();

            //避免并行错误，如果没有事务，用using的方式
            if (Equals(null, GetTransaction(transId)))
            {
                using var conn = NewDbConnection(this.ConnString);
                using var cmd = conn.CreateCommand();
                PrepareCommand(conn, cmd, commandType, commandText, commandParameters, false);
                var adapter = BuildDataAdapter(cmd);
                if (String.IsNullOrEmpty(tableName))
                    adapter.Fill(ds);
                else
                {
                    adapter.Fill(ds);
                    ds.Tables[0].TableName = tableName;
                }
                ds.RemotingFormat = SerializationFormat.Binary;
            }
            else
            {
                var cmd = this.DbConnection.CreateCommand();
                PrepareCommand(DbConnection, cmd, commandType, commandText, commandParameters);
                var adapter = BuildDataAdapter(cmd);
                if (String.IsNullOrEmpty(tableName))
                    adapter.Fill(ds);
                else
                {
                    adapter.Fill(ds);
                    ds.Tables[0].TableName = tableName;
                }
                ds.RemotingFormat = SerializationFormat.Binary;
            }
            return ds;
        }
        #endregion

        #region ExecuteDataTable
        public DataTable ExecuteDataTable(string commandText)
        {
            return ExecuteDataTable(CommandType.Text, commandText, null, "");
        }

        public DataTable ExecuteDataTable(string commandText, String transId)
        {
            return ExecuteDataTable(CommandType.Text, commandText, null, transId);
        }

        public DataTable ExecuteDataTable(CommandType commandType, string commandText, String transId)
        {
            return ExecuteDataTable(commandType, commandText, null, transId);
        }

        public DataTable ExecuteDataTable(string commandText, QueryParameterCollection commandParameters, String transId)
        {
            return ExecuteDataTable(CommandType.Text, commandText, commandParameters, transId);
        }

        public DataTable ExecuteDataTable(CommandType commandType, string commandText, QueryParameterCollection commandParameters, String transId)
        {
            // Call the corresponding ExecuteDataset method and extract the first table.
            DataSet ds = ExecuteDataset(commandType, commandText, commandParameters, null, null, transId);
            return ds.Tables.Count > 0 ? ds.Tables[0] : new DataTable();
        }
        #endregion

        #region ExecuteNonQuery
        public int ExecuteNonQuery(string commandText)
        {
            return this.ExecuteNonQuery(CommandType.Text, commandText, null, "");
        }
        public int ExecuteNonQuery(string commandText, String transId)
        {
            return this.ExecuteNonQuery(CommandType.Text, commandText, null, transId);
        }
        public int ExecuteNonQuery(CommandType commandType, string commandText, String transId)
        {
            return this.ExecuteNonQuery(commandType, commandText, null, transId);
        }

        public int ExecuteNonQuery(string commandText, QueryParameterCollection commandParameters)
        {
            return this.ExecuteNonQuery(CommandType.Text, commandText, commandParameters, null);
        }

        public int ExecuteNonQuery(string commandText, QueryParameterCollection commandParameters, String transId)
        {
            return this.ExecuteNonQuery(CommandType.Text, commandText, commandParameters, transId);
        }

        public int ExecuteNonQuery(CommandType commandType, string commandText, QueryParameterCollection commandParameters, string transId)
        {
            int iResult;
            //避免并行错误，如果没有事务，用using的方式
            if (Equals(null, GetTransaction(transId)))
            {
                using var conn = NewDbConnection(this.ConnString);
                using var cmd = conn.CreateCommand();
                PrepareCommand(conn, cmd, commandType, commandText, commandParameters);
                iResult = cmd.ExecuteNonQuery();
            }
            else
            {
                using var cmd = DbConnection.CreateCommand();
                PrepareCommand(DbConnection, cmd, commandType, commandText, commandParameters);
                iResult = cmd.ExecuteNonQuery();
            }
            return iResult;
        }
        #endregion

        #region ExecuteReader
        public IDataReader ExecuteReader(string commandText)
        {
            return ExecuteReader(CommandType.Text, commandText, null, "");
        }
        public IDataReader ExecuteReader(string commandText, String transId)
        {
            return ExecuteReader(CommandType.Text, commandText, null, transId);
        }
        public IDataReader ExecuteReader(CommandType commandType, string commandText, String transId)
        {
            return ExecuteReader(commandType, commandText, null, transId);
        }

        public IDataReader ExecuteReader(string commandText, QueryParameterCollection commandParameters, String transId)
        {
            return ExecuteReader(CommandType.Text, commandText, commandParameters, transId);
        }

        public IDataReader ExecuteReader(CommandType commandType, string commandText, QueryParameterCollection commandParameters, string transId)
        {
            IDataReader dr;
            //避免并行错误，如果没有事务，用using的方式
            if (Equals(null, GetTransaction(transId)))
            {
                using var conn = NewDbConnection(this.ConnString);
                using var cmd = conn.CreateCommand();
                PrepareCommand(conn, cmd, commandType, commandText, commandParameters);
                dr = cmd.ExecuteReader();
            }
            else
            {
                using var cmd = DbConnection.CreateCommand();
                PrepareCommand(DbConnection, cmd, commandType, commandText, commandParameters);
                dr = cmd.ExecuteReader();
            }
            return dr;
        }
        #endregion

        #region ExecuteScalar
        public object ExecuteScalar(string commandText)
        {
            return ExecuteScalar(CommandType.Text, commandText, null, "");
        }
        public object ExecuteScalar(string commandText, String transId)
        {
            return ExecuteScalar(CommandType.Text, commandText, null, transId);
        }
        public object ExecuteScalar(CommandType commandType, string commandText, String transId)
        {
            return ExecuteScalar(commandType, commandText, null, transId);
        }

        public object ExecuteScalar(string commandText, QueryParameterCollection commandParameters)
        {
            return ExecuteScalar(CommandType.Text, commandText, commandParameters, null);
        }

        public object ExecuteScalar(string commandText, QueryParameterCollection commandParameters, String transId)
        {
            return ExecuteScalar(CommandType.Text, commandText, commandParameters, transId);
        }

        public object ExecuteScalar(CommandType commandType, string commandText, QueryParameterCollection commandParameters, string transId)
        {
            Object oValue;
            //避免并行错误，如果没有事务，用using的方式
            if (Equals(null, GetTransaction(transId)))
            {
                using var conn = NewDbConnection(this.ConnString);
                using var cmd = conn.CreateCommand();
                PrepareCommand(conn, cmd, commandType, commandText, commandParameters);
                oValue = cmd.ExecuteScalar();
            }
            else
            {
                using var cmd = DbConnection.CreateCommand();
                PrepareCommand(DbConnection, cmd, commandType, commandText, commandParameters);
                oValue = cmd.ExecuteScalar();
            }
            return oValue;
        }
        #endregion

        #region ExecuteXmlReader
        public XmlReader ExecuteXmlReader(string commandText)
        {
            return ExecuteXmlReader(CommandType.Text, commandText, null, "");
        }
        public XmlReader ExecuteXmlReader(string commandText, String transId)
        {
            return ExecuteXmlReader(CommandType.Text, commandText, null, transId);
        }
        public XmlReader ExecuteXmlReader(CommandType commandType, string commandText, String transId)
        {
            return ExecuteXmlReader(commandType, commandText, null, transId);
        }
        public XmlReader ExecuteXmlReader(string commandText, QueryParameterCollection commandParameters, String transId)
        {
            return ExecuteXmlReader(CommandType.Text, commandText, commandParameters, transId);
        }
        public abstract XmlReader ExecuteXmlReader(CommandType commandType, string commandText, QueryParameterCollection commandParameters, string transId);
        #endregion

        #region FillTables
        public void FillTables(string commandText, DataTable[] tables, string transId = "")
        {
            FillTables(CommandType.Text, commandText, null, tables, transId);
        }
        public void FillTables(CommandType commandType, string commandText, DataTable[] tables, string transId = "")
        {
            FillTables(commandType, commandText, null, tables, transId);
        }

        public void FillTables(string commandText, QueryParameterCollection commandParameters, DataTable[] tables, string transId = "")
        {
            FillTables(CommandType.Text, commandText, commandParameters, tables, transId);
        }

        public void FillTables(CommandType commandType, string commandText, QueryParameterCollection commandParameters, DataTable[] tables, string transId = "")
        {
            //避免并行错误，如果没有事务，用using的方式
            using var ds = new DataSet();
            if (Equals(null, GetTransaction(transId)))
            {
                using var conn = NewDbConnection(this.ConnString);
                using var cmd = conn.CreateCommand();
                PrepareCommand(conn, cmd, commandType, commandText, commandParameters);
                var adapter = BuildDataAdapter(cmd);
                adapter.Fill(ds);
            }
            else
            {
                using var cmd = DbConnection.CreateCommand();
                PrepareCommand(DbConnection, cmd, commandType, commandText, commandParameters);
                var adapter = BuildDataAdapter(cmd);
                adapter.Fill(ds);
            }
            for (int i = 0; i < tables.Length; i++)
            {
                if (ds.Tables.Count > i)
                {
                    tables[i] = ds.Tables[i].Copy();
                }
            }
        }
        #endregion

        #region PrepareCommand
        public void PrepareCommand(IDbConnection conn, IDbCommand cmd, CommandType commandType, String commandText, QueryParameterCollection parameters, Boolean openDb = true)
        {
            if (conn.State.Equals(ConnectionState.Closed) && openDb)
                conn.Open();

            cmd.CommandType = commandType;
            cmd.CommandText = commandText;
            cmd.Connection = conn;
            cmd.CommandTimeout = 3600;//超时改为1个小时
            cmd.Parameters.Clear();
            IDbDataParameter param;
            if (Equals(null, parameters))
                return;
            for (int i = 0; i < parameters.Count; i++)
            {
                param = cmd.CreateParameter();
                param.ParameterName = parameters[i].ParameterName;
                param.Value = parameters[i].Value;
                param.DbType = parameters[i].DbType;//数据类型
                param.Size = parameters[i].Size;//长度
                param.Precision = parameters[i].Precision;//数据总账度
                param.Scale = parameters[i].Scale;//小数位数
                param.SourceColumn = parameters[i].SourceColumn;
                param.SourceVersion = parameters[i].SourceVersion;
                param.Direction = parameters[i].Direction;
                cmd.Parameters.Add(param);
            }
        }
        #endregion

    }
}