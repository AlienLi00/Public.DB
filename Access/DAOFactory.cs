namespace Public.DB
{
    public class DAOFactory
    {
        /// <summary>
        /// 根据参数建立对应数据库的DAO
        /// </summary>
        public static DAO CreateDAO(DatabaseProperty pp)
        {
            DAO dao;
            switch (pp.DatabaseType)
            {
                case DatabaseType.MSSQLServer:
                    dao = new MSSqlDAO(pp.ConnectionString);
                    break;

                case DatabaseType.Oracle:
                    dao = new OracleDAO(pp.ConnectionString);
                    break;

                case DatabaseType.MySql:
                    dao = new MySqlDAO(pp.ConnectionString);
                    break;

                case DatabaseType.PostgreSql:
                    dao = new PostgreDAO(pp.ConnectionString);
                    break;

                default:
                    dao = new MSSqlDAO(pp.ConnectionString);
                    break;
            }
            return dao;
        }
    }
}
