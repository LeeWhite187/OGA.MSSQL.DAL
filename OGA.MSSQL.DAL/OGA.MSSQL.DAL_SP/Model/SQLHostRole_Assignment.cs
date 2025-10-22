using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.MSSQL.Model
{
    public class SQLHostRole_Assignment
    {
        ///// <summary>
        ///// Server instance
        ///// </summary>
        //public string ServerName;
        /// <summary>
        /// Name of role assigned to login
        /// From sys.database_principals.name
        /// </summary>
        public string GroupName;
        /// <summary>
        /// Host User Name
        /// From sys.database_principals.name
        /// </summary>
        public string Login;
        /// <summary>
        /// Principal ID of the login
        /// From sys.server_principals.principal_id
        /// </summary>
        public string Principal_ID;
        /// <summary>
        /// Role SID
        /// From sys.server_principals.sid
        /// </summary>
        public string RoleSID;
    }
}
