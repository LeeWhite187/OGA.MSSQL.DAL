using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.MSSQL.Model
{
    public class DBRole_Assignment
    {
        ///// <summary>
        ///// Server instance that database resides on
        ///// </summary>
        //public string ServerName;
        /// <summary>
        /// Database of role assignments
        /// From DB_NAME() being Current Database Name.
        /// </summary>
        public string DBName;
        /// <summary>
        /// Name of role assigned to database user
        /// From sys.database_principals.name
        /// </summary>
        public string GroupName;
        /// <summary>
        /// Database User Name
        /// From sys.database_principals.name
        /// </summary>
        public string UserName;
        /// <summary>
        /// Login name associated with database user
        /// From sys.server_principals.name
        /// </summary>
        public string LoginName;
        ///// <summary>
        ///// Default Database assigned to the user
        ///// </summary>
        //public string Default_Database_Name;
        /// <summary>
        /// Default Schema assigned to the user
        /// From sys.database_principals.default_schema_name
        /// </summary>
        public string Default_Schema_Name;
        /// <summary>
        /// Principal ID of the user
        /// From sys.database_principals.principal_id
        /// </summary>
        public string Principal_ID;
        /// <summary>
        /// User SID
        /// From sys.database_principals.sid
        /// </summary>
        public string SID;
    }
}
