using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OGA.MSSQL
{
    public enum eDBRoles
    {
        /// <summary>
        /// Unassigned
        /// </summary>
        none,
        /// <summary>
        /// Database access administrators
        /// </summary>
        db_accessadmin,
        /// <summary>
        /// Database backup operators
        /// </summary>
        db_backupoperator,
        /// <summary>
        /// Database data readers
        /// </summary>
        db_datareader,
        /// <summary>
        /// Database data writers
        /// </summary>
        db_datawriter,
        /// <summary>
        /// Database DDL administrators
        /// </summary>
        db_ddladmin,
        /// <summary>
        /// Database deny data readers
        /// </summary>
        db_denydatareader,
        /// <summary>
        /// Database deny data writers
        /// </summary>
        db_denydatawriter,
        /// <summary>
        /// Database owners
        /// </summary>
        db_owner,
        /// <summary>
        /// Database security administrators
        /// </summary>
        db_securityadmin
    }
}
