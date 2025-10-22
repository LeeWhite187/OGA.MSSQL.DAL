using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace OGA.MSSQL
{
    public enum eSQLRoles
    {
        /// <summary>
        /// Unassigned
        /// </summary>
        none,
        /// <summary>
        /// Database access administrators
        /// Is a database level role.
        /// </summary>
        db_accessadmin,
        /// <summary>
        /// Database backup operators
        /// Is a database level role.
        /// </summary>
        db_backupoperator,
        /// <summary>
        /// Database data readers
        /// Is a database level role.
        /// </summary>
        db_datareader,
        /// <summary>
        /// Database data writers
        /// Is a database level role.
        /// </summary>
        db_datawriter,
        /// <summary>
        /// Database DDL administrators
        /// </summary>
        db_ddladmin,
        /// <summary>
        /// Database deny data readers
        /// Is a database level role.
        /// </summary>
        db_denydatareader,
        /// <summary>
        /// Database deny data writers
        /// Is a database level role.
        /// </summary>
        db_denydatawriter,
        /// <summary>
        /// Database owners
        /// Is a database level role.
        /// </summary>
        db_owner,
        /// <summary>
        /// Database security administrators
        /// Ability to create logins / users
        /// Is a database level role.
        /// </summary>
        db_securityadmin,
        /// <summary>
        /// SQL Host System Administrator.
        /// Is server level role.
        /// Has all privileges.
        /// </summary>
        sysadmin,
        /// <summary>
        /// Is a server level role.
        /// </summary>
        diskadmin,
        /// <summary>
        /// Is a server level role.
        /// </summary>
        bulkadmin,
        /// <summary>
        /// Is a server level role.
        /// </summary>
        setupadmin,
        /// <summary>
        /// Allows a user to Run and monitor queries.
        /// Is a server level role.
        /// </summary>
        processadmin,
        /// <summary>
        /// Allows a user to configure server options.
        /// Is server level role.
        /// </summary>
        serveradmin,
        /// <summary>
        /// Server level role allowing a user to create/drop databases.
        /// </summary>
        dbcreator
    }
}
