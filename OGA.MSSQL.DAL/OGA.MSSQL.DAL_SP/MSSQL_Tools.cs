using Microsoft.AspNetCore.ResponseCompression;
using Mono.Posix;
using Mono.Unix.Native;
using NLog.LayoutRenderers;
using OGA.MSSQL.DAL;
using OGA.MSSQL.DAL.CreateVerify.Model;
using OGA.MSSQL.DAL_SP.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace OGA.MSSQL
{
    /// <summary>
    /// Provides methods for managing databases, backup, restore, permissions, and users.
    /// </summary>
    public class MSSQL_Tools : IDisposable
    {
        #region Private Fields

        static private string _classname = nameof(MSSQL_Tools);

        static private volatile int _instancecounter;

        private OGA.MSSQL.MSSQL_DAL _master_dal;

        private Dictionary<string, MSSQL_DAL> _dbdals;

        private bool disposedValue;

        #endregion


        #region Public Properties

        public int InstanceId { get; set; }
        public string HostName { get; set; }
        public string Service { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        /// <summary>
        /// Set this flag if you want database connections to NOT pool in background.
        /// Generally, this should be off (false) in production.
        /// But, it is good to enable in testing, to ensure that connections properly close when expected, and are not pooled for reuse.
        /// </summary>
        public bool Cfg_ClearConnectionPoolOnClose { get; set; } = false;

        #endregion


        #region ctor / dtor

        public MSSQL_Tools()
        {
            _instancecounter++;
            InstanceId = _instancecounter;

            this._dbdals = new Dictionary<string, MSSQL_DAL>();
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                this.Close_MasterDAL();
                this.Close_DatabaseDALs();
                this._dbdals.Clear();

                // TODO: free unmanaged resources (unmanaged objects) and override finalizer
                // TODO: set large fields to null
                disposedValue = true;
            }
        }

        // // TODO: override finalizer only if 'Dispose(bool disposing)' has code to free unmanaged resources
        // ~cSQL_Tools()
        // {
        //     // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
        //     Dispose(disposing: false);
        // }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }

        #endregion


        #region Connectivity Methods

        /// <summary>
        /// Provides a quick ability to test credentials to a SQL Server instance, without creating a persistent connection.
        /// </summary>
        /// <returns></returns>
        public int TestConnection()
        {
            var result = 0;

            MSSQL_DAL? dal = null;
            try
            {
                dal = new MSSQL_DAL();
                dal.host = HostName;
                dal.service = Service;
                dal.database = "master";
                dal.username = Username;
                dal.password = Password;
                dal.Cfg_ClearConnectionPoolOnClose = this.Cfg_ClearConnectionPoolOnClose;

                result = dal.Test_Connection();
            }
            finally
            {
                try
                {
                    dal?.Disconnect();
                }
                catch (Exception) { }
                try
                {
                    dal?.Dispose();
                }
                catch (Exception) { }
            }

            return result;
        }

        /// <summary>
        /// Provides a quick ability to test credentials to a specific SQL Server database, without creating a persistent connection.
        /// </summary>
        /// <returns></returns>
        public int TestConnection_toDatabase(string database)
        {
            var result = 0;

            MSSQL_DAL? dal = null;
            try
            {
                dal = new MSSQL_DAL();
                dal.host = HostName;
                dal.service = Service;
                dal.database = database;
                dal.username = Username;
                dal.password = Password;
                dal.Cfg_ClearConnectionPoolOnClose = this.Cfg_ClearConnectionPoolOnClose;

                result = dal.Test_Connection();
            }
            finally
            {
                try
                {
                    dal?.Disconnect();
                }
                catch (Exception) { }
                try
                {
                    dal?.Dispose();
                }
                catch (Exception) { }
            }

            return result;
        }

        #endregion


        #region Engine Management

        /// <summary>
        /// Queries for the default data folder, where new databases are stored.
        /// NOTE: This method indicates the default folderpath for new databases, if no path was specified at creation.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="folderpath"></param>
        /// <returns></returns>
        public (int res, string? folderpath) Get_DefaultDataDirectory()
        {
            // Compose the sql query for the file locations...
            string sql = "SELECT SERVERPROPERTY('InstanceDefaultDataPath') AS folderpath;";

            var res = this.Get_Scalar_fromMaster(sql, "get data folder path");
            if (res.res != 1 || res.value == null)
                return (res.res, null);

            var folderpath = res.value ?? "";
            return (res.res, folderpath);
        }

        /// <summary>
        /// Queries for the default log folder, where new database log files are stored.
        /// NOTE: This method indicates the default folderpath for new databases, if no path was specified at creation.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="folderpath"></param>
        /// <returns></returns>
        public (int res, string? folderpath) Get_DefaultLogDirectory()
        {
            // Compose the sql query for the file locations...
            string sql = "SELECT SERVERPROPERTY('InstanceDefaultLogPath')  AS folderpath;";

            var res = this.Get_Scalar_fromMaster(sql, "get log folder path");
            if (res.res != 1 || res.value == null)
                return (res.res, null);

            var folderpath = res.value ?? "";
            return (res.res, folderpath);
        }

        /// <summary>
        /// Attempts to query the number of active connections to a database.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public (int res, int count) GetConnectionCountforDatabase(string database)
        {
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetConnectionCountforDatabase)} - " +
                    $"Attempting to get connection count to database ({(database ?? "")})...");

                if (string.IsNullOrWhiteSpace(database))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetConnectionCountforDatabase)} - " +
                        $"Empty database name.");

                    return (-1, -1);
                }

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetConnectionCountforDatabase)} - " +
                        $"Failed to connect to server.");

                    return (-1, -1);
                }

                // Get connection count to each database...
                string sql = @$"SELECT ActiveConnections
                               FROM (
		                               SELECT
			                               COALESCE(DB_NAME(database_id), 'master') AS DatabaseName,
			                               COUNT(*) AS ActiveConnections
		                               FROM sys.dm_exec_sessions
		                               GROUP BY COALESCE(DB_NAME(database_id), 'master')
	                               ) b
                               where b.DatabaseName = '{database}';";
                var resconn = this.Get_Scalar_fromMaster(sql, "Get Connection Count");
                if (resconn.res == 0)
                {
                    // Database not present.
                    // We will return a zero for this.

                    return (1, 0);
                }
                if (resconn.res != 1)
                {
                    // Failed to get connection count.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetConnectionCountforDatabase)} - " +
                        "Failed to get connection count.");

                    return (-2, -1);
                }

                if(!int.TryParse(resconn.value, out var count))
                {
                    // Failed to get connection count.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetConnectionCountforDatabase)} - " +
                        "Failed to get connection count.");

                    return (-2, -1);
                }

                return (1, count);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetConnectionCountforDatabase)} - " +
                    "Exception occurred while changing database owner.");

                return (-20, -1);
            }
            finally
            {
            }
        }

        #endregion


        #region Bulk Queries

        /// <summary>
        /// Executes a query using SQLCMD.exe, sending the output to the desired CSV filepath.
        /// </summary>
        /// <param name="sqlhost"></param>
        /// <param name="query"></param>
        /// <param name="filepath"></param>
        /// <returns></returns>
        public int Execute_SQLCMD_Query_to_CSV(string query, string filepath)
        {
            return Execute_SQLCMD_Query_to_CSV(query, filepath, false);
        }
        /// <summary>
        /// Executes a query using SQLCMD.exe, sending the output to the desired CSV filepath.
        /// Set using_trusted security to true if using windows user that runs SQLCMD. Set using_trusted security to false if using explicit user and password set in this instance.
        /// </summary>
        /// <param name="sqlhost"></param>
        /// <param name="query"></param>
        /// <param name="filepath"></param>
        /// <param name="using_trusted_Security"></param>
        /// <returns></returns>
        public int Execute_SQLCMD_Query_to_CSV(string query, string filepath, bool using_trusted_Security)
        {
            // Build a command of the form:
            // SQLCMD -S localhost -U <username> -P <password> -Q "SELECT * FROM [PKG_Bottle_Data].[dbo].[tbl_Color_InspData]" -s "," -o "D:\Color.csv"
            // Generate the output file path.

            // Information source:
            // https://docs.microsoft.com/en-us/previous-versions/sql/sql-server-2012/ms162773(v=sql.110)?redirectedfrom=MSDN

            string hostservice = Get_FullyQualified_SQLHostName();

            string authentication_argument = "";
            if (using_trusted_Security)
                authentication_argument = "-E";
            else
                authentication_argument = "-U \"" + this.Username + "\" -P \"" + this.Password + "\"";

            string args = "-S \"" + hostservice + "\" " +
                          authentication_argument + " " +
                          "-Q \"" + query + "\" " +
                          "-s \",\" " +
                          "-o \"" + filepath + "\"";

            // Attempt to get the path to the SQLCMD executable that we will leverage for exporting data.
            if (Locate_SQLCMDexe_in_FileSystem(out var toolpath) != 1)
            {
                // Failed to locate the SQLCMD exe path.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Failed to locate the SQLCMD executable. It might not be installed, or the toolpath is not in the usual spots, or it's SQL Server 2018 which does not include SQLCMD.",
                    _classname);

                return -999;
            }
            // We have the SQLCMD executable path.

            string stdout = "";
            string stderr = "";
            int exitcode = -10;
            int res = OGA.Common.Process.cProcess_Helper.Run_Process(toolpath, args, ref stdout, ref stderr, ref exitcode);

            OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                "SQLCMD exeuted query with the following results:\r\n" +
                "sqlhost = " + hostservice + "\r\n" +
                "query = " + query + "\r\n" +
                "filepath = " + filepath + "\r\n" +
                "stdout = " + stdout + "\r\n" +
                "exitcode = " + exitcode.ToString() + "\r\n" +
                "stderr = " + stderr,
                _classname);

            return 1;
        }

        #endregion


        #region Database Management

        /// <summary>
        /// Queries for the owner of the given database.
        /// NOTE: In SQL Server, ownership doesn't equal authority.
        ///     Meaning, just because a user is a database owner, if they don't have explicit privileges to the database, they may not even be able to connect to it.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public (int res, string? owner) Get_DatabaseOwner(string database)
        {
            // Compose the sql query for the file locations...
            string sql = $"SELECT suser_sname(owner_sid) AS OwnerLogin " +
                         $"FROM sys.databases " +
                         $"WHERE name = '" + database + "';";

            var res = this.Get_Scalar_fromMaster(sql, "get database owner");
            if (res.res != 1 || res.value == null)
                return (res.res, null);

            var owner = res.value ?? "";
            return (res.res, owner);
        }

        /// <summary>
        /// Performs an alter database to transfer ownership.
        /// NOTE: In SQL Server, ownership doesn't equal authority.
        ///     Meaning, just because a user is a database owner, if they don't have explicit privileges to the database, they may not even be able to connect to it.
        /// SO: This method can be used to assin ownership.
        /// But, any privileges must be assigned by adding the user to the database, and giving them a role in it.
        /// Returns 1 for success, 0 if database or user not found, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="newowner"></param>
        /// <returns></returns>
        public int ChangeDatabaseOwner(string database, string newowner)
        {
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                    $"Attempting to get change owner...");

                if (string.IsNullOrWhiteSpace(database))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        $"Empty database name.");

                    return -1;
                }
                if (string.IsNullOrWhiteSpace(newowner))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        $"Empty newowner name.");

                    return -1;
                }

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Verify the database exists...
                if (this.Does_Database_Exist(database) != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        $"Database ({(database ?? "")}) not found.");

                    return 0;
                }
                // Verify the user exists...
                if (this.Does_Login_Exist(newowner) != 1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        $"User ({(newowner ?? "")}) not found.");

                    return 0;
                }

                // Transfer ownership to the new user...
                string sql = $"ALTER AUTHORIZATION ON DATABASE::{database} TO [{newowner}];";
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to change database owner.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        "Failed to change database owner.");

                    return -2;
                }
                // We executed the alter database command.

                // Verify the owner changed...
                var resver = this.Get_DatabaseOwner(database);
                if (resver.res != 1 || string.IsNullOrEmpty(resver.owner))
                {
                    // Failed to query database owner.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        "Failed to query database owner.");

                    return -3;
                }

                if (resver.owner != newowner)
                {
                    // Failed to update database owner.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                        "Failed to update database owner.");

                    return -4;
                }
                // If here, the database owner was changed, and verified to be updated.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeDatabaseOwner)} - " +
                    "Exception occurred while changing database owner.");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Returns 1 if database was found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Does_Database_Exist(string database)
        {
            object tempval = "";

            try
            {
                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Does_Database_Exist)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Create the sql query text.
                string sql = "SELECT database_id FROM sys.databases WHERE Name = '" + database + "'";

                // Make the call to get a databaseID if the database exists.
                if (_master_dal.Execute_Scalar(sql, System.Data.CommandType.Text, out tempval) != 1)
                {
                    // Error occurred while checking to see if the database exists.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Error occurred while checking to see if the database exists."
                            , _classname);

                    return -2;
                }

                // See if we found the database.
                int val = 0;
                try
                {
                    if (tempval == null)
                    {
                        // The received database id is null.
                        // Meaning, no database id was found for the given database name.
                        return 0;
                    }

                    // Attempt to recover the databaseId from the scalar call.
                    int.TryParse(tempval.ToString(), out val);

                    // If here, we recovered an integer.
                    // We will see if it's a database ID.

                    if (val > 0)
                    {
                        // Database was found.
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }
                }
                catch (Exception e)
                {
                    // If here, no database was found.
                    return 0;
                }
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
            finally
            {
                try
                {
                    if (tempval != null && tempval is IDisposable)
                    {
                        ((IDisposable)tempval)?.Dispose();
                    }
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Creates a database with the given name.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Create_Database(string database)
        {
            return Create_Database(database, "");
        }
        /// <summary>
        /// Creates a database with the given name.
        /// Accepts a folder path where the database files will be stored.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="backendfilefolder"></param>
        /// <returns></returns>
        public int Create_Database(string database, string backendfilefolder)
        {
            string sql = "";

            try
            {
                // Check that the database name was give.
                if (String.IsNullOrWhiteSpace(database))
                {
                    // database name not set.
                    return -1;
                }
                if (database == "")
                {
                    // database name not set.
                    return -1;
                }
                // Database name is set.

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Database)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // See if the database doesn't already exist.
                if (this.Does_Database_Exist(database) == 1)
                {
                    // The database already exists.
                    // We cannot create it again.
                    return -2;
                }

                // Formulate the backend file paths.
                if (String.IsNullOrWhiteSpace(backendfilefolder))
                {
                    // The backend folder is not set.
                    // Use defaults.
                    sql = "CREATE DATABASE [" + database + "];";
                }
                else if (backendfilefolder == "")
                {
                    // The backend folder is not set.
                    // Use defaults.
                    sql = "CREATE DATABASE [" + database + "];";
                }
                else
                {
                    // The backend folder was specified.

                    // Confirm the folder is valid.
                    if (!System.IO.Directory.Exists(backendfilefolder))
                    {
                        // Folder does not exist.
                        return -3;
                    }
                    // Folder exists.

                    // Creat the database filepath.
                    string dbfilepath = System.IO.Path.Combine(backendfilefolder, database + ".mdf");
                    // Create the log path.
                    string logfilepath = System.IO.Path.Combine(backendfilefolder, database + "_log.ldf");

                    // Create the sql statement that includes the backend data files.
                    sql = "CREATE DATABASE[" + database + "] " +
                        "ON(NAME = N'" + database + "', FILENAME = N'" + dbfilepath + "', SIZE = 1024MB, FILEGROWTH = 256MB) " +
                        "LOG ON(NAME = N'" + database + "_log', FILENAME = N'" + logfilepath + "', SIZE = 512MB, FILEGROWTH = 125MB);";
                }
                // We have the sql script to run.

                // Execute it on the sql server instance.
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while adding the database to the SQL server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Error occurred while adding the database to the SQL server."
                            , _classname);

                    return -4;
                }

                // Check if the database is now present on the server.
                if (this.Does_Database_Exist(database) != 1)
                {
                    // The database was not created successfully.
                    return -5;
                }
                // If here, the database was added.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
        }

        /// <summary>
        /// Drops connections to the given database, setting it to single user mode.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Set_Database_toSingleUser(string database)
        {
            try
            {
                // Check that the database name was give.
                if (String.IsNullOrWhiteSpace(database))
                {
                    // database name not set.
                    return -1;
                }
                if (database == "")
                {
                    // database name not set.
                    return -1;
                }
                // Database name is set.

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Set_Database_toSingleUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // See if the database exists.
                int res = this.Does_Database_Exist(database);
                if (res == 0)
                {
                    // The database doesn't exist.
                    return 0;
                }
                else if (res < 0)
                {
                    // Failed to connect.
                    return -2;
                }
                // The database exists.
                // We will attempt to convert it to single user.

                string sql = "ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;";
                // We have the sql script to run.

                // Execute it on the sql server instance.
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while converting the database to single user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while converting the database to single user."
                        , _classname);

                    return -4;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
        }

        /// <summary>
        /// Set the given database to multiuser mode.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Set_Database_toMultiUser(string database)
        {
            try
            {
                // Check that the database name was give.
                if (String.IsNullOrWhiteSpace(database))
                {
                    // database name not set.
                    return -1;
                }
                if (database == "")
                {
                    // database name not set.
                    return -1;
                }
                // Database name is set.

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Set_Database_toSingleUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // See if the database exists.
                int res = this.Does_Database_Exist(database);
                if (res == 0)
                {
                    // The database doesn't exist.
                    return 0;
                }
                else if (res < 0)
                {
                    // Failed to connect.
                    return -2;
                }
                // The database exists.
                // We will attempt to convert it to single user.

                string sql = "ALTER DATABASE [" + database + "] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;";
                // We have the sql script to run.

                // Execute it on the sql server instance.
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while converting the database to single user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while converting the database to single user."
                        , _classname);

                    return -4;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
        }

        /// <summary>
        /// Queries for what access mode a given database is in.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public (int res, eAccessMode accessmode) Get_Database_AccessMode(string database)
        {
            // Compose the sql query for the database access mode...
            string sql = $"SELECT user_access_desc FROM sys.databases WHERE name = '{database}';";

            var res = this.Get_Scalar_fromMaster(sql, "get database access mode");
            if (res.res != 1 || res.value == null)
                return (res.res, eAccessMode.Unknown);

            var accessmode = res.value ?? "";

            if (accessmode == "SINGLE_USER")
                return (1, eAccessMode.SingleUser);
            else if (accessmode == "MULTI_USER")
                return (1, eAccessMode.MultiUser);
            else if (accessmode == "RESTRICTED_USER")
                return (1, eAccessMode.RestrictedUser);
            else
                return (-1, eAccessMode.Unknown);
        }

        /// <summary>
        /// Drops the given database from the SQL Server instance.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Drop_Database(string database)
        {
            try
            {
                // Check that the database name was give.
                if (String.IsNullOrWhiteSpace(database))
                {
                    // database name not set.
                    return -1;
                }
                if (database == "")
                {
                    // database name not set.
                    return -1;
                }
                // Database name is set.

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Database)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // See if the database exists.
                int res = this.Does_Database_Exist(database);
                if (res == 0)
                {
                    // The database doesn't exist.
                    return 0;
                }
                else if (res < 0)
                {
                    // Failed to connect.
                    return -2;
                }
                // The database exists.
                // We will attempt to drop it.

                // Catalog any backend files in case one is offlined that we need to cleanup.
                // Get the backend files associated with the database...
                var resp = Get_Backend_Filepaths_for_Database(database);
                if(resp.res != 1 || resp.filepaths == null)
                {
                    // Failed to get backend filepaths for database.
                    return -3;
                }
                // We have the backend filepaths.

                var paths = resp.filepaths;

                string sql = "DROP DATABASE " + database + ";";
                // We have the sql script to run.

                // Execute it on the sql server instance.
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while dropping the database from the SQL server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while dropping the database from the SQL server."
                        , _classname);

                    return -4;
                }

                // Check if the database is still present on the server.
                if (this.Does_Database_Exist(database) == 1)
                {
                    // The database was not deleted successfully.
                    return -5;
                }
                // If here, the database was deleted.

                // Cleanup any backend files that were offline.
                try
                {
                    // Make sure the shell is enabled...
                    var resenable = this.SQLEngine_EnableCmdShell();
                    if (resenable != 1)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Error occurred while enabling the command shell on the SQL server."
                            , _classname);
                    }

                    // Remove each file from the server.
                    foreach (var s in paths)
                    {
                        try
                        {
                            var resdel = this.SQLEngine_DeleteFile(s);
                            if (resdel != 1)
                            {
                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                                "Error occurred while deleting a file on the SQL server."
                                    , _classname);
                            }
                        }
                        catch (Exception e)
                        {
                        }
                    }
                }
                finally
                {
                    // Disable command shell access...
                    var resdisable = this.SQLEngine_DisableCmdShell();
                    if (resdisable != 1)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Error occurred while disabling the command shell on the SQL server."
                            , _classname);
                    }
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
        }

        /// <summary>
        /// Retrieves the list of databases on the given SQL host.
        /// Returns 1 for success, negatives for errors.
        /// </summary>
        /// <param name="dblist"></param>
        /// <returns></returns>
        public int Get_DatabaseList(out List<string> dblist)
        {
            System.Data.DataTable dt = null;
            dblist = new List<string>();

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseList)} - " +
                    $"Attempting to get database names...");

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseList)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query we will perform...
                string sql = "SELECT name AS DatabaseName FROM sys.databases ORDER BY name;";

                if (_master_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get database names from the host.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseList)} - " +
                        "Failed to get database names from the host.");

                    return -2;
                }
                // We have a table of database names.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // The catalog will always show up in this query.
                    // So, if we have no entries something is wrong.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseList)} - " +
                        "Failed to get database names from the host.");

                    return -1;
                }
                // If here, we have database names.

                foreach (System.Data.DataRow r in dt.Rows)
                {
                    string sss = r[0] + "";
                    dblist.Add(sss);
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseList)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Performs a backup of the given database to the given filepath.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public int Backup_Database(string databaseName, string filePath)
        {
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to backup database to file...\r\n:" +
                    "Database = {1};\r\n" +
                    "BackupFile = {2};\r\n",
                    _classname,
                    databaseName,
                    filePath);

                string sql = "Backup database [" + databaseName + "] to disk='" + filePath + "'";

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Backup_Database)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // We have a persistent connection.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Making the call to backup the database.",
                    _classname);
                // NOTE: THIS CALL SHOULD RETURN -1 FOR SUCCESS, STRAIGHT FROM SQL.
                if (_master_dal.Execute_NonQuery(sql, 1700).res != -1)
                {
                    // Failed to backup database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to backup database.",
                        _classname);

                    return -2;
                }
                // Database was backed up.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Backup finished.",
                    _classname);

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, databaseName);

                return -20;
            }
        }

        /// <summary>
        /// Restores a database backup, given by filepath, to a target database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="filepath"></param>
        /// <returns></returns>
        public int Restore_Database(string database, string filepath)
        {
            string sql = "";

            try
            {
                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Restore_Database)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // We have a persistent connection.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to restore database {1} from file {2}..."
                        , _classname, database, filepath);

                // Check if the database exists or not.
                int res2 = this.Does_Database_Exist(database);
                if (res2 < 0)
                {
                    // Error occurred while checking for database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Error occurred while checking for database."
                            , _classname);

                    return -16;
                }
                else if (res2 == 1)
                {
                    // The database exists.
                    // This restore will overwrite data in the database.
                    // So, we need to take it to single user mode.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                        "Changing database to single user mode..."
                            , _classname);
                    sql = "ALTER DATABASE " + database + " SET SINGLE_USER WITH ROLLBACK IMMEDIATE;";
                    // NOTE: THIS CALL SHOULD RETURN -1 FOR SUCCESS, STRAIGHT FROM SQL.
                    if (_master_dal.Execute_NonQuery(sql).res != -1)
                    {
                        // Failed to switch to the master database.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                            "Failed to switch to the master database."
                                , _classname);

                        return -2;
                    }
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                        "Database changed to single user mode."
                            , _classname);
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Restoring database {1} from disk at {2}..."
                        , _classname, database, filepath);
                sql = "RESTORE DATABASE " + database + " FROM DISK = '" + filepath + "';";
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to switch to the master database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to switch to the master database."
                            , _classname);

                    return -3;
                }
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Database restored."
                        , _classname, database, filepath);

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Changing database to multi user mode..."
                        , _classname);
                // NOTE: THIS CALL SHOULD RETURN -1 FOR SUCCESS, STRAIGHT FROM SQL.
                sql = "ALTER DATABASE " + database + " SET MULTI_USER;";
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to switch to the master database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to switch to change the database to multi user mode."
                            , _classname);

                    return -5;
                }
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Database changed to multi user mode."
                        , _classname);

                return 1;
            }
            catch (System.Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred while attempting to restore the database."
                        , _classname);

                return -10;
            }
        }

        /// <summary>
        /// Retrieves a list of filepaths of the database log files, etc, for the given database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="filepaths"></param>
        /// <returns></returns>
        public (int res, List<string>? filepaths) Get_Backend_Filepaths_for_Database(string database)
        {
            System.Data.DataTable dt = null;
            List<string> filepaths;

            // Check that the database name was give.
            if (String.IsNullOrWhiteSpace(database))
            {
                // database name not set.
                return (-1, null);
            }
            if (database == "")
            {
                // database name not set.
                return (-1, null);
            }
            // Database name is set.

            try
            {
                string sql = "SELECT [name], [physical_name] FROM sys.master_files " +
                               "WHERE database_id = (SELECT [database_id] FROM [master].[sys].[databases] WHERE [name] = '" + database + "')";

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get backend filepaths for database {1}..."
                        , _classname, database);

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_Backend_Filepaths_for_Database)} - " +
                        $"Failed to connect to server.");

                    return (-1, null);
                }

                if (_master_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get row counts from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get filepaths from the database."
                            , _classname);

                    return (-2, null);
                }
                // We have a datatable of row counts.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // The database has no backend files.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "No backend files found for database {1}. Database name might be wrong."
                            , _classname, database);

                    return (-3, null);
                }
                // If here, we have filepaths for the database.

                filepaths = new List<string>();

                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the current filepath.
                    string filepath = r["physical_name"].ToString() + "";
                    filepaths.Add(filepath);
                }
                // If here, we have iterated all rows, and can return to the caller.

                return (1, filepaths);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return (-20, null);
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        #endregion


        #region User Management

        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int Does_Login_Exist(string login)
        {
            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                    $"Attempting to check for login to SQL Host...");

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Does_Login_Exist)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to get logins from the sql server instance.
                string sql = "SELECT " +
                             "sp.[name] AS [login], " +
                             "sp.[type_desc] AS [login_type], " +
                             "sp.[default_database_name], " +
                             "sp.[type], " +
                             "sp.[create_date], " +
                             "sp.[modify_date], " +
                             "CASE " +
                             "WHEN sp.[is_disabled] = 1 THEN 'Disabled' " +
                             "ELSE 'Enabled' " +
                             "END AS [status] " +
                             "FROM sys.server_principals sp " +
                             "LEFT JOIN sys.sql_logins sl " +
                             "ON sp.[principal_id] = sl.[principal_id] " +
                             "WHERE sp.[type] NOT IN ('C', 'R') " +
                             "AND sp.[name] NOT LIKE '##%'";

                if (_master_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get logins from the sql server instance.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                        "Failed to get logins from the sql server instance.");

                    return -2;
                }
                // We have a datatable of sql server logins.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No logins on the server.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                        "Did not get any logins. Something is wrong.");

                    return -3;
                }
                // If here, we have logins.

                // See if we have a match to the given login.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the current login...
                    string sss = r["login"] + "";

                    // See if we have a match...
                    // NOTE: SQL Server might not be case-sensitive.
                    if (sss.ToUpper() == login.ToUpper())
                    {
                        // Got a match.

                        return 1;
                    }
                }
                // If here, we didn't find a match.

                return 0;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                    $"Exception occurred to SQL Host.");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Returns 1 on success, 0 if already present, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int Add_WindowsLogin(string login)
        {
            // Check that the user is in the logins list already.
            int res = this.Does_Login_Exist(login);
            if (res < 0)
            {
                // Error occurred.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                    "Error connecting to the sql server instance.");

                return -1;
            }
            else if (res == 1)
            {
                // User is in the database.
                return 0;
            }
            // User not present.

            // We will add it.

            try
            {
                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Add_WindowsLogin)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to get logins from the sql server instance.
                string sql = "CREATE LOGIN [" + login + "] FROM WINDOWS WITH DEFAULT_DATABASE =[master], DEFAULT_LANGUAGE =[us_english]";

                var resadd = _master_dal.Execute_NonQuery(sql);
                if (resadd.res != 1)
                {
                    // Failed to add Windows logins to the sql server instance.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                        "Failed to add login to sql server instance.");

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Returns 1 on success, 0 if already present, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <param name="password">Account password</param>
        /// <returns></returns>
        public int Add_LocalLogin(string login, string password)
        {
            // Check that the user is in the logins list already.
            int res = this.Does_Login_Exist(login);
            if (res < 0)
            {
                // Error occurred.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:-:{nameof(Add_LocalLogin)} - " +
                    "Error connecting to the sql server instance.");

                return -1;
            }
            else if (res == 1)
            {
                // User is in the database.
                return 0;
            }
            // User not present.

            // We will add it.

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Add_LocalLogin)} - " +
                    $"Attempting to add login to SQL Host...");

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Add_LocalLogin)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to add a login to the sql server instance.
                string sql = "CREATE LOGIN [" + login + "] WITH PASSWORD = '" + password + "', DEFAULT_DATABASE = [master], DEFAULT_LANGUAGE = [us_english];";

                var resadd = _master_dal.Execute_NonQuery(sql);
                if (resadd.res != -1)
                {
                    // Failed to get logins from the sql server instance.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Add_LocalLogin)} - " +
                        "Failed to add login to sql server instance.");

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Add_LocalLogin)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Public call to delete a user account from the SQL host.
        /// NOTE: This call works with 'Login' as server-level principles, not with users in a database.
        /// NOTE: If you want to remove a user from a database, call DeleteUserfromDatabase().
        /// Confirms the user was deleted.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <returns></returns>
        public int DeleteLogin(string username)
        {
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                    $"Attempting to delete user...");

                if (string.IsNullOrWhiteSpace(username))
                {
                    // Empty username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                        $"Empty Username.");

                    return -1;
                }

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Delete the login...
                string sql = $"IF EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'{username}') DROP LOGIN [{username}];";

                if (this._master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Delete login command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                        "Delete command failed.");

                    return -2;
                }

                // Check if the login was deleted...
                var resq = this.Does_Login_Exist(username);
                if (resq != 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                        "Login was not confirmed as dropped.");

                    return -3;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                    $"Exception occurred to SQL Host.");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="username">User string</param>
        /// <param name="database">Target database</param>
        /// <returns></returns>
        public int Does_User_Exist_forDatabase(string username, string database)
        {
            try
            {
                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Does_User_Exist_forDatabase)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql query to check if the user is present in the target database.
                string sql = $"SELECT " +
                                $"    CASE WHEN EXISTS " +
                                $"    ( " +
                                $"        SELECT 1 " +
                                $"        FROM sys.database_principals AS dp " +
                                $"        JOIN sys.server_principals AS sp ON dp.sid = sp.sid " +
                                $"        WHERE sp.name = '{username}' " +
                                $"    ) " +
                                $"    THEN 1 ELSE 0 END AS IsUserInDatabase;";

                // The above statement will return 0 or 1, depending on user presence.
                if (dbdal.Execute_Scalar(sql, System.Data.CommandType.Text, out var raw) != 1)
                {
                    // Failed to identify the user presence in the target database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Does_User_Exist_forDatabase)} - " +
                        "Failed to identify the user presence in the target database.");

                    return -2;
                }

                if (raw == null)
                {
                    // Not found.
                    return 0;
                }

                // See if we found the user.
                int val = (int)raw;

                if (val > 0)
                {
                    // User exists in target database.
                    return 1;
                }
                else
                {
                    // User not present in target database.
                    return 0;
                }
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Does_User_Exist_forDatabase)} - " +
                    $"Exception occurred to database: {(database ?? "")}");

                return -20;
            }
        }

        /// <summary>
        /// Adds a user to a particular database.
        /// If desiredroles is null, we simply add the user to the database, with no change in roles.
        /// Returns 1 on success, 0 if already set, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <param name="database">Name of database</param>
        /// <param name="desiredroles">List of desired roles to add to user</param>
        /// <returns></returns>
        public int Add_User_to_Database(string login, string database, List<eDBRoles>? desiredroles = null)
        {
            try
            {
                // Check that the login is is in the logins list already, on the SQL Host.
                int res = this.Does_Login_Exist(login);
                if (res != 1)
                {
                    // Error occurred.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Login not found found."
                            , _classname);

                    return -1;
                }
                // Login exists as a server-principle.
                // We need to, as well, check that the user is a user in the database.

                // see if the login is also a user in the target database...
                var resexists = this.Does_User_Exist_forDatabase(login, database);
                if (resexists == 0)
                {
                    // User not found in database.

                    // We need to add the user to the target database.
                    if (this.priv_AddUsertoDatabase(login, database) != 1)
                    {
                        // Failed to add user to database.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                            "Failed to add user to database."
                                , _classname);

                        return -2;
                    }
                }
                else if (resexists == 1)
                {
                    // User exists in database.
                }
                else
                {
                    // An error occurred while attempting to check if the user is in the target database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to check user presence in database."
                            , _classname);

                    return -2;
                }
                // If here, the user is present in the server and the database.

                // See if we are to affect the user's roles in the database...
                if(desiredroles == null)
                {
                    // The desired roles is null.
                    // This is different from it being empty.
                    // We will regard this as a method overload to simply add the user to the database.
                    // Not to affect the user's roles in the database.

                    return 1;
                }
                // The desired roles is not null.
                // We will reconcile user roles to match desired.

                // Get a list of roles that the user currently has...
                if (this.Get_DBRoles_for_Login(login, database, out var foundroles) != 1)
                {
                    // Failed to get roles for the user.
                    return -2;
                }
                // We have current roles for the user.

                // Reconcile the found roles with desired...
                // From the given set of roles to expect for the user, and the current roles for the user, we need to determine the list of ones to add and one to remove.
                var pcl = DetermineDatabaseRoleChanges(foundroles, desiredroles);
                if(pcl.Count == 0)
                {
                    // No changes to make.
                    return 1;
                }

                string sql = "";
                string sqladd = "";
                string sqlremove = "";

                // Iterate the role change list, and compose the set of SQL instructions to execute...
                foreach(var r in pcl)
                {
                    if(r.isgrant)
                    {
                        // Role to add.
                        sqladd = sqladd + $"ALTER ROLE [{r.ToString()}] ADD MEMBER [{login}];";
                    }
                    else
                    {
                        // Role to remove.
                        sqlremove = sqlremove + $"ALTER ROLE [{r.ToString()}] DROP MEMBER [{login}];";
                    }
                }

                // Create a single sql command to run...
                if(!string.IsNullOrWhiteSpace(sqladd))
                    sql = sql + sqladd;
                if(!string.IsNullOrWhiteSpace(sqlremove))
                    sql = sql + sqlremove;

                // At this point, we have a list of roles to add.

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Add_User_to_Database)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }
                // We will add each one to the user.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to add database roles for login..."
                        , _classname, database);

                // Execute the add and drop role commands, to update the user's membership with desired...
                if (dbdal.Execute_NonQuery(sql).res != 1)
                {
                    // Failed to update database roles to the sql server instance.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to update database roles to the sql server instance."
                            , _classname);

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname, database);

                return -20;
            }
        }

        /// <summary>
        /// Determines the net changes between two given sets of database roles.
        /// Used by logic that updates user database roles as required.
        /// </summary>
        /// <param name="existingprivs"></param>
        /// <param name="desiredprivs"></param>
        /// <returns></returns>
        static public List<(bool isgrant, eDBRoles sfsd)> DetermineDatabaseRoleChanges(List<eDBRoles> existingprivs, List<eDBRoles> desiredprivs)
        {
            var pcl = new List<(bool isgrant, eDBRoles priv)>();

            // Figure out desiredprivs to add...
            if (desiredprivs.Contains(eDBRoles.db_accessadmin) && !existingprivs.Contains(eDBRoles.db_accessadmin))
                pcl.Add((true, eDBRoles.db_accessadmin));
            if (desiredprivs.Contains(eDBRoles.db_backupoperator) && !existingprivs.Contains(eDBRoles.db_backupoperator))
                pcl.Add((true, eDBRoles.db_backupoperator));
            if (desiredprivs.Contains(eDBRoles.db_datareader) && !existingprivs.Contains(eDBRoles.db_datareader))
                pcl.Add((true, eDBRoles.db_datareader));
            if (desiredprivs.Contains(eDBRoles.db_datawriter) && !existingprivs.Contains(eDBRoles.db_datawriter))
                pcl.Add((true, eDBRoles.db_datawriter));
            if (desiredprivs.Contains(eDBRoles.db_ddladmin) && !existingprivs.Contains(eDBRoles.db_ddladmin))
                pcl.Add((true, eDBRoles.db_ddladmin));
            if (desiredprivs.Contains(eDBRoles.db_denydatareader) && !existingprivs.Contains(eDBRoles.db_denydatareader))
                pcl.Add((true, eDBRoles.db_denydatareader));
            if (desiredprivs.Contains(eDBRoles.db_denydatawriter) && !existingprivs.Contains(eDBRoles.db_denydatawriter))
                pcl.Add((true, eDBRoles.db_denydatawriter));
            if (desiredprivs.Contains(eDBRoles.db_owner) && !existingprivs.Contains(eDBRoles.db_owner))
                pcl.Add((true, eDBRoles.db_owner));
            if (desiredprivs.Contains(eDBRoles.db_securityadmin) && !existingprivs.Contains(eDBRoles.db_securityadmin))
                pcl.Add((true, eDBRoles.db_securityadmin));

            // Figure out privileges to remove...
            if (!desiredprivs.Contains(eDBRoles.db_accessadmin) && existingprivs.Contains(eDBRoles.db_accessadmin))
                pcl.Add((false, eDBRoles.db_accessadmin));
            if (!desiredprivs.Contains(eDBRoles.db_backupoperator) && existingprivs.Contains(eDBRoles.db_backupoperator))
                pcl.Add((false, eDBRoles.db_backupoperator));
            if (!desiredprivs.Contains(eDBRoles.db_datareader) && existingprivs.Contains(eDBRoles.db_datareader))
                pcl.Add((false, eDBRoles.db_datareader));
            if (!desiredprivs.Contains(eDBRoles.db_datawriter) && existingprivs.Contains(eDBRoles.db_datawriter))
                pcl.Add((false, eDBRoles.db_datawriter));
            if (!desiredprivs.Contains(eDBRoles.db_ddladmin) && existingprivs.Contains(eDBRoles.db_ddladmin))
                pcl.Add((false, eDBRoles.db_ddladmin));
            if (!desiredprivs.Contains(eDBRoles.db_denydatareader) && existingprivs.Contains(eDBRoles.db_denydatareader))
                pcl.Add((false, eDBRoles.db_denydatareader));
            if (!desiredprivs.Contains(eDBRoles.db_denydatawriter) && existingprivs.Contains(eDBRoles.db_denydatawriter))
                pcl.Add((false, eDBRoles.db_denydatawriter));
            if (!desiredprivs.Contains(eDBRoles.db_owner) && existingprivs.Contains(eDBRoles.db_owner))
                pcl.Add((false, eDBRoles.db_owner));
            if (!desiredprivs.Contains(eDBRoles.db_securityadmin) && existingprivs.Contains(eDBRoles.db_securityadmin))
                pcl.Add((false, eDBRoles.db_securityadmin));

            return pcl;
        }

        /// <summary>
        /// Public call to delete a database user.
        /// NOTE: This call works with 'User' as a database-level user, not with Server-level logins.
        /// NOTE: If you want to remove a login from the SQL host, call DeleteLogin().
        /// Confirms the user was deleted.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public int DeleteUserfromDatabase(string username, string database)
        {
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUserfromDatabase)} - " +
                    $"Attempting to delete user...");

                if (string.IsNullOrWhiteSpace(username))
                {
                    // Empty username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUserfromDatabase)} - " +
                        $"Empty Username.");

                    return -1;
                }

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUserfromDatabase)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Delete the user...
                string sql = $"DROP USER IF EXISTS {username};";
                if (dbdal.Execute_NonQuery(sql).res != -1)
                {
                    // Delete user command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUserfromDatabase)} - " +
                        "Delete user command failed.");

                    return -2;
                }

                // Check if the user was deleted...
                var resq = this.Does_User_Exist_forDatabase(username, database);
                if (resq != 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUserfromDatabase)} - " +
                        "User was not confirmed as dropped.");

                    return -3;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUserfromDatabase)} - " +
                    $"Exception occurred to database: {(database ?? "")}");

                return -20;
            }
            finally
            {
            }
        }

        /// <summary>
        /// Public call to change a user password.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public int ChangeLoginPassword(string username, string password = "")
        {
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeLoginPassword)} - " +
                    $"Attempting to create user...");

                if (string.IsNullOrWhiteSpace(username))
                {
                    // Empty Username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeLoginPassword)} - " +
                        $"Empty Username.");

                    return -1;
                }

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeLoginPassword)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Change the user password...
                string sql = $"ALTER LOGIN [{username}] WITH PASSWORD = '{password}';";
                if (this._master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Change user password command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeLoginPassword)} - " +
                        "Change user password command failed.");

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeLoginPassword)} - " +
                    $"Exception occurred when changing password on SQL Host.");

                return -20;
            }
            finally
            {
            }
        }

        #endregion


        #region Permissions Management

        /// <summary>
        /// Get Database Roles assigned to the given Login.
        /// </summary>
        /// <param name="login"></param>
        /// <param name="database"></param>
        /// <param name="roles"></param>
        /// <returns></returns>
        public int Get_DBRoles_for_Login(string login, string database, out List<OGA.MSSQL.eDBRoles> roles)
        {
            roles = null;

            if (Get_DBRoles_for_Database(database, out var dbroles) != 1)
            {
                // Error occurred.
                return -1;
            }
            // If here, we have a list of roles for the database.

            roles = new List<eDBRoles>();

            // Sift through them for the given login.
            foreach (var s in dbroles)
            {
                if (s.LoginName.ToLower() == login.ToLower())
                {
                    // Got a match.

                    // See if we can recover a database role from the Groupname field.
                    eDBRoles dbr = Recover_DatabaseRole_from_String(s.GroupName);
                    if (dbr == eDBRoles.none)
                    {
                        // No database role in the current record.
                        // Nothing to add.
                    }
                    else
                    {
                        // Database role recovered.
                        // Add it to the list.
                        roles.Add(dbr);
                    }
                }
            }

            return 1;
        }

        /// <summary>
        /// Get a list of all database roles for the database.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="roles"></param>
        /// <returns></returns>
        public int Get_DBRoles_for_Database(string database, out List<OGA.MSSQL.Model.DBRole_Assignment> roles)
        {
            System.Data.DataTable dt = null;
            roles = null;

            // Compose the query that will pull database roles.
            string sql = @"SELECT
                                DB_NAME() AS DatabaseName,
                                roles.name AS RoleName,
	                            roles.sid AS [RoleSid],
	                            rm.role_principal_id [RoleId],
	                            members.principal_id AS [User_PrincipleId],
                                members.name AS MemberName,
                                sp.name AS LoginName,
                                members.type_desc AS MemberType,
	                            members.type,
	                            members.authentication_type,
	                            members.sid AS [MemberSid],
	                            members.default_schema_name
                            FROM sys.database_role_members AS rm
                            JOIN sys.database_principals AS roles
                                ON rm.role_principal_id = roles.principal_id
                            JOIN sys.database_principals AS members
                                ON rm.member_principal_id = members.principal_id
                            LEFT JOIN sys.server_principals AS sp
                                ON members.sid = sp.sid
                            ORDER BY RoleName, MemberName;
                            ";

            // First, see if the database exists...
            int res = Does_Database_Exist(database);
            if (res < 0)
            {
                // Error occurred.
                return -1;
            }
            else if (res == 0)
            {
                // Database does not exist.
                return -2;
            }
            // If here, the database exists.

            // Now, get the set of database roles...
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get database roles..."
                        , _classname, database);

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DBRoles_for_Database)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get database roles from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get database roles from the database."
                            , _classname);

                    return -2;
                }
                // We have a datatable of database roles.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No database roles are defined.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Did not get any database roles. Something is wrong."
                            , _classname, database);

                    return -3;
                }
                // If here, we have database roles.

                roles = new List<OGA.MSSQL.Model.DBRole_Assignment>();

                // Convert each result row to a database role instance.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    OGA.MSSQL.Model.DBRole_Assignment role = new OGA.MSSQL.Model.DBRole_Assignment();

                    //role.ServerName = r["ServerName"] + "";
                    role.DBName = r["DatabaseName"] + "";
                    role.GroupName = r["RoleName"] + "";
                    role.UserName = r["MemberName"] + "";
                    role.LoginName = r["LoginName"] + "";
                    role.Default_Schema_Name = r["default_schema_name"] + "";
                    role.Principal_ID = r["User_PrincipleId"] + "";
                    role.SID = r["RoleSid"] + "";

                    roles.Add(role);
                }
                // If here, we got a list of roles for the database.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        #endregion


        #region Table Management

        /// <summary>
        /// Retrieves the list of tables for the given database.
        /// NOTE: This command must be executed on a connection with the given database, not to the system database, postgres.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tablelist"></param>
        /// <returns></returns>
        public int Get_TableList_forDatabase(string database, out List<string> tablelist)
        {
            System.Data.DataTable dt = null;
            tablelist = new List<string>();

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_TableList_forDatabase)} - " +
                    $"Attempting to get table names for database {database ?? ""}...");

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_TableList_forDatabase)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = $"SELECT TABLE_NAME " +
                             $"FROM INFORMATION_SCHEMA.TABLES " +
                             $"WHERE TABLE_CATALOG = '{database}' " +
                             $"AND TABLE_TYPE = 'BASE TABLE' " +
                             $"AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys'); ";


                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get table names from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_TableList_forDatabase)} - " +
                        "Failed to get table names from the database.");

                    return -2;
                }
                // We have a datatable of table names.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No tables in the database.
                    // Or, the database doesn't exist.

                    return 1;
                }
                // If here, we have tables for the database.

                // See if we have a match to the given tablename.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    string sss = r[0] + "";
                    tablelist.Add(sss);
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_TableList_forDatabase)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public int Is_Table_in_Database(string database, string tablename)
        {
            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get table names for database {1}..."
                        , _classname, database);

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Is_Table_in_Database)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = "SELECT TABLE_NAME FROM information_schema.tables WHERE [TABLE_TYPE] = 'BASE TABLE'";

                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get table names from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get table names from the database."
                            , _classname);

                    return -2;
                }
                // We have a datatable of table names.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No tables in the database.
                    // Or, the database doesn't exist.
                    // Quite likely, the database name is wrong.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Did not get any tables for database {1}. Table name might be wrong."
                            , _classname, database);

                    return -3;
                }
                // If here, we have tables for the database.

                // See if we have a match to the given tablename.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    string sss = r[0] + "";

                    if (sss.ToUpper() == tablename.ToUpper())
                    {
                        // Got a match.

                        return 1;
                    }
                }
                // If here, we didn't find a match.

                return 0;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Returns zero or positive for row count, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tablename"></param>
        /// <returns></returns>
        public int Get_TableSize(string database,string tablename)
        {
            if (this.Get_RowCount_for_Tables(database, out var rowdata) != 1)
            {
                // Failed to get table row count data.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Failed to get table row count data."
                        , _classname);

                return -1;
            }
            // We have a list of table row sizes to filter down.

            try
            {
                int val = rowdata.FirstOrDefault(m => m.Key.ToUpper() == tablename.ToUpper()).Value;

                return val;
            }
            catch (Exception e)
            {
                // Failed to get table row count data.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred while getting table row count data."
                        , _classname);

                return -1;
            }
        }

        /// <summary>
        /// Checks if the given table exists in the connected database.
        /// Returns 1 if exists, 0 if not. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int DoesTableExist(string database, string tableName)
        {
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                    $"Attempting to query if table ({(tableName ?? "")}) exists...");

                if (string.IsNullOrWhiteSpace(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // Get the table list...
                var res2 = this.Get_TableList_forDatabase(database, out var tl);
                if (res2 != 1 || tl == null)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                        $"Table Not Found.");

                    return -1;
                }
                if (!tl.Contains(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                        $"Table Not Found.");

                    return 0;
                }
                // If here, the table was found in the connected database.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Creates a table in the connected database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tabledef"></param>
        /// <returns></returns>
        public int Create_Table(string database, TableDefinition tabledef)
        {
            string sql = "";

            if (tabledef == null)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                    $"Null table definition.");

                return -1;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                    $"Attempting to create table ({(tabledef.tablename ?? "")})...");

                if (string.IsNullOrWhiteSpace(tabledef.tablename))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Table name is set.

                // Check if the table exists or not...
                var res2 = this.DoesTableExist(database, tabledef.tablename);
                if (res2 == 1)
                {
                    // Already present.
                    return 1;
                }
                if (res2 < 0)
                {
                    // Failed to query for table.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                        $"Failed to query for table.");

                    return -1;
                }
                // If here, the table doesn't yet exist.

                // Formulate the sql command...
                sql = tabledef.CreateSQLCmd();

                // Execute it on the postgres instance.
                if (dbdal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while adding the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                        "Error occurred while adding the table.");

                    return -4;
                }

                // Check if the table is now present on the server.
                if (this.DoesTableExist(database, tabledef.tablename) != 1)
                {
                    // The table was not created successfully.
                    return -5;
                }
                // If here, the table was added.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Drops a table from the connected database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int Drop_Table(string database, string tableName)
        {
            string sql = "";

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                    $"Attempting to drop table ({(tableName ?? "")})...");

                if (string.IsNullOrWhiteSpace(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Check if the table exists or not...
                var res2 = this.DoesTableExist(database, tableName);
                if (res2 == 0)
                {
                    // Already deleted.
                    return 1;
                }
                if (res2 < 0)
                {
                    // Failed to query for table.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        $"Failed to query for table.");

                    return -1;
                }
                // If here, the table still exists.

                // Formulate the sql command...
                sql = $"DROP TABLE IF EXISTS {tableName};";

                // Execute it on the postgres instance.
                if (dbdal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while dropping the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        "Error occurred while dropping the table.");

                    return -4;
                }

                // Check if the table is still present on the server.
                if (this.DoesTableExist(database, tableName) != 1)
                {
                    // The table was not dropped as expected.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        "Table drop failed. Table is still present.");

                    return -5;
                }
                // If here, the table was dropped.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                    "Exception occurred");

                return -20;
            }
        }

        /// <summary>
        /// Gets the row count for each table in the database the user connects to.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="rowdata"></param>
        /// <returns></returns>
        public int Get_RowCount_for_Tables(string database, out List<KeyValuePair<string, int>> rowdata)
        {
            /* This method implements the following sql query:
             * SELECT
                 SCHEMA_NAME(schema_id) AS [SchemaName]
                ,[Tables].name AS [TableName]
                ,SUM([Partitions].[rows]) AS [TotalRowCount]
                FROM sys.tables AS [Tables]
                JOIN sys.partitions AS [Partitions]
                ON [Tables].[object_id] = [Partitions].[object_id]
                AND [Partitions].index_id IN ( 0, 1 )
                -- WHERE [Tables].name = N'name of the table'
                GROUP BY SCHEMA_NAME(schema_id), [Tables].name
             */

            System.Data.DataTable dt = null;
            rowdata = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get table row counts for database {1}..."
                        , _classname, database);

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_RowCount_for_Tables)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = "SELECT SCHEMA_NAME(schema_id) AS[SchemaName],[Tables].name AS[TableName],SUM([Partitions].[rows]) AS[TotalRowCount] " +
                    "FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN ( 0, 1 ) " +
                    "GROUP BY SCHEMA_NAME(schema_id), [Tables].name";

                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get row counts from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get row counts from the database."
                            , _classname);

                    return -2;
                }
                // We have a datatable of row counts.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No tables in the database.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Did not get any row counts for database{1}. Database name might be wrong."
                            , _classname, database);

                    return -3;
                }
                // If here, we have row counts for the database.

                // Turn them into a list.
                rowdata = new List<KeyValuePair<string, int>>();
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the table name.
                    string tablename = r["TableName"].ToString() + "";
                    int tablesize = 0;

                    // Get the table size.
                    try
                    {
                        string tempval = r["TotalRowCount"].ToString() + "";
                        tablesize = Convert.ToInt32(tempval);
                    }
                    catch (Exception e)
                    {
                        // An exception occurred while parsing in table row size data.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                            "An exception occurred while parsing in table row size data for database {1}."
                                , _classname, database);

                        return -4;
                    }
                    KeyValuePair<string, int> vv = new KeyValuePair<string, int>(tablename, tablesize);
                    rowdata.Add(vv);
                }
                // If here, we have iterated all rows, and can return to the caller.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Gets a list of column names for the given table of the database the instance is connected to.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tablename"></param>
        /// <param name="ColumnNames"></param>
        /// <returns></returns>
        public int Get_Columns_for_Table(string database, string tablename, out List<string> ColumnNames)
        {
            System.Data.DataTable dt = null;
            ColumnNames = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get column names for table {2} of database {1}..."
                        , _classname, database, tablename);

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_Columns_for_Table)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = "SELECT [name] FROM sys.columns WHERE object_id = OBJECT_ID('" + tablename + "')";

                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get column names from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get column names from the database."
                            , _classname);

                    return -2;
                }
                // We have a datatable of column names.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No columns in the table.
                    // Or, the table doesn't exist.
                    // Quite likely, the table name is wrong.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Did not get any columns for table {1}. Table name might be wrong."
                            , _classname, tablename);

                    return -3;
                }
                // If here, we have columns for the table.

                // Turn them into a list.
                ColumnNames = new List<string>();
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    ColumnNames.Add(r[0] + "");
                }
                // Return what we have.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, database);

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Retrieves the list of columns and types for the given table.
        /// NOTE: This command must be executed on a connection with the given database, not to the system database, postgres.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tableName"></param>
        /// <param name="columnlist"></param>
        /// <returns></returns>
        public int Get_ColumnInfo_forTable(string database, string tableName, out List<ColumnInfo> columnlist)
        {
            System.Data.DataTable dt = null;
            columnlist = new List<ColumnInfo>();

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                    $"Attempting to get column info for table {tableName ?? ""}...");

                if (string.IsNullOrWhiteSpace(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                        $"Table name is empty.");

                    return -1;
                }

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = "SELECT DB_NAME() AS table_catalog, " +
                             "s.name AS table_schema, " +
                             "t.name AS table_name, " +
                             "c.column_id AS ordinal_position, " +
                             "c.name AS column_name, " +
                             "TYPE_NAME(c.user_type_id) AS data_type, " +
                             "CASE " +
                             "    WHEN TYPE_NAME(c.user_type_id) like 'nvarchar' THEN (c.max_length / 2)" +
                             "    WHEN TYPE_NAME(c.user_type_id) like '%char' THEN c.max_length " +
                             "    WHEN TYPE_NAME(c.user_type_id) = 'text' THEN c.max_length " +
                             "    ELSE null " +
                             "END AS character_maximum_length, " +
                             "c.max_length AS character_maximum_length, " +
                             "c.precision, " +
                             "c.scale, " +
                             "c.is_nullable, " +
                             "c.is_identity, " +
                             "ic.seed_value, " +
                             "ic.increment_value, " +
                             "ic.last_value, " +
                             "CASE " +
                             "    WHEN c.is_identity = 1 THEN 'BY DEFAULT' " +
                             "    ELSE NULL " +
                             "END AS identity_generation " +
                             "FROM sys.columns AS c " +
                             "JOIN sys.tables  AS t ON c.object_id = t.object_id " +
                             "JOIN sys.schemas AS s ON t.schema_id = s.schema_id " +
                             "LEFT JOIN sys.identity_columns AS ic  " +
                             "ON c.object_id = ic.object_id AND c.column_id = ic.column_id " +
                             "WHERE t.name = '" + tableName + "' " +
                             //"AND s.name = 'dbo' " +
                             "ORDER BY c.column_id;";

                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get column names from the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                        "Failed to get column info from the table.");

                    return -2;
                }
                // We have a datatable of column info.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No column in the table.
                    // Or, the table doesn't exist.

                    // Verify the table exists...
                    var restable = this.DoesTableExist(database, tableName);
                    if (restable != 1)
                    {
                        // Table doesn't exist.
                        // That's why our column list query returned nothing.

                        return 0;
                    }

                    return 1;
                }
                // If here, we have columns for the table.

                foreach (System.Data.DataRow r in dt.Rows)
                {
                    var ct = new ColumnInfo();

                    var res1a = MSSQL_DAL.Recover_FieldValue_from_DBRow(r, "column_name", out string val);
                    if (res1a != 1)
                    {
                        // Failed to get column_name column value.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                            $"Failed to get column_name column value.");

                        return -22;
                    }
                    ct.name = val + "";

                    var res1b = MSSQL_DAL.Recover_FieldValue_from_DBRow(r, "data_type", out string val2);
                    if (res1b != 1)
                    {
                        // Failed to get data_type column value.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                            $"Failed to get data_type column value.");

                        return -22;
                    }
                    ct.dataType = val2 + "";

                    var res1 = MSSQL_DAL.Recover_FieldValue_from_DBRow(r, "ordinal_position", out int displayorder);
                    if (res1 != 1)
                    {
                        // Failed to get ordinal_position column value.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                            $"Failed to get ordinal_position column value.");

                        return -22;
                    }
                    ct.ordinal = displayorder;

                    try
                    {
                        int maxlength = Convert.ToInt32(r["character_maximum_length"]);
                        ct.maxlength = maxlength;
                    }
                    catch (Exception e)
                    {
                        ct.maxlength = null;
                    }

                    try
                    {
                        var res5 = MSSQL_DAL.Recover_FieldValue_from_DBRow(r, "is_nullable", out bool nullable);
                        if (res5 != 1)
                        {
                            // Failed to get is_nullable column value.

                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                                $"Failed to get is_nullable column value.");

                            return -22;
                        }

                        if (nullable)
                            ct.isNullable = true;
                        else
                            ct.isNullable = false;
                    }
                    catch (Exception e)
                    {
                        ct.isNullable = false;
                    }

                    // Recover any identity information for the column...
                    try
                    {
                        var res5 = MSSQL_DAL.Recover_FieldValue_from_DBRow(r, "is_identity", out bool isidentity);
                        if (res5 != 1)
                        {
                            // Failed to get is_identity column value.

                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                                $"Failed to get is_identity column value.");

                            return -22;
                        }

                        if (!isidentity)
                        {
                            ct.isIdentity = false;
                            ct.identityBehavior = DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                        }
                        else
                        {
                            ct.isIdentity = true;

                            // Get the identity behavior...
                            try
                            {
                                string ib = ((string)r["identity_generation"]) ?? "";
                                if (ib == "BY DEFAULT")
                                    ct.identityBehavior = DAL.CreateVerify.Model.eIdentityBehavior.GenerateByDefault;
                                else
                                    ct.identityBehavior = DAL.CreateVerify.Model.eIdentityBehavior.UNSET;
                            }
                            catch (Exception e)
                            {
                                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                                    $"Exception occurred while parsing identity_generation for column ({(ct.name ?? "")})");

                                return -21;
                            }
                        }
                    }
                    catch (Exception e)
                    {
                        ct.isIdentity = false;
                    }

                    columnlist.Add(ct);
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                    "Exception occurred");

                return -20;
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        #endregion


        #region Private Methods

        private int priv_AddUsertoDatabase(string username, string database)
        {
            try
            {
                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(priv_AddUsertoDatabase)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql string that adds the user to it...
                string sql = @$"IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'{username}')
                                BEGIN
                                    CREATE USER [{username}] FOR LOGIN [{username}];
                                END;";

                // Execute it on the sql server instance.
                if (dbdal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while adding user to the target database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while adding user to the target database."
                        , _classname);

                    return -1;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname, database);

                return -20;
            }
        }

        /// <summary>
        /// Attempts to enable command shell access on the SQL Engine.
        /// </summary>
        /// <returns></returns>
        private int SQLEngine_EnableCmdShell()
        {
            try
            {
                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(SQLEngine_DeleteFile)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Enable advanced options...
                string sql1 = "EXEC sp_configure 'show advanced options', 1; RECONFIGURE;";
                // We have the sql script to run.

                // Execute it on the sql server instance...
                if (_master_dal.Execute_NonQuery(sql1).res != -1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while enabling command shell access on SQL server."
                        , _classname);

                    return -1;
                }

                // Enable command shell...
                string sql2 = "EXEC sp_configure 'xp_cmdshell', 1; RECONFIGURE;";
                // We have the sql script to run.

                // Execute it on the sql server instance...
                if (_master_dal.Execute_NonQuery(sql2).res != -1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while enabling command shell access on SQL server."
                        , _classname);

                    return -1;
                }

                return 1;
            }
            catch (Exception e)
            {
                return -10;
            }
        }
        /// <summary>
        /// Attempts to disable command shell access on the SQL Engine.
        /// </summary>
        /// <returns></returns>
        private int SQLEngine_DisableCmdShell()
        {
            try
            {
                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(SQLEngine_DeleteFile)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Disable command shell...
                string sql2 = "EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE;";
                // We have the sql script to run.

                // Execute it on the sql server instance...
                if (_master_dal.Execute_NonQuery(sql2).res != -1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while disabling command shell access on SQL server."
                        , _classname);

                    return -1;
                }

                return 1;
            }
            catch (Exception e)
            {
                return -10;
            }
        }

        /// <summary>
        /// Attempts to delete a file on the SQL Engine host, via its command line access.
        /// Running this, requires executing SQLEngine_DisableCmdShell(), first.
        /// </summary>
        /// <param name="filepath"></param>
        /// <returns></returns>
        private int SQLEngine_DeleteFile(string filepath)
        {
            try
            {
                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(SQLEngine_DeleteFile)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the string to delete the given file...
                // This composite statement recovers the return value from the command shell execution, as @result.
                // Then, it passes @result back to us as a scalar query result.
                string sql = "DECLARE @result INT; " +
                              "EXEC @result = xp_cmdshell 'DEL \"" + filepath + "\"'; " +
                              "SELECT @result;";
                //string sql = "EXEC xp_cmdshell 'DEL \"" + filepath + "\"';";
                // We have the sql script to run.

                // Execute it on the sql server instance...
                //int res = _dal.Execute_NonQuery(sql);
                int res = _master_dal.Execute_Scalar(sql, System.Data.CommandType.Text, out var result);
                if (res != 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while performing file delete on SQL server."
                        , _classname);

                    return -1;
                }

                return 1;
            }
            catch (Exception e)
            {
                return -10;
            }
        }

        private string Get_FullyQualified_SQLHostName()
        {
            if (this.Service.Trim().Length > 0)
                return this.HostName + "\\" + this.Service;
            else
                return this.HostName;
        }

        /// <summary>
        /// Attempts to locate the SQLCMD executable in the filesystem.
        /// Returns the full path when found.
        /// </summary>
        /// <param name="toolpath"></param>
        /// <returns></returns>
        private int Locate_SQLCMDexe_in_FileSystem(out string toolpath)
        {
            // The tool path is here for SQL Server 2008...
            string sqlcmd_for_2008 = "C:\\Program Files\\Microsoft SQL Server\\100\\Tools\\Binn\\SQLCMD.exe";
            // The tool path is here for SQL Server 2012...
            string sqlcmd_for_2012 = "C:\\Program Files\\Microsoft SQL Server\\110\\Tools\\Binn\\SQLCMD.exe";
            // The tool path is here for SQL Server 2014...
            string sqlcmd_for_2014 = "C:\\Program Files\\Microsoft SQL Server\\120\\Tools\\Binn\\SQLCMD.exe";
            // The tool path is here for SQL Server 2017...
            string sqlcmd_for_2017 = "C:\\Program Files\\Microsoft SQL Server\\110\\Tools\\Binn\\SQLCMD.exe";
            // It's here for older versions...
            string sqlcmd_for_older = "C:\\Program Files\\Microsoft SQL Server\\Client SDK\\ODBC\\130\\Tools\\Binn\\SQLCMD.exe";

            if (System.IO.File.Exists(sqlcmd_for_2017))
            {
                // Found it at the 2017 path.
                toolpath = sqlcmd_for_2017;
                return 1;
            }
            if (System.IO.File.Exists(sqlcmd_for_2008))
            {
                // Found it at the 2008 path.
                toolpath = sqlcmd_for_2008;
                return 1;
            }
            if (System.IO.File.Exists(sqlcmd_for_2012))
            {
                // Found it at the 2012 path.
                toolpath = sqlcmd_for_2012;
                return 1;
            }
            if (System.IO.File.Exists(sqlcmd_for_2014))
            {
                // Found it at the 2014 path.
                toolpath = sqlcmd_for_2014;
                return 1;
            }
            if (System.IO.File.Exists(sqlcmd_for_older))
            {
                // Found it at the older path.
                toolpath = sqlcmd_for_older;
                return 1;
            }

            // If here, we failed to lcoate the SQLCMD tool path.
            toolpath = "";
            return -1;
        }

        private eDBRoles Recover_DatabaseRole_from_String(string groupName)
        {
            string tempstr = groupName.ToLower();

            // See what the given role is.
            if (tempstr == eDBRoles.db_accessadmin.ToString())
                return eDBRoles.db_accessadmin;
            else if (tempstr == eDBRoles.db_backupoperator.ToString())
                return eDBRoles.db_backupoperator;
            else if (tempstr == eDBRoles.db_datareader.ToString())
                return eDBRoles.db_datareader;
            else if (tempstr == eDBRoles.db_datawriter.ToString())
                return eDBRoles.db_datawriter;
            else if (tempstr == eDBRoles.db_ddladmin.ToString())
                return eDBRoles.db_ddladmin;
            else if (tempstr == eDBRoles.db_denydatareader.ToString())
                return eDBRoles.db_denydatareader;
            else if (tempstr == eDBRoles.db_denydatawriter.ToString())
                return eDBRoles.db_denydatawriter;
            else if (tempstr == eDBRoles.db_owner.ToString())
                return eDBRoles.db_owner;
            else if (tempstr == eDBRoles.db_securityadmin.ToString())
                return eDBRoles.db_securityadmin;
            else
                return eDBRoles.none;
        }

        private void Close_MasterDAL()
        {
            try
            {
                _master_dal?.Dispose();
            }
            catch (Exception) { }
            _master_dal = null;
        }

        /// <summary>
        /// Centralized logic for retrieving config values from the master database, usually queried as a specific column of a single-record from a tabular result.
        /// Returns 1 if found, 0 if nothing found, negatives for errors.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="workattempted"></param>
        /// <param name="parentmethodname"></param>
        /// <returns></returns>
        private (int res, string? value) Get_Scalar_fromMaster(string sql, string workattempted = "", [CallerMemberName] string parentmethodname = "")
        {
            try
            {
                // Verify givens...
                if (string.IsNullOrWhiteSpace(sql))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                        $"Given statement was empty.");

                    return (-1, null);
                }

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                        $"Failed to connect to server.");

                    return (-1, null);
                }

                // Run the query...
                if (_master_dal.Execute_Scalar(sql, System.Data.CommandType.Text, out var raw) != 1)
                {
                    // Failed to get result data.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                        $"Failed to {workattempted}.");

                    return (-2, null);
                }

                if (raw == null)
                {
                    // Not found.
                    return (0, null);
                }

                // Get the value...
                string? val = raw == DBNull.Value ? null : raw.ToString();

                return (1, val);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                    $"Exception occurred while attempting to {workattempted}.");

                return (-20, null);
            }
        }

        /// <summary>
        /// Centralized logic for retrieving config values from the master database, usually queried as a specific column of a single-record from a tabular result.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="columnname"></param>
        /// <param name="workattempted"></param>
        /// <param name="parentmethodname"></param>
        /// <returns></returns>
        private (int res, string? value) Get_Value_fromMaster_Tabular(string sql, string columnname, string workattempted = "", [CallerMemberName] string parentmethodname = "")
        {
            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                    $"Attempting to {workattempted}...");

                // Verify givens...
                if (string.IsNullOrWhiteSpace(sql))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                        $"Given statement was empty.");

                    return (-1, null);
                }
                if (string.IsNullOrWhiteSpace(columnname))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                        $"Given columnname was empty.");

                    return (-1, null);
                }

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                        $"Failed to connect to server.");

                    return (-1, null);
                }

                // Run the tabular query...
                if (_master_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get result data.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                        $"Failed to {workattempted}.");

                    return (-2, null);
                }
                // We have results.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // Result was not found.
                    return (0, "");
                }

                // Get the value...
                object raw = dt.Rows[0][columnname];
                string? val = raw == DBNull.Value ? null : raw.ToString();

                return (1, val);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{parentmethodname} - " +
                    $"Exception occurred while attempting to {workattempted}.");

                return (-20, null);
            }
            finally
            {
                try
                {
                    dt?.Dispose();
                }
                catch (Exception) { }
            }
        }

        /// <summary>
        /// Call this to stand up the admin connection to the SQL host.
        /// This is used for server and engine level actions, such as managing logins (not users, but logins), adding databases, etc.
        /// </summary>
        private void SetupDALtoMaster()
        {
            if (_master_dal == null)
            {
                _master_dal = new MSSQL_DAL();
                _master_dal.host = HostName;
                _master_dal.service = Service;
                _master_dal.database = "master";
                _master_dal.username = Username;
                _master_dal.password = Password;
                _master_dal.Cfg_ClearConnectionPoolOnClose = this.Cfg_ClearConnectionPoolOnClose;
            }
        }

        /// <summary>
        /// Iterates each database DAL and closes and dereferences it.
        /// </summary>
        private void Close_DatabaseDALs()
        {
            while(this._dbdals.Count > 0)
            {
                // Get the current entry...
                var entry = this._dbdals.First();
                if (entry.Value == null)
                    continue;

                // Remove it...
                this._dbdals.Remove(entry.Key);

                var dal = entry.Value;

                try
                {
                    dal?.Dispose();
                }catch(Exception e) { }
            }
        }
        
        /// <summary>
        /// Creates and connects the admin DAL to master.
        /// </summary>
        /// <returns></returns>
        private bool ConnectMasterDAL()
        {
            SetupDALtoMaster();

            // Connect to master...
            var resconn = this._master_dal.Connect();
            if(resconn != 1)
            {
                // Failed to connect to master.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(ConnectMasterDAL)} - " +
                    $"Failed to connect to master.");

                return false;
            }

            return true;
        }

        /// <summary>
        /// Creates and connects the a database DAL to a target database.
        /// </summary>
        /// <returns></returns>
        private MSSQL_DAL? GetDatabaseDAL(string database)
        {
            bool success = false;
            MSSQL_DAL? dal = null;

            try
            {
                // Check if we have a dal already...
                if(!this._dbdals.TryGetValue(database, out dal) || dal == null)
                {
                    // Database dal not found.

                    // Make one...
                    if (dal == null)
                    {
                        dal = new MSSQL_DAL();
                        dal.host = HostName;
                        dal.service = Service;
                        dal.database = database;
                        dal.username = Username;
                        dal.password = Password;
                        dal.Cfg_ClearConnectionPoolOnClose = this.Cfg_ClearConnectionPoolOnClose;
                    }

                    // Ensure it's connected before we add it...
                    if(dal.Connect() != 1)
                    {
                        // Failed to connect to target database.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseDAL)} - " +
                            $"Failed to connect to target database ({(database ?? "")}).");

                        return null;
                    }

                    // Add it to the list...
                    this._dbdals.Add(database, dal);
                }

                // Ensure the cached entry is connected before we use it...
                if(dal.Connect() != 1)
                {
                    // Failed to connect to target database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseDAL)} - " +
                        $"Failed to connect to target database ({(database ?? "")}).");

                    return null;
                }

                success = true;
                return dal;
            }
            catch(Exception)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseDAL)} - " +
                    $"Exception occurred connecting to target database ({(database ?? "")}).");

                return null;
            }
            finally
            {
                if(!success)
                {
                    dal?.Dispose();
                }
            }
        }

        #endregion
    }
}
