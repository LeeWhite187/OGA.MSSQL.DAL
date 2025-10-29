using Microsoft.AspNetCore.ResponseCompression;
using Mono.Posix;
using Mono.Unix.Native;
using NLog.LayoutRenderers;
using OGA.MSSQL.DAL;
using OGA.MSSQL.DAL.CreateVerify.Model;
using OGA.MSSQL.DAL.Model;
using OGA.MSSQL.DAL_SP.Model;
using OGA.MSSQL.Model;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics.Arm;
using System.Text;
using System.Text.RegularExpressions;
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

        /// <summary>
        /// Checks if a file exists on the SQL Engine host, via its command line access.
        /// Running this, requires executing SQLEngine_DisableCmdShell(), first.
        /// Returns 1 if present, 0 if not found, negatives for errors.
        /// </summary>
        /// <param name="filepath"></param>
        /// <returns></returns>
        public int SQLEngine_DoesFileExist(string filepath)
        {
            object tempval;

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
                string sql = "DECLARE @result TABLE (output NVARCHAR(255)); " +
                            "INSERT INTO @result EXEC xp_cmdshell 'IF EXIST \"" + filepath + "\" (ECHO 1) ELSE (ECHO 0)'; " +
                            "SELECT TOP 1 output FROM @result WHERE output IS NOT NULL;";


                //string sql = "EXEC xp_cmdshell 'DEL \"" + filepath + "\"';";
                // We have the sql script to run.

                // Execute it on the sql server instance...
                //int res = _dal.Execute_NonQuery(sql);
                int res = _master_dal.Execute_Scalar(sql, System.Data.CommandType.Text, out tempval);
                if (res != 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while performing file delete on SQL server."
                        , _classname);

                    return -1;
                }

                // See if we found the database.
                int val = 0;
                try
                {
                    if (tempval == null)
                    {
                        // The received value is null.
                        // Default it to no...
                        return 0;
                    }

                    // Attempt to recover the answer from the scalar call...
                    int.TryParse(tempval.ToString(), out val);

                    if (val > 0)
                    {
                        // File was found.
                        return 1;
                    }
                    else
                    {
                        return 0;
                    }
                }
                catch (Exception e)
                {
                    // If here, no file was found.
                    return 0;
                }
            }
            catch (Exception e)
            {
                return -10;
            }
        }

        /// <summary>
        /// Attempts to enable command shell access on the SQL Engine.
        /// </summary>
        /// <returns></returns>
        public int SQLEngine_EnableCmdShell()
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
        public int SQLEngine_DisableCmdShell()
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
        public int SQLEngine_DeleteFile(string filepath)
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
                // Check that the database name was given.
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
                // We will attempt to convert it to multi user.

                string sql = "ALTER DATABASE [" + database + "] SET MULTI_USER WITH ROLLBACK IMMEDIATE;";
                // We have the sql script to run.

                // Execute it on the sql server instance.
                if (_master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Error occurred while converting the database to multi user.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while converting the database to multi user."
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
        /// Set 'force' to true, if there may be other clients connected to the database.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="force"></param>
        /// <returns></returns>
        public int Drop_Database(string database, bool force = false)
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


                // Compose the drop statement, and include a single user mode if needed...
                string sql;
                if (force)
                {
                    sql = $@"IF DB_ID('{database}') IS NOT NULL
                            BEGIN
                                ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                                DROP DATABASE [{database}];
                            END;";
                }
                else
                {
                    sql = $@"IF DB_ID('{database}') IS NOT NULL
                                DROP DATABASE [{database}];";
                }
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

        /// <summary>
        /// Gets the disk space used by the given database.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public (int res, long size) Get_DatabaseSize(string databaseName)
        {
            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseSize)} - " +
                    $"Attempting to get disk size for database, {databaseName ?? ""}...");

                if(string.IsNullOrWhiteSpace(databaseName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseSize)} - " +
                        "Empty database.");

                    return (-1, 0);
                }

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseSize)} - " +
                        $"Failed to connect to server.");

                    return (-1, 0);
                }
                // We have a persistent connection.

                // Compose the sql query we will perform.
                string sql = @$"SELECT
                                    SUM(size) * 8 * 1024 AS DatabaseSizeBytes,
                                    CAST(SUM(size) * 8 / 1024.0 AS DECIMAL(10,2)) AS DatabaseSizeMB
                                FROM sys.master_files
                                WHERE database_id = DB_ID('{databaseName}');";

                if (_master_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get database size.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseSize)} - " +
                        $"Failed to get disk size for database, {databaseName ?? ""}.");

                    return (-2, 0);
                }
                // We have a database size.

                // See if it contains anything.
                if (dt.Rows.Count != 1)
                {
                    // Database not found.

                    return (0, 0);
                }
                // If here, we have the database entry.

                var resconv = MSSQL_DAL.Recover_FieldValue_from_DBRow(dt.Rows[0], "DatabaseSizeBytes", out long val);
                if(resconv != 1 || val == null)
                {
                    // Database not found.

                    return (0, 0);
                }

                return (1, val);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseSize)} - " +
                    "Exception occurred");

                return (-20, 0);
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


        #region Login Management

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
        /// Returns -3 if password is invalid.
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
                if(resadd.res == -88 && resadd.err.Contains("Password violates complexity rule."))
                {
                    // Password violates complexity rule.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Add_LocalLogin)} - " +
                        "Password violates complexity rule.");

                    return -3;
                }
                if (resadd.res != -1)
                {
                    // Failed to add login to sql server instance.
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
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="loginlist">Account login strings</param>
        /// <returns></returns>
        public int GetLoginList(out List<string> loginlist)
        {
            loginlist = new List<string>();
            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetLoginList)} - " +
                    $"Attempting to get login list on SQL Host...");

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Add_WindowsLogin)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query to get logins on the SQL Host...
                string sql = "SELECT name AS LoginName " +
                             "FROM sys.server_principals " +
                             "WHERE type IN ('S', 'U', 'G') " +
                             "ORDER BY name ASC;";

                if (this._master_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetLoginList)} - " +
                        "Failed to get logins.");

                    return -2;
                }
                // We have a datatable of logins.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                    return 1;

                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the current user...
                    string sss = r["LoginName"] + "";
                    loginlist.Add(sss);
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetLoginList)} - " +
                    $"Exception occurred while getting a list of logins on the SQL host.");

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

        #endregion


        #region User Management

        /// <summary>
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database">Database name</param>
        /// <param name="userlist">Database user list of strings</param>
        /// <returns></returns>
        public int GetDatabaseUsers(string database, out List<string> userlist)
        {
            userlist = new List<string>();
            System.Data.DataTable dt = null;

            try
            {
                if (string.IsNullOrWhiteSpace(database))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseUsers)} - " +
                        $"Empty database name.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseUsers)} - " +
                    $"Attempting to get user list for database ({(database ?? "")})...");

                // Connect to the database...
                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseUsers)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Compose the sql query to get users in a database...
                string sql = @"SELECT name AS UserName
                               FROM sys.database_principals
                               WHERE type IN ('S', 'U', 'G')  -- S=SQL user, U=Windows user, G=Windows group
                                 AND name NOT IN ('guest', 'INFORMATION_SCHEMA', 'sys')  -- filter system users
                               ORDER BY name ASC;";

                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseUsers)} - " +
                        $"Failed to get users for database ({(database ?? "")}).");

                    return -2;
                }
                // We have a datatable of logins.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                    return 1;

                foreach (System.Data.DataRow r in dt.Rows)
                {
                    // Get the current user...
                    string sss = r["UserName"] + "";
                    userlist.Add(sss);
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(GetDatabaseUsers)} - " +
                    $"Exception occurred while getting a list of users for database ({(database ?? "")}).");

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
        /// <param name="username">User string</param>
        /// <param name="database">Target database</param>
        /// <returns></returns>
        public int Does_User_Exist_forDatabase(string database, string username)
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
        public int Add_User_to_Database(string database, string login, List<eSQLRoles>? desiredroles = null)
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
                var resexists = this.Does_User_Exist_forDatabase(database, login);
                if (resexists == 0)
                {
                    // User not found in database.

                    // We need to add the user to the target database.
                    if (this.priv_AddUsertoDatabase(database, login) != 1)
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
                if (this.Get_DatabaseRoles_for_User(database, login, out var foundroles) != 1)
                {
                    // Failed to get roles for the user.
                    return -2;
                }
                // We have current roles for the user.

                // Reconcile the found roles with desired...
                // From the given set of roles to expect for the user, and the current roles for the user, we need to determine the list of ones to add and one to remove.
                var pcl = DetermineRoleChanges(foundroles, desiredroles);
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
                        sqladd = sqladd + $"ALTER ROLE [{r.role.ToString()}] ADD MEMBER [{login}];";
                    }
                    else
                    {
                        // Role to remove.
                        sqlremove = sqlremove + $"ALTER ROLE [{r.role.ToString()}] DROP MEMBER [{login}];";
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
                if (dbdal.Execute_NonQuery(sql).res != -1)
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
        /// Public call to delete a database user.
        /// NOTE: This call works with 'User' as a database-level user, not with Server-level logins.
        /// NOTE: If you want to remove a login from the SQL host, call DeleteLogin().
        /// Confirms the user was deleted.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public int DeleteUserfromDatabase(string database, string username)
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
                var resq = this.Does_User_Exist_forDatabase(database, username);
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
        /// Public call to change a login password.
        /// NOTE: A login is a server-level object.
        /// NOTE: So, changing a password requires having the systemadmin role, or granted ALTER LOGIN.
        /// Returns 1 for success. Negatives for errors.
        /// Returns -3 if password is invalid.
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
                var respf = this._master_dal.Execute_NonQuery(sql);
                if(respf.res == -88 && respf.err.Contains("Password violates complexity rule."))
                {
                    // Password violates complexity rule.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(ChangeLoginPassword)} - " +
                        "Password violates complexity rule.");

                    return -3;
                }
                if (respf.res != -1)
                {
                    // Failed to change password.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(ChangeLoginPassword)} - " +
                        "Failed to change password.");

                    return -2;
                }

                return 1;



                if (respf.res != -1)
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
        /// Get SQL Host Roles assigned to the given Login.
        /// </summary>
        /// <param name="login"></param>
        /// <param name="roles"></param>
        /// <returns></returns>
        public int Get_SQlHostRoles_for_Login(string login, out List<OGA.MSSQL.eSQLRoles> roles)
        {
            roles = null;

            if (Get_Roles_for_SQLHost(out var serverroles) != 1)
            {
                // Error occurred.
                return -1;
            }
            // If here, we have a list of roles.

            roles = new List<eSQLRoles>();

            // Sift through them for the given login.
            foreach (var s in serverroles)
            {
                if (s.Login.ToLower() == login.ToLower())
                {
                    // Got a match.

                    // See if we can recover a sql role from the Groupname field.
                    eSQLRoles dbr = Recover_SQLRoles_from_String(s.GroupName);
                    if (dbr == eSQLRoles.none)
                    {
                        // No sql role in the current record.
                        // Nothing to add.
                    }
                    else
                    {
                        // sql role recovered.
                        // Add it to the list.
                        roles.Add(dbr);
                    }
                }
            }

            return 1;
        }

        /// <summary>
        /// Get a list of all roles for the SQL Host.
        /// </summary>
        /// <param name="roles"></param>
        /// <returns></returns>
        public int Get_Roles_for_SQLHost(out List<OGA.MSSQL.Model.SQLHostRole_Assignment> roles)
        {
            System.Data.DataTable dt = null;
            roles = null;

            // Compose the query that will pull SQL Host roles.
            string sql = @"SELECT
                                roles.name AS ServerRoleName,
	                            roles.sid AS [RoleSid],
                                members.principal_id AS LoginPrincipalId,
                                members.name AS LoginName,
                                members.type_desc AS LoginType,
                                roles.type_desc AS RoleType
                            FROM sys.server_role_members AS rm
                            JOIN sys.server_principals AS members
                                ON rm.member_principal_id = members.principal_id
                            JOIN sys.server_principals AS roles
                                ON rm.role_principal_id = roles.principal_id
                            ORDER BY members.name, roles.name;";

            // Now, get the set of host roles...
            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get host roles..."
                        , _classname);

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_Roles_for_SQLHost)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the SQL Host."
                        , _classname);

                if (this._master_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get roles for the SQL Host.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get roles for the SQL Host."
                            , _classname);

                    return -2;
                }
                // We have a datatable of SQL Host roles.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No SQL Host roles are defined.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Did not get any SQL Host roles. Something is wrong."
                            , _classname);

                    return -3;
                }
                // If here, we have SQL Host roles.

                roles = new List<OGA.MSSQL.Model.SQLHostRole_Assignment>();

                // Convert each result row to a database role instance.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    OGA.MSSQL.Model.SQLHostRole_Assignment role = new OGA.MSSQL.Model.SQLHostRole_Assignment();

                    role.GroupName = r["ServerRoleName"] + "";
                    role.Login = r["LoginName"] + "";
                    role.Principal_ID = r["LoginPrincipalId"] + "";
                    role.RoleSID = r["RoleSid"] + "";

                    roles.Add(role);
                }
                // If here, we got a list of SQL Host roles.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname);

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
        /// Adds the given server role to a SQL host login.
        /// Returns 1 on success, 1 if already set, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <param name="desiredrole">desired role for the login to have</param>
        /// <returns></returns>
        public int Add_Login_Role(string login, eSQLRoles desiredrole)
        {
            try
            {
                // Check that the login is is in the logins list already, on the SQL Host.
                int res = this.Does_Login_Exist(login);
                if (res != 1)
                {
                    // Error occurred.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Login not found."
                            , _classname);

                    return -1;
                }
                // Login exists as a server-principle.

                if(desiredrole == eSQLRoles.none)
                {
                    // The desired roles is null.
                    // We regard this as an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Desired roles cannot be null."
                            , _classname);

                    return -1;
                }

                // Get a list of roles that the login currently has...
                if (this.Get_SQlHostRoles_for_Login(login, out var foundroles) != 1)
                {
                    // Failed to get roles for the login.
                    return -2;
                }
                // We have current roles for the login.

                // See if the role is already present...
                if(foundroles.Any(m=>m == desiredrole))
                {
                    // Already present.
                    // Nothing to do.
                    return 1;
                }

                // Role to add.
                string sql = $"ALTER SERVER ROLE [{desiredrole.ToString()}] ADD MEMBER [{login}];";
                // sqlremove = sqlremove + $"ALTER ROLE [{r.ToString()}] DROP MEMBER [{login}];";

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // We will add the role to the login.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to add SQL Host role for login..."
                        , _classname);

                // Execute the add role command, to update the login's...
                if (this._master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to update SQL Host role to the sql server instance.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to update SQL Host role to the sql server instance."
                            , _classname);

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname);

                return -20;
            }
        }

        /// <summary>
        /// Removes the given server role from a SQL host login.
        /// Returns 1 on success, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <param name="desiredrole">server role to remove</param>
        /// <returns></returns>
        public int Drop_Login_Role(string login, eSQLRoles desiredrole)
        {
            try
            {
                // Check that the login is is in the logins list already, on the SQL Host.
                int res = this.Does_Login_Exist(login);
                if (res != 1)
                {
                    // Error occurred.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Login not found."
                            , _classname);

                    return -1;
                }
                // Login exists as a server-principle.

                if(desiredrole == eSQLRoles.none)
                {
                    // The desired roles is null.
                    // We regard this as an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Desired roles cannot be null."
                            , _classname);

                    return -1;
                }

                // Get a list of roles that the login currently has...
                if (this.Get_SQlHostRoles_for_Login(login, out var foundroles) != 1)
                {
                    // Failed to get roles for the login.
                    return -2;
                }
                // We have current roles for the login.

                // See if the role is present...
                if(!foundroles.Any(m=>m == desiredrole))
                {
                    // Not present.
                    // Nothing to do.
                    return 1;
                }

                // Role to remove.
                string sql = $"ALTER SERVER ROLE [{desiredrole.ToString()}] DROP MEMBER [{login}];";

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // We will drop the role from the login.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to drop SQL Host role from login..."
                        , _classname);

                // Execute the drop role command, to update the login's...
                if (this._master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to update SQL Host role to the sql server instance.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to update SQL Host role to the sql server instance."
                            , _classname);

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname);

                return -20;
            }
        }

        /// <summary>
        /// Determines if the given login has the given role.
        /// Returns 1 if true, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="login"></param>
        /// <param name="role"></param>
        /// <returns></returns>
        public int Does_Login_HaveRole(string login, eSQLRoles role)
        {
            var resr = this.Get_SQlHostRoles_for_Login(login, out var loginroles);
            if(resr != 1 || loginroles == null)
            {
                // Failed to get roles for user.
                return -1;
            }

            // Check if the login has the role...
            if (loginroles.Any(m => m == role))
                return 1;
            else
                return 0;
        }

        /// <summary>
        /// Sets the roles for a SQL host login.
        /// Returns 1 on success, 0 if already set, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <param name="desiredroles">List of desired roles for the login to have</param>
        /// <returns></returns>
        public int Set_Login_Roles(string login, List<eSQLRoles> desiredroles)
        {
            try
            {
                // Check that the login is is in the logins list already, on the SQL Host.
                int res = this.Does_Login_Exist(login);
                if (res != 1)
                {
                    // Error occurred.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Login not found."
                            , _classname);

                    return -1;
                }
                // Login exists as a server-principle.

                if(desiredroles == null)
                {
                    // The desired roles is null.
                    // We regard this as an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Desired roles cannot be null."
                            , _classname);

                    return -1;
                }
                // We will reconcile login roles to match desired.

                // Get a list of roles that the login currently has...
                if (this.Get_SQlHostRoles_for_Login(login, out var foundroles) != 1)
                {
                    // Failed to get roles for the login.
                    return -2;
                }
                // We have current roles for the login.

                // Reconcile the found roles with desired...
                // From the given set of roles to expect for the login, and the current roles for the login, we need to determine the list of ones to add and one to remove.
                var pcl = DetermineRoleChanges(foundroles, desiredroles);
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
                        sqladd = sqladd + $"ALTER SERVER ROLE [{r.role.ToString()}] ADD MEMBER [{login}];";
                    }
                    else
                    {
                        // Role to remove.
                        sqlremove = sqlremove + $"ALTER SERVER ROLE [{r.role.ToString()}] DROP MEMBER [{login}];";
                    }
                }

                // Create a single sql command to run...
                if(!string.IsNullOrWhiteSpace(sqladd))
                    sql = sql + sqladd;
                if(!string.IsNullOrWhiteSpace(sqlremove))
                    sql = sql + sqlremove;

                // At this point, we have a list of roles to add.

                // Connect to the database...
                if (!this.ConnectMasterDAL())
                {
                    // Failed to connect to master.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteLogin)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // We will add each one to the login.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to add SQL Host roles for login..."
                        , _classname);

                // Execute the add and drop role commands, to update the login's membership with desired...
                if (this._master_dal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to update SQL Host roles to the sql server instance.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to update SQL Host roles to the sql server instance."
                            , _classname);

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname);

                return -20;
            }
        }

        /// <summary>
        /// Get Database Roles assigned to the given user.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="roles"></param>
        /// <returns></returns>
        public int Get_DatabaseRoles_for_User(string database, string user, out List<OGA.MSSQL.eSQLRoles> roles)
        {
            roles = null;

            if (Get_DatabaseRoles_for_Database(database, out var dbroles) != 1)
            {
                // Error occurred.
                return -1;
            }
            // If here, we have a list of roles for the database.

            roles = new List<eSQLRoles>();

            // Sift through them for the given user.
            foreach (var s in dbroles)
            {
                if (s.LoginName.ToLower() == user.ToLower())
                {
                    // Got a match.

                    // See if we can recover a database role from the Groupname field.
                    eSQLRoles dbr = Recover_SQLRoles_from_String(s.GroupName);
                    if (dbr == eSQLRoles.none)
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
        public int Get_DatabaseRoles_for_Database(string database, out List<OGA.MSSQL.Model.DBRole_Assignment> roles)
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
                            ORDER BY RoleName, MemberName;";

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
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_DatabaseRoles_for_Database)} - " +
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
                    role.RoleSID = r["RoleSid"] + "";

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

        /// <summary>
        /// Adds the given database role to database user.
        /// Returns 1 on success, 1 if already set, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="desiredrole"></param>
        /// <returns></returns>
        public int Add_User_to_DatabaseRole(string database, string user, eSQLRoles desiredrole)
        {
            try
            {
                // Check that the user is is in the database users list already.
                int res = this.Does_User_Exist_forDatabase(database, user);
                if (res != 1)
                {
                    // Error occurred.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "User not found."
                            , _classname);

                    return -1;
                }
                // User exists.

                if(desiredrole == eSQLRoles.none)
                {
                    // The desired roles is null.
                    // We regard this as an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Desired roles cannot be null."
                            , _classname);

                    return -1;
                }

                // Get a list of database roles that the user currently has...
                if (this.Get_DatabaseRoles_for_User(database, user, out var foundroles) != 1)
                {
                    // Failed to get database roles for the user.
                    return -2;
                }
                // We have current database roles for the user.

                // See if the role is already present...
                if(foundroles.Any(m=>m == desiredrole))
                {
                    // Already present.
                    // Nothing to do.
                    return 1;
                }

                // Role to add.
                string sql = $"ALTER ROLE [{desiredrole.ToString()}] ADD MEMBER [{user}];";
                //string sql = $"ALTER ROLE [{desiredrole.ToString()}] DROP MEMBER [{user}];";

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Add_User_to_DatabaseRole)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to add database role for user..."
                        , _classname);

                // Execute the add database role command, to update the user's...
                if (dbdal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to update database role.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to update database role."
                            , _classname);

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname);

                return -20;
            }
        }

        /// <summary>
        /// Removes the given database role from a database user.
        /// Returns 1 on success, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="desiredrole"></param>
        /// <returns></returns>
        public int Drop_User_from_DatabaseRole(string database, string user, eSQLRoles desiredrole)
        {
            try
            {
                // Check that the user is is in the database users list already.
                int res = this.Does_User_Exist_forDatabase(database, user);
                if (res == 0)
                {
                    // User doesn't exist in database.
                    // So, it definitely doesn't have the role to be removed.

                    // Return success..
                    return 1;
                }
                else if (res < 0)
                {
                    // Failed to query for users.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to query for users."
                            , _classname);

                    return -1;
                }
                // User exists.

                if(desiredrole == eSQLRoles.none)
                {
                    // The desired roles is null.
                    // We regard this as an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Desired roles cannot be null."
                            , _classname);

                    return -1;
                }

                // Get a list of database roles that the user currently has...
                if (this.Get_DatabaseRoles_for_User(database, user, out var foundroles) != 1)
                {
                    // Failed to get database roles for the user.
                    return -2;
                }
                // We have current database roles for the user.

                // See if the role is present...
                if(!foundroles.Any(m=>m == desiredrole))
                {
                    // Not present.
                    // Nothing to do.
                    return 1;
                }

                // Role to remove.
                //string sql = $"ALTER ROLE [{desiredrole.ToString()}] ADD MEMBER [{user}];";
                string sql = $"ALTER ROLE [{desiredrole.ToString()}] DROP MEMBER [{user}];";

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_User_from_DatabaseRole)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to remove database role from user..."
                        , _classname);

                // Execute the remove database role command, to update the user's...
                if (dbdal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to update database role.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to update database role."
                            , _classname);

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname);

                return -20;
            }
        }

        /// <summary>
        /// Determines if the given database user has the given database role.
        /// Returns 1 if true, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="role"></param>
        /// <returns></returns>
        public int Does_User_Have_DatabaseRole(string database, string user, eSQLRoles role)
        {
            var resr = this.Get_DatabaseRoles_for_User(database, user, out var userroles);
            if(resr != 1 || userroles == null)
            {
                // Failed to get roles for user.
                return -1;
            }

            // Check if the user has the role...
            if (userroles.Any(m => m == role))
                return 1;
            else
                return 0;
        }

        /// <summary>
        /// Sets the database roles for a user.
        /// Returns 1 on success, 0 if already set, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="user"></param>
        /// <param name="desiredroles"></param>
        /// <returns></returns>
        public int Set_User_DatabaseRoles(string database, string user, List<eSQLRoles> desiredroles)
        {
            try
            {
                // First, see if the database exists...
                if (Does_Database_Exist(database) != 1)
                {
                    // Database not present or can't access.
                    return -1;
                }

                // Check that the user is is in the database.
                int res = this.Does_User_Exist_forDatabase(database, user);
                if (res != 1)
                {
                    // Error occurred.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "User not found."
                            , _classname);

                    return -1;
                }
                // User exists in database.

                if(desiredroles == null)
                {
                    // The desired roles is null.
                    // We regard this as an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Desired roles cannot be null."
                            , _classname);

                    return -1;
                }
                // We will reconcile user roles to match desired.

                // Get a list of roles that the user currently has...
                if (this.Get_DatabaseRoles_for_User(database, user, out var foundroles) != 1)
                {
                    // Failed to get database roles for the user.
                    return -2;
                }
                // We have current database roles for the user.

                // Reconcile the found database roles with desired...
                // From the given set of database roles to expect for the user, and the current database roles for the user, we need to determine the list of ones to add and one to remove.
                var pcl = DetermineRoleChanges(foundroles, desiredroles);
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
                        sqladd = sqladd + $"ALTER ROLE [{r.role.ToString()}] ADD MEMBER [{user}];";
                    }
                    else
                    {
                        // Role to remove.
                        sqlremove = sqlremove + $"ALTER ROLE [{r.role.ToString()}] DROP MEMBER [{user}];";
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
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Set_User_DatabaseRoles)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to add database roles for user..."
                        , _classname);

                // Execute the add and drop database role commands, to update the user's membership with desired...
                if (dbdal.Execute_NonQuery(sql).res != -1)
                {
                    // Failed to update database roles.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to update database roles."
                            , _classname);

                    return -2;
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname);

                return -20;
            }
        }

        /// <summary>
        /// Determines the net changes between two given sets of roles.
        /// Used by logic that updates user and login roles as required.
        /// </summary>
        /// <param name="existingprivs"></param>
        /// <param name="desiredprivs"></param>
        /// <returns></returns>
        static public List<(bool isgrant, eSQLRoles role)> DetermineRoleChanges(List<eSQLRoles> existingprivs, List<eSQLRoles> desiredprivs)
        {
            var pcl = new List<(bool isgrant, eSQLRoles priv)>();

            // Figure out desiredprivs to add...
            if (desiredprivs.Contains(eSQLRoles.db_accessadmin) && !existingprivs.Contains(eSQLRoles.db_accessadmin))
                pcl.Add((true, eSQLRoles.db_accessadmin));
            if (desiredprivs.Contains(eSQLRoles.db_backupoperator) && !existingprivs.Contains(eSQLRoles.db_backupoperator))
                pcl.Add((true, eSQLRoles.db_backupoperator));
            if (desiredprivs.Contains(eSQLRoles.db_datareader) && !existingprivs.Contains(eSQLRoles.db_datareader))
                pcl.Add((true, eSQLRoles.db_datareader));
            if (desiredprivs.Contains(eSQLRoles.db_datawriter) && !existingprivs.Contains(eSQLRoles.db_datawriter))
                pcl.Add((true, eSQLRoles.db_datawriter));
            if (desiredprivs.Contains(eSQLRoles.db_ddladmin) && !existingprivs.Contains(eSQLRoles.db_ddladmin))
                pcl.Add((true, eSQLRoles.db_ddladmin));
            if (desiredprivs.Contains(eSQLRoles.db_denydatareader) && !existingprivs.Contains(eSQLRoles.db_denydatareader))
                pcl.Add((true, eSQLRoles.db_denydatareader));
            if (desiredprivs.Contains(eSQLRoles.db_denydatawriter) && !existingprivs.Contains(eSQLRoles.db_denydatawriter))
                pcl.Add((true, eSQLRoles.db_denydatawriter));
            if (desiredprivs.Contains(eSQLRoles.db_owner) && !existingprivs.Contains(eSQLRoles.db_owner))
                pcl.Add((true, eSQLRoles.db_owner));
            if (desiredprivs.Contains(eSQLRoles.db_securityadmin) && !existingprivs.Contains(eSQLRoles.db_securityadmin))
                pcl.Add((true, eSQLRoles.db_securityadmin));
            if (desiredprivs.Contains(eSQLRoles.sysadmin) && !existingprivs.Contains(eSQLRoles.sysadmin))
                pcl.Add((true, eSQLRoles.sysadmin));
            if (desiredprivs.Contains(eSQLRoles.diskadmin) && !existingprivs.Contains(eSQLRoles.diskadmin))
                pcl.Add((true, eSQLRoles.diskadmin));
            if (desiredprivs.Contains(eSQLRoles.bulkadmin) && !existingprivs.Contains(eSQLRoles.bulkadmin))
                pcl.Add((true, eSQLRoles.bulkadmin));
            if (desiredprivs.Contains(eSQLRoles.setupadmin) && !existingprivs.Contains(eSQLRoles.setupadmin))
                pcl.Add((true, eSQLRoles.setupadmin));
            if (desiredprivs.Contains(eSQLRoles.processadmin) && !existingprivs.Contains(eSQLRoles.processadmin))
                pcl.Add((true, eSQLRoles.processadmin));
            if (desiredprivs.Contains(eSQLRoles.serveradmin) && !existingprivs.Contains(eSQLRoles.serveradmin))
                pcl.Add((true, eSQLRoles.serveradmin));
            if (desiredprivs.Contains(eSQLRoles.dbcreator) && !existingprivs.Contains(eSQLRoles.dbcreator))
                pcl.Add((true, eSQLRoles.dbcreator));

            // Figure out privileges to remove...
            if (!desiredprivs.Contains(eSQLRoles.db_accessadmin) && existingprivs.Contains(eSQLRoles.db_accessadmin))
                pcl.Add((false, eSQLRoles.db_accessadmin));
            if (!desiredprivs.Contains(eSQLRoles.db_backupoperator) && existingprivs.Contains(eSQLRoles.db_backupoperator))
                pcl.Add((false, eSQLRoles.db_backupoperator));
            if (!desiredprivs.Contains(eSQLRoles.db_datareader) && existingprivs.Contains(eSQLRoles.db_datareader))
                pcl.Add((false, eSQLRoles.db_datareader));
            if (!desiredprivs.Contains(eSQLRoles.db_datawriter) && existingprivs.Contains(eSQLRoles.db_datawriter))
                pcl.Add((false, eSQLRoles.db_datawriter));
            if (!desiredprivs.Contains(eSQLRoles.db_ddladmin) && existingprivs.Contains(eSQLRoles.db_ddladmin))
                pcl.Add((false, eSQLRoles.db_ddladmin));
            if (!desiredprivs.Contains(eSQLRoles.db_denydatareader) && existingprivs.Contains(eSQLRoles.db_denydatareader))
                pcl.Add((false, eSQLRoles.db_denydatareader));
            if (!desiredprivs.Contains(eSQLRoles.db_denydatawriter) && existingprivs.Contains(eSQLRoles.db_denydatawriter))
                pcl.Add((false, eSQLRoles.db_denydatawriter));
            if (!desiredprivs.Contains(eSQLRoles.db_owner) && existingprivs.Contains(eSQLRoles.db_owner))
                pcl.Add((false, eSQLRoles.db_owner));
            if (!desiredprivs.Contains(eSQLRoles.db_securityadmin) && existingprivs.Contains(eSQLRoles.db_securityadmin))
                pcl.Add((false, eSQLRoles.db_securityadmin));
            if (!desiredprivs.Contains(eSQLRoles.sysadmin) && existingprivs.Contains(eSQLRoles.sysadmin))
                pcl.Add((false, eSQLRoles.sysadmin));
            if (!desiredprivs.Contains(eSQLRoles.diskadmin) && existingprivs.Contains(eSQLRoles.diskadmin))
                pcl.Add((false, eSQLRoles.diskadmin));
            if (!desiredprivs.Contains(eSQLRoles.bulkadmin) && existingprivs.Contains(eSQLRoles.bulkadmin))
                pcl.Add((false, eSQLRoles.bulkadmin));
            if (!desiredprivs.Contains(eSQLRoles.setupadmin) && existingprivs.Contains(eSQLRoles.setupadmin))
                pcl.Add((false, eSQLRoles.setupadmin));
            if (!desiredprivs.Contains(eSQLRoles.processadmin) && existingprivs.Contains(eSQLRoles.processadmin))
                pcl.Add((false, eSQLRoles.processadmin));
            if (!desiredprivs.Contains(eSQLRoles.serveradmin) && existingprivs.Contains(eSQLRoles.serveradmin))
                pcl.Add((false, eSQLRoles.serveradmin));
            if (!desiredprivs.Contains(eSQLRoles.dbcreator) && existingprivs.Contains(eSQLRoles.dbcreator))
                pcl.Add((false, eSQLRoles.dbcreator));

            return pcl;
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
        /// Retrieves the list of primary key constraints for the given table.
        /// NOTE: This command must be executed on a connection with the given database, not to the system database, postgres.
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="tableName"></param>
        /// <param name="pklist"></param>
        /// <returns></returns>
        public int Get_PrimaryKeyConstraints_forTable(string database, string tableName, out List<PriKeyConstraint> pklist)
        {
            System.Data.DataTable dt = null;
            pklist = new List<PriKeyConstraint>();

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_PrimaryKeyConstraints_forTable)} - " +
                    $"Attempting to get primary key constraints for table {tableName ?? ""}...");

                if(string.IsNullOrWhiteSpace(database))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_PrimaryKeyConstraints_forTable)} - " +
                        $"Database name is empty.");

                    return -1;
                }
                if(string.IsNullOrWhiteSpace(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_PrimaryKeyConstraints_forTable)} - " +
                        $"Table name is empty.");

                    return -1;
                }

                // This action requires a connection to the target database.
                var dbdal = this.GetDatabaseDAL(database);
                if(dbdal == null)
                {
                    // Failed to connect to target database.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_PrimaryKeyConstraints_forTable)} - " +
                        "Failed to connect to target database.");

                    return -1;
                }

                // Verify the table exists...
                var restable = this.Is_Table_in_Database(database, tableName);
                if(restable != 1)
                {
                    // Table doesn't exist.

                    return 0;
                }

                // Compose the sql query we will perform.
                string sql = $@"SELECT
                                    kcu.TABLE_SCHEMA,
                                    kcu.TABLE_NAME,
                                    tco.CONSTRAINT_NAME,
                                    kcu.ORDINAL_POSITION AS POSITION,
                                    kcu.COLUMN_NAME AS KEY_COLUMN
                                FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS tco
                                JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE AS kcu
                                    ON kcu.CONSTRAINT_NAME = tco.CONSTRAINT_NAME
                                    AND kcu.CONSTRAINT_SCHEMA = tco.CONSTRAINT_SCHEMA
                                WHERE tco.CONSTRAINT_TYPE = 'PRIMARY KEY'
                                  AND kcu.TABLE_NAME = '{tableName}'
                                ORDER BY kcu.TABLE_SCHEMA, kcu.TABLE_NAME, kcu.ORDINAL_POSITION;";

                if (dbdal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get primary keys from the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_PrimaryKeyConstraints_forTable)} - " +
                        "Failed to get primary keys from the table.");

                    return -2;
                }
                // We have a datatable of primary keys.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No primary keys in the table.
                    // Or, the table doesn't exist.

                    return 1;
                }
                // If here, we have primary keys for the table.

                // Convert the raw list to our type...
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    var pk = new PriKeyConstraint();
                    pk.table_schema = r["TABLE_SCHEMA"] + "";
                    pk.table_name = r["TABLE_NAME"] + "";
                    pk.constraint_name = r["CONSTRAINT_NAME"] + "";
                    pk.key_column = r["KEY_COLUMN"] + "";

                    try
                    {
                        pk.position = Convert.ToInt32(r["POSITION"]);
                    }
                    catch(Exception e)
                    {
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_PrimaryKeyConstraints_forTable)} - " +
                            $"Exception occurred while converting primary key position for table ({(tableName ?? "")}).");

                        pk.position = -1;
                    }

                    pklist.Add(pk);
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_PrimaryKeyConstraints_forTable)} - " +
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

        /// <summary>
        /// Requires the given user name to begin with a letter or underscore, and contain letters, numbers, or underscores.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        static public bool UserNameIsValid(string val)
        {
            return StringIsAlphaNumberandUnderscore(val);
        }
        /// <summary>
        /// Requires the given column name to begin with a letter or underscore, and contain letters, numbers, or underscores.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        static public bool ColumnNameIsValid(string val)
        {
            return StringIsAlphaNumberandUnderscore(val);
        }
        /// <summary>
        /// Requires the given table name to begin with a letter or underscore, and contain letters, numbers, or underscores.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        static public bool TableNameIsValid(string val)
        {
            return StringIsAlphaNumberandUnderscore(val);
        }
        /// <summary>
        /// Requires the given database name to begin with a letter or underscore, and contain letters, numbers, or underscores.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        static public bool DatabaseNameIsValid(string val)
        {
            return StringIsAlphaNumberandUnderscore(val);
        }

        /// <summary>
        /// Checks that the given string begins with a letter or underscore, and contain letters, numbers, or underscores.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        static public bool StringIsAlphaNumberandUnderscore(string val)
        {
            if (string.IsNullOrWhiteSpace(val))
            {
                return false;
            }

            Regex regex = new Regex("^[A-Za-z_][A-Za-z0-9_]*$");
            if (regex.IsMatch(val))
            {
                return true;
            }

            return false;
        }

        /// <summary>
        /// Private method that confirms the connection has SA privileges.
        /// Returns 1 on success, negatives for errors.
        /// </summary>
        /// <param name="dal">DAL reference</param>
        /// <returns></returns>
        private (int res, string login, bool isadmin) _Confirm_SAPrivileges(MSSQL_DAL dal)
        {
            System.Data.DataTable dt = null;

            try
            {
                string sql = @$"SELECT SUSER_NAME() AS CurrentLogin,
                               IS_SRVROLEMEMBER('sysadmin') AS IsSysAdmin;";

                // Execute the SQL to see what privileges we have...
                if (dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to query for privileges.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to query for privileges."
                            , _classname);

                    return (-2, "", false);
                }

                // Ensure we got one row...
                if (dt.Rows.Count != 1)
                {
                    // Failed to query for privileges.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to query for privileges."
                            , _classname);

                    return (-2, "", false);
                }

                if(MSSQL_DAL.Recover_FieldValue_from_DBRow(dt.Rows[0], "CurrentLogin", out string login) != 1)
                {
                    // Failed to query for privileges.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to query for privileges."
                            , _classname);

                    return (-2, "", false);
                }
                if(MSSQL_DAL.Recover_FieldValue_from_DBRow(dt.Rows[0], "IsSysAdmin", out bool isadmin) != 1)
                {
                    // Failed to query for privileges.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to query for privileges."
                            , _classname);

                    return (-2, "", false);
                }

                return (1, login, isadmin);
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname);

                return (-20, "", false);
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


        private int priv_AddUsertoDatabase(string database, string username)
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

        private eSQLRoles Recover_SQLRoles_from_String(string groupName)
        {
            string tempstr = groupName.ToLower();

            // See what the given role is.
            if (tempstr == eSQLRoles.db_accessadmin.ToString())
                return eSQLRoles.db_accessadmin;
            else if (tempstr == eSQLRoles.db_backupoperator.ToString())
                return eSQLRoles.db_backupoperator;
            else if (tempstr == eSQLRoles.db_datareader.ToString())
                return eSQLRoles.db_datareader;
            else if (tempstr == eSQLRoles.db_datawriter.ToString())
                return eSQLRoles.db_datawriter;
            else if (tempstr == eSQLRoles.db_ddladmin.ToString())
                return eSQLRoles.db_ddladmin;
            else if (tempstr == eSQLRoles.db_denydatareader.ToString())
                return eSQLRoles.db_denydatareader;
            else if (tempstr == eSQLRoles.db_denydatawriter.ToString())
                return eSQLRoles.db_denydatawriter;
            else if (tempstr == eSQLRoles.db_owner.ToString())
                return eSQLRoles.db_owner;
            else if (tempstr == eSQLRoles.db_securityadmin.ToString())
                return eSQLRoles.db_securityadmin;
            else if (tempstr == eSQLRoles.sysadmin.ToString())
                return eSQLRoles.sysadmin;
            else if (tempstr == eSQLRoles.diskadmin.ToString())
                return eSQLRoles.diskadmin;
            else if (tempstr == eSQLRoles.bulkadmin.ToString())
                return eSQLRoles.bulkadmin;
            else if (tempstr == eSQLRoles.setupadmin.ToString())
                return eSQLRoles.setupadmin;
            else if (tempstr == eSQLRoles.processadmin.ToString())
                return eSQLRoles.processadmin;
            else if (tempstr == eSQLRoles.serveradmin.ToString())
                return eSQLRoles.serveradmin;
            else if (tempstr == eSQLRoles.dbcreator.ToString())
                return eSQLRoles.dbcreator;
            else
                return eSQLRoles.none;
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
