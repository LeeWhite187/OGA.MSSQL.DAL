using OGA.MSSQL.DAL;
using OGA.MSSQL.DAL_SP.Model;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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

        private OGA.MSSQL.MSSQL_DAL _dal;

        private bool disposedValue;

        #endregion


        #region Public Properties

        public int InstanceId { get; set; }
        public string HostName { get; set; }
        public string Service { get; set; }
        public string Database { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        #endregion


        #region ctor / dtor

        public MSSQL_Tools()
        {
            _instancecounter++;
            InstanceId = _instancecounter;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects)
                }

                try
                {
                    _dal?.Disconnect();
                }
                catch (Exception) { }
                try
                {
                    _dal?.Dispose();
                }
                catch (Exception) { }

                _dal = null;

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
            using(var dal = new MSSQL_DAL())
            {
                dal.host = HostName;
                dal.service = Service;
                dal.database = "master";
                dal.username = Username;
                dal.password = Password;

                return dal.Test_Connection();
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
        /// Should be called with a connection string, open to the master database.
        /// Returns 1 if database was found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Does_Database_Exist(string database)
        {
            object tempval = "";

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                // Create the sql query text.
                string sql = "SELECT database_id FROM sys.databases WHERE Name = '" + database + "'";

                // Make the call to get a databaseID if the database exists.
                if (_dal.Execute_Scalar(sql, System.Data.CommandType.Text, out tempval) != 1)
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
                    if(tempval == null)
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
                    , _classname, Database);

                return -20;
            }
            finally
            {
                try
                {
                    if(tempval != null && tempval is IDisposable)
                    {
                        ((IDisposable)tempval)?.Dispose();
                    }
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
        public int Is_Database_Present(string database)
        {
            System.Data.DataTable dt = null;

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get database names..."
                        , _classname, Database);

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database."
                            , _classname);

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                // Compose the sql query we will perform.
                string sql = "SELECT [name] FROM [master].[sys].[databases]";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get table names from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get database names from the database."
                            , _classname);

                    return -2;
                }
                // We have a datatable of database names.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // No databases on the server.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Did not get any database names. Something is wrong."
                            , _classname, database);

                    return -3;
                }
                // If here, we have database names.

                // See if we have a match to the given database name.
                foreach (System.Data.DataRow r in dt.Rows)
                {
                    string sss = r[0] + "";

                    if (sss.ToUpper() == database.ToUpper())
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
                    , _classname, Database);

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

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

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
                int res123 = _dal.Execute_NonQuery(sql);
                if (res123 != -1)
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
                    , _classname, Database);

                return -20;
            }
        }

        /// <summary>
        /// Drops connections to the given database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Set_Database_toSingleUser(string database)
        {
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

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
                int res12323 = _dal.Execute_NonQuery(sql);
                if (res12323 != -1)
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
                    , _classname, Database);

                return -20;
            }
        }

        /// <summary>
        /// Drops the given database from the SQL Server instance.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public int Drop_Database(string database)
        {
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

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
                if (Get_Backend_Filepaths_for_Database(database, out var paths) != 1)
                {
                    // Failed to get backend filepaths for database.
                    return -3;
                }
                // We have the backend filepaths.

                string sql = "DROP DATABASE " + database + ";";
                // We have the sql script to run.

                // Execute it on the sql server instance.
                int res12323 = _dal.Execute_NonQuery(sql);
                if (res12323 != -1)
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
                    if(resenable != 1)
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
                            if(resdel != 1)
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
                    if(resdisable != 1)
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
                    , _classname, Database);

                return -20;
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
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

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

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database.",
                        _classname);

                    return -1;
                }
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database.",
                    _classname);

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Making the call to backup the database.",
                    _classname);
                // NOTE: THIS CALL SHOULD RETURN -1 FOR SUCCESS, STRAIGHT FROM SQL.
                if (_dal.Execute_NonQuery(sql, 1700) != -1)
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
                    , _classname, Database);

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

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                if (_dal.Connect() != 1)
                {
                    // Failed to connect with SQL Server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect with the database."
                            , _classname);

                    return -1;
                }
                // We made a persistent connection.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to restore database {1} from file {2}..."
                        , _classname, database, filepath);

                try
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                        "Switching to master database..."
                            , _classname);
                    sql = "Use Master";
                    // NOTE: THIS CALL SHOULD RETURN -1 FOR SUCCESS, STRAIGHT FROM SQL.
                    if (_dal.Execute_NonQuery(sql) != -1)
                    {
                        // Failed to switch to the master database.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                            "Failed to switch to the master database."
                                , _classname);

                        return -1;
                    }
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                        "Switched to master Database."
                            , _classname);

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
                        if (_dal.Execute_NonQuery(sql) != -1)
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
                    if (_dal.Execute_NonQuery(sql) != -1)
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
                        "Switching to master database..."
                            , _classname);
                    sql = "Use Master";
                    // NOTE: THIS CALL SHOULD RETURN -1 FOR SUCCESS, STRAIGHT FROM SQL.
                    if (_dal.Execute_NonQuery(sql) != -1)
                    {
                        // Failed to switch to the master database.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                            "Failed to switch to the master database."
                                , _classname);

                        return -1;
                    }
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                        "Switched to master Database."
                            , _classname);

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                        "Changing database to multi user mode..."
                            , _classname);
                    // NOTE: THIS CALL SHOULD RETURN -1 FOR SUCCESS, STRAIGHT FROM SQL.
                    sql = "ALTER DATABASE " + database + " SET MULTI_USER;";
                    if (_dal.Execute_NonQuery(sql) != -1)
                    {
                        // Failed to switch to the master database.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                            "Failed to switch to the master database."
                                , _classname);

                        return -5;
                    }
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                        "Database changed to single user mode."
                            , _classname);
                }
                catch (System.Exception e)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                        "Exception occurred while attempting to restore the database."
                            , _classname);

                    return -10;
                }

                return 1;
            }
            finally
            {
                try
                {
                    _dal.Disconnect();
                } catch(Exception e) { }
            }
        }

        /// <summary>
        /// Retrieves a list of filepaths of the database log files, etc, for the given database.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="database"></param>
        /// <param name="filepaths"></param>
        /// <returns></returns>
        public int Get_Backend_Filepaths_for_Database(string database, out List<string> filepaths)
        {
            System.Data.DataTable dt = null;
            filepaths = null;

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

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                string sql = "SELECT [name], [physical_name] FROM sys.master_files " +
                               "WHERE database_id = (SELECT [database_id] FROM [master].[sys].[databases] WHERE [name] = '" + database + "')";

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get backend filepaths for database {1}..."
                        , _classname, Database);

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database."
                            , _classname);

                    return -1;
                }
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
                {
                    // Failed to get row counts from the database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to get filepaths from the database."
                            , _classname);

                    return -2;
                }
                // We have a datatable of row counts.

                // See if it contains anything.
                if (dt.Rows.Count == 0)
                {
                    // The database has no backend files.
                    // Return an error.

                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "No backend files found for database{1}. Database name might be wrong."
                            , _classname, Database);

                    return -3;
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

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, Database);

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


        #region User Management

        /// <summary>
        /// Returns 1 if found, 0 if not, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <returns></returns>
        public int Does_Login_Exist(string login)
        {
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            System.Data.DataTable dt = null;

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                    $"Attempting to check for login to database: {(Database ?? "")}...");

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                        $"Failed to connect to database: {(Database ?? "")}.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                    "We can connect to the database.");

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

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
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
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:-:{nameof(Does_Login_Exist)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

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
            if(res < 0)
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
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                    $"Attempting to add login to database: {(Database ?? "")}...");

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                        $"Failed to connect to database: {(Database ?? "")}.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                    $"We can connect to database: {(Database ?? "")}.");

                // Compose the sql query to get logins from the sql server instance.
                string sql = "CREATE LOGIN [" + login + "] FROM WINDOWS WITH DEFAULT_DATABASE =[master], DEFAULT_LANGUAGE =[us_english]";

                if (_dal.Execute_NonQuery(sql) != 1)
                {
                    // Failed to get logins from the sql server instance.
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
            if(res < 0)
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
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                    $"Attempting to add login to database: {(Database ?? "")}...");

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                        $"Failed to connect to database: {(Database ?? "")}.");

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:-:{nameof(Add_WindowsLogin)} - " +
                    $"We can connect to database: {(Database ?? "")}.");

                // Compose the sql query to add a login to the sql server instance.
                string sql = "CREATE LOGIN [" + login + "] WITH PASSWORD = '" + password + "', DEFAULT_DATABASE = [master], DEFAULT_LANGUAGE = [us_english];";

                if (_dal.Execute_NonQuery(sql) != 1)
                {
                    // Failed to get logins from the sql server instance.
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
        /// Adds a user to a particular database.
        /// Returns 1 on success, 0 if already set, negatives for errors.
        /// </summary>
        /// <param name="login">Account login string</param>
        /// <param name="database">Name of database</param>
        /// <param name="desiredroles">List of desired roles to add to user</param>
        /// <returns></returns>
        public int Add_User_to_Database(string login, string database, List<eDBRoles> desiredroles)
        {
            // Check that the user is in the logins list already.
            int res = this.Does_Login_Exist(login);
            if (res != 1)
            {
                // Error occurred.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Login not found found."
                        , _classname);

                return -1;
            }
            // Login exists.

            // See if the login is tied to the desired role already.
            if(this.Get_DBRoles_for_Login(login, database, out var foundroles) != 1)
            {
                // Failed to get roles for the user.
                return -2;
            }
            // We have roles for the user.

            List<eDBRoles> rolestoadd = new List<eDBRoles>();
            // See if the desired roles are in the list.
            foreach(var dr in desiredroles)
            {
                if(!foundroles.Contains(dr))
                {
                    // The current desired role is not in the found list.
                    // We need to add it.
                    rolestoadd.Add(dr);
                }
                else
                {
                    // The current desired role is already in the found list.
                }
            }
            // At this point, we have a list of roles to add.

            // We will add it.
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to add database roles for login..."
                        , _classname, Database);

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database."
                            , _classname);

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                if(rolestoadd.Count == 0)
                {
                    // Nothing to do.
                    return 1;
                }
                // At least one role has to be added.

                // Iterate each role and add them for the user in the database...
                foreach (var rta in rolestoadd)
                {
                    // Compose the sql query to add the database user and each needed role.
                    string sql = "IF NOT EXISTS (SELECT * FROM sys.database_principals WHERE name = N'" + login + "') " +
                                 "BEGIN " +
                                 "    CREATE USER[" + login + "] FOR LOGIN[" + login + "] " +
                                 "END; " +
                                 "EXEC sp_addrolemember N'" + rta.ToString() + "', N'" + login + "'";

                    if (_dal.Execute_NonQuery(sql) != 1)
                    {
                        // Failed to add database role to the sql server instance.
                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                            "Failed to add database role to the sql server instance."
                                , _classname);

                        return -2;
                    }
                }

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred."
                    , _classname, Database);

                return -20;
            }
        }

        /// <summary>
        /// Public call to delete a database user.
        /// Returns 1 for success. Negatives for errors.
        /// </summary>
        /// <param name="username"></param>
        /// <returns></returns>
        public int DeleteUser(string username)
        {
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUser)} - " +
                    $"Attempting to delete user...");

                if(string.IsNullOrWhiteSpace(username))
                {
                    // Empty username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUser)} - " +
                        $"Empty Username.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUser)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Delete the user...
                string sql = $"DROP USER IF EXISTS {username};";
                if (this._dal.Execute_NonQuery(sql) != -1)
                {
                    // Delete user command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUser)} - " +
                        "Delete user command failed.");

                    return -2;
                }

                // Check if the user was deleted...
                var resq = this.Does_Login_Exist(username);
                if(resq != 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUser)} - " +
                        "User was not confirmed as dropped.");

                    return -3;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DeleteUser)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

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
        public int ChangeUserPassword(string username, string password = "")
        {
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeUserPassword)} - " +
                    $"Attempting to create user...");

                if(string.IsNullOrWhiteSpace(username))
                {
                    // Empty Username.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeUserPassword)} - " +
                        $"Empty Username.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeUserPassword)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Change the user password...
                string sql = $"ALTER LOGIN [{username}] WITH PASSWORD = '{password}';";
                if (this._dal.Execute_NonQuery(sql) != -1)
                {
                    // Change user password command failed.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeUserPassword)} - " +
                        "Change user password command failed.");

                    return -2;
                }

                return 1;
            }
            catch(Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e,
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(ChangeUserPassword)} - " +
                    $"Exception occurred to database: {(Database ?? "")}");

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
            string sql = "";
            roles = null;

            // Compose the query that will pull database roles.
            sql = "USE MASTER " +
                    "GO " +
                    "BEGIN " +
                    "DECLARE @SQLVerNo INT; " +
                    "            SET @SQLVerNo = cast(substring(CAST(Serverproperty('ProductVersion') AS VARCHAR(50)), 0, charindex('.', CAST(Serverproperty('ProductVersion') AS VARCHAR(50)), 0)) as int); " +
                    "            IF @SQLVerNo >= 9 " +
                    "    IF EXISTS(SELECT TOP 1 * " +
                    "              FROM Tempdb.sys.objects(nolock) " +
                    "               WHERE name LIKE '#TUser%') " +
                    "        DROP TABLE #TUser " +
                    "ELSE " +
                    "IF @SQLVerNo = 8 " +
                    "BEGIN " +
                    "    IF EXISTS(SELECT TOP 1 * " +
                    "               FROM Tempdb.dbo.sysobjects(nolock) " +
                    "               WHERE name LIKE '#TUser%') " +
                    "        DROP TABLE #TUser " +
                    "END " +
                    "CREATE TABLE #TUser ( " +
                    "    ServerName    varchar(256), " +
                    "    DBName SYSNAME, " +
                    "    [Name]        SYSNAME, " +
                    "    GroupName SYSNAME NULL, " +
                    "    LoginName SYSNAME NULL, " +
                    "    default_database_name SYSNAME NULL, " +
                    "    default_schema_name VARCHAR(256) NULL, " +
                    "    Principal_id INT, " +
                    "    [sid]         VARBINARY(85)) " +
                    "IF @SQLVerNo = 8 " +
                    "BEGIN " +
                    "    INSERT INTO #TUser " +
                    "	EXEC sp_MSForEachdb " +
                    "    ' " +
                    "     SELECT " +
                    "	   @@SERVERNAME, " +
                    "       '' ? '' as DBName, " +
                    "       u.name As UserName, " +
                    "      CASE WHEN(r.uid IS NULL) THEN ''public'' ELSE r.name END AS GroupName, " +
                    "      l.name AS LoginName, " +
                    "      NULL AS Default_db_Name, " +
                    "      NULL as default_Schema_name, " +
                    "      u.uid, " +
                    "      u.sid " +
                    "    FROM [?].dbo.sysUsers u " +
                    "      LEFT JOIN ([?].dbo.sysMembers m " +
                    "      JOIN[?].dbo.sysUsers r " +
                    "      ON m.groupuid = r.uid) " +
                    "       ON m.memberuid = u.uid " +
                    "       LEFT JOIN dbo.sysLogins l " +
                    "       ON u.sid = l.sid " +
                    "     WHERE u.islogin = 1 OR u.isntname = 1 OR u.isntgroup = 1 " +
                    "       /*and u.name like ''tester''*/ " +
                    "     ORDER BY u.name " +
                    "	' " +
                    "END " +
                    "ELSE " +
                    "IF @SQLVerNo >= 9 " +
                    "BEGIN " +
                    "    INSERT INTO #TUser " +
                    "	EXEC sp_MSForEachdb " +
                    "	' " +
                    "     SELECT " +
                    "	   @@SERVERNAME, " +
                    "	   ''?'', " +
                    "       u.name, " +
                    "       CASE WHEN (r.principal_id IS NULL) THEN ''public'' ELSE r.name END GroupName, " +
                    "       l.name LoginName, " +
                    "       l.default_database_name, " +
                    "       u.default_schema_name, " +
                    "       u.principal_id, " +
                    "       u.sid " +
                    "     FROM [?].sys.database_principals u " +
                    "       LEFT JOIN ([?].sys.database_role_members m " +
                    "       JOIN[?].sys.database_principals r " +
                    "       ON m.role_principal_id = r.principal_id) " +
                    "       ON m.member_principal_id = u.principal_id " +
                    "       LEFT JOIN[?].sys.server_principals l " +
                    "       ON u.sid = l.sid " +
                    "     WHERE u.TYPE<> ''R'' " +
                    "       /*and u.name like ''tester''*/ " +
                    "     order by u.name " +
                    "	 ' " +
                    "END " +
                    "SELECT* " +
                    "FROM #TUser " +
                    "WHERE DBName NOT IN ('master', 'msdb', 'tempdb', 'model') " +
                    "ORDER BY DBName, " +
                    " [name], " +
                    " GroupName " +
                    "DROP TABLE #TUser " +
                    "END";

            // First, see if the database exists...
            int res = Is_Database_Present(database);
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
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get database roles..."
                        , _classname, Database);

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database."
                            , _classname);

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
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

                    role.ServerName = r["ServerName"] + "";
                    role.DBName = r["DBName"] + "";
                    role.UserName = r["Name"] + "";
                    role.GroupName = r["GroupName"] + "";
                    role.LoginName = r["LoginName"] + "";
                    role.Default_Database_Name = r["default_database_name"] + "";
                    role.Default_Schema_Name = r["default_schema_name"] + "";
                    role.Principal_ID = r["Principal_id"] + "";
                    role.SID = r["sid"] + "";

                    roles.Add(role);
                }
                // If here, we got a list of roles for the database.

                return 1;
            }
            catch (Exception e)
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(e, "{0}: " +
                    "Exception occurred"
                    , _classname, Database);

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
        /// <param name="databaseName"></param>
        /// <param name="tablelist"></param>
        /// <returns></returns>
        public int Get_TableList_forDatabase(string databaseName, out List<string> tablelist)
        {
            System.Data.DataTable dt = null;
            tablelist = new List<string>();

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_TableList_forDatabase)} - " +
                    $"Attempting to get table names for database {databaseName ?? ""}...");

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_TableList_forDatabase)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Compose the sql query we will perform.
                string sql = $"SELECT TABLE_NAME " +
                             $"FROM INFORMATION_SCHEMA.TABLES " +
                             $"WHERE TABLE_CATALOG = '{databaseName}' " +
                             $"AND TABLE_TYPE = 'BASE TABLE' " +
                             $"AND TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'sys'); ";


                if (_dal.Execute_Table_Query(sql, out dt) != 1)
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

            if(_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get table names for database {1}..."
                        , _classname, Database);

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database."
                            , _classname);

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                // Compose the sql query we will perform.
                string sql = "SELECT TABLE_NAME FROM information_schema.tables WHERE [TABLE_TYPE] = 'BASE TABLE'";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
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
                    , _classname, Database);

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
        /// <param name="tablename"></param>
        /// <returns></returns>
        public int Get_TableSize(string tablename)
        {
            if(Get_RowCount_for_Tables(out var rowdata) != 1)
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
            catch(Exception e)
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
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int DoesTableExist(string tableName)
        {
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                    $"Attempting to query if table ({(tableName ?? "")}) exists...");

                if(string.IsNullOrWhiteSpace(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Get the table list...
                var res2 = this.Get_TableList_forDatabase(Database, out var tl);
                if(res2 != 1 || tl == null)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(DoesTableExist)} - " +
                        $"Table Not Found.");

                    return -1;
                }
                if(!tl.Contains(tableName))
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
        /// <param name="tabledef"></param>
        /// <returns></returns>
        public int Create_Table(TableDefinition tabledef)
        {
            string sql = "";

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            if(tabledef == null)
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

                if(string.IsNullOrWhiteSpace(tabledef.tablename))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }
                // Table name is set.

                // Check if the table exists or not...
                var res2 = this.DoesTableExist(tabledef.tablename);
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
                int res123 = _dal.Execute_NonQuery(sql);
                if (res123 != -1)
                {
                    // Error occurred while adding the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Create_Table)} - " +
                        "Error occurred while adding the table.");

                    return -4;
                }

                // Check if the table is now present on the server.
                if (this.DoesTableExist(tabledef.tablename) != 1)
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
        /// <param name="tableName"></param>
        /// <returns></returns>
        public int Drop_Table(string tableName)
        {
            string sql = "";

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                    $"Attempting to drop table ({(tableName ?? "")})...");

                if(string.IsNullOrWhiteSpace(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        $"Empty table name.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        $"Failed to connect to server.");

                    return -1;
                }

                // Check if the table exists or not...
                var res2 = this.DoesTableExist(tableName);
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
                int res123 = _dal.Execute_NonQuery(sql);
                if (res123 != -1)
                {
                    // Error occurred while dropping the table.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Drop_Table)} - " +
                        "Error occurred while dropping the table.");

                    return -4;
                }

                // Check if the table is still present on the server.
                if (this.DoesTableExist(tableName) != 1)
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
        /// <param name="rowdata"></param>
        /// <returns></returns>
        public int Get_RowCount_for_Tables(out List<KeyValuePair<string, int>> rowdata)
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

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get table row counts for database {1}..."
                        , _classname, Database);

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database."
                            , _classname);

                    return -1;
                }
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                // Compose the sql query we will perform.
                string sql = "SELECT SCHEMA_NAME(schema_id) AS[SchemaName],[Tables].name AS[TableName],SUM([Partitions].[rows]) AS[TotalRowCount] " +
                    "FROM sys.tables AS [Tables] JOIN sys.partitions AS [Partitions] ON [Tables].[object_id] = [Partitions].[object_id] AND [Partitions].index_id IN ( 0, 1 ) " +
                    "GROUP BY SCHEMA_NAME(schema_id), [Tables].name";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
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
                            , _classname, Database);

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
                                , _classname, Database);

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
                    , _classname, Database);

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
        /// <param name="tablename"></param>
        /// <param name="ColumnNames"></param>
        /// <returns></returns>
        public int Get_Columns_for_Table(string tablename, out List<string> ColumnNames)
        {
            System.Data.DataTable dt = null;
            ColumnNames = null;

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "Attempting to get column names for table {2} of database {1}..."
                        , _classname, Database, tablename);

                // See if we can connect to the database.
                if (_dal.Test_Connection() != 1)
                {
                    // Failed to connect to database.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                        "Failed to connect to database."
                            , _classname);

                    return -1;
                }

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info("{0}: " +
                    "We can connect to the database."
                        , _classname);

                // Compose the sql query we will perform.
                string sql = "SELECT [name] FROM sys.columns WHERE object_id = OBJECT_ID('" + tablename + "')";

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
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
                    , _classname, Database);

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
        /// <param name="tableName"></param>
        /// <param name="columnlist"></param>
        /// <returns></returns>
        public int Get_ColumnInfo_forTable(string tableName, out List<ColumnInfo> columnlist)
        {
            System.Data.DataTable dt = null;
            columnlist = new List<ColumnInfo>();

            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = Database;
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Info(
                    $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                    $"Attempting to get column info for table {tableName ?? ""}...");

                if(string.IsNullOrWhiteSpace(tableName))
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                        $"Table name is empty.");

                    return -1;
                }

                // Connect to the database...
                var resconn = this._dal.Connect();
                if(resconn != 1)
                {
                    // Failed to connect to server.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                        $"Failed to connect to server.");

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

                if (_dal.Execute_Table_Query(sql, out dt) != 1)
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
                    var restable = this.DoesTableExist(tableName);
                    if(restable != 1)
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
                    if(res1a != 1)
                    {
                        // Failed to get column_name column value.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                            $"Failed to get column_name column value.");

                        return -22;
                    }
                    ct.name = val + "";

                    var res1b = MSSQL_DAL.Recover_FieldValue_from_DBRow(r, "data_type", out string val2);
                    if(res1b != 1)
                    {
                        // Failed to get data_type column value.

                        OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                            $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                            $"Failed to get data_type column value.");

                        return -22;
                    }
                    ct.dataType = val2 + "";

                    var res1 = MSSQL_DAL.Recover_FieldValue_from_DBRow(r, "ordinal_position", out int displayorder);
                    if(res1 != 1)
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
                        if(res5 != 1)
                        {
                            // Failed to get is_nullable column value.

                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                                $"Failed to get is_nullable column value.");

                            return -22;
                        }

                        if(nullable)
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
                        if(res5 != 1)
                        {
                            // Failed to get is_identity column value.

                            OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                                $"{_classname}:{this.InstanceId.ToString()}:{nameof(Get_ColumnInfo_forTable)} - " +
                                $"Failed to get is_identity column value.");

                            return -22;
                        }

                        if(!isidentity)
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
        /// Attempts to enable command shell access on the SQL Engine.
        /// </summary>
        /// <returns></returns>
        private int SQLEngine_EnableCmdShell()
        {
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                // Enable advanced options...
                string sql1 = "EXEC sp_configure 'show advanced options', 1; RECONFIGURE;";
                // We have the sql script to run.

                // Execute it on the sql server instance...
                int res1 = _dal.Execute_NonQuery(sql1);
                if (res1 != -1)
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
                int res2 = _dal.Execute_NonQuery(sql2);
                if (res2 != -1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while enabling command shell access on SQL server."
                        , _classname);

                    return -1;
                }

                return 1;
            }
            catch(Exception e)
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
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
                // Disable command shell...
                string sql2 = "EXEC sp_configure 'xp_cmdshell', 0; RECONFIGURE;";
                // We have the sql script to run.

                // Execute it on the sql server instance...
                int res2 = _dal.Execute_NonQuery(sql2);
                if (res2 != -1)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while disabling command shell access on SQL server."
                        , _classname);

                    return -1;
                }

                return 1;
            }
            catch(Exception e)
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
            if (_dal == null)
            {
                _dal = new MSSQL_DAL();
                _dal.host = HostName;
                _dal.service = Service;
                _dal.database = "master";
                _dal.username = Username;
                _dal.password = Password;
            }

            try
            {
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
                int res = _dal.Execute_Scalar(sql, System.Data.CommandType.Text, out var result);
                if (res != 0)
                {
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error("{0}: " +
                    "Error occurred while performing file delete on SQL server."
                        , _classname);

                    return -1;
                }

                return 1;
            }
            catch(Exception e)
            {
                return -10;
            }
        }

        private string Get_FullyQualified_SQLHostName()
        {
            if(this.Service.Trim().Length > 0)
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

        #endregion
    }
}
