using Microsoft.VisualStudio.TestTools.UnitTesting;
using OGA.MSSQL;
using OGA.SharedKernel.Process;
using OGA.SharedKernel;
using OGA.Testing.Lib;
using System;
using System.Collections.Generic;
using System.Web;
using OGA.Common.Config.structs;
using System.Threading.Tasks;
using OGA.MSSQL.DAL;
using System.Linq;
using OGA.MSSQL.DAL_SP_Tests.Helpers;
using Mono.Unix.Native;
using System.Data.Common;
using System.Xml.Linq;
using System.Security.Cryptography;

namespace OGA.MSSQL_Tests
{
    /*  Unit Tests for SQL Server MSSQL_Tools class.

        //  Test_1_1_1  Verify that we can connect to the test SQL Server database with the test admin creds.
        //  Test_1_1_2  Verify that connection fails to the test MSSQL database with the bad admin creds.
        //  Test_1_1_3  Verify that a call to TestConnection() closes and disposes its connection.
        //  Test_1_1_4  Verify that a call to TestConnection_toDatabase() closes and disposes its connection.

        //  Test_1_2_1  Verify we can query for the owner of a database.
        //  Test_1_2_2  Verify we can change the owner of a database.
        //  Test_1_2_3  Verify that a test user can connect to a test database if he is given ownership of it.

        //  Test_1_4_1  Verify that we can connect to the test a specific database with a test login that is a user of the database.
        //  Test_1_4_2  Verify that we can connect to the test a specific database with a test login that's not a user of the database.

        //  Test_1_5_1  Verify that we can create a local test user, and are able to change its password.

        //  Test_1_9_1  Verify that we can get the folder path of the data folder.
        //  Test_1_9_2  Verify that we can get the folder path of the log folder.
        //  Test_1_9_3  Verify that we can get the folder path of a database.

        //  Test_1_10_1  Verify that we can get a list of tables for a given database.

        //  Test_1_11_1  Verify that we can get a list of databases on the MSSQL host.


FINISH TESTS FROM HERE DOWN...

    Move all the user management tests to the UserMgmt_Tests class.


        //  Test_1_3_1  Verify we can get the primary key column data for a table.
     
        //  Test_1_8_1  Verify that we can create a database whose name doesn't already exist.
        //  Test_1_8_2  Verify that we cannot create a database whose name already exists.
        //  Test_1_8_3  Verify that we can verify if a database exists.
        //  Test_1_8_4  Verify that we can delete a database that exists.
        //  Test_1_8_5  Verify that we cannot delete a database with an unknown name.
        //  Test_1_8_6  Verify that a user without CreateDB is not allowed to create a database.


    ADD test to verify that adding a local user to the sql engine with a bad password gives a specific error.
    ADD test to verify that adding a local user to the sql engine with a good password gives a specific error.


    Add test to verify we can remove a user from a database.

    Add test to verify we can get the configured db roles for a database.
    Add test to verify we can query for user privileges to a database.
    Add test to verify we can set a particular privilege for a user of a database.
    Add tests to verify the addition and presence, before and after, of each db role for a user in a database.

    Add test to verify that we can add a user to a database, with a specific set of roles.
    Add test to verify that we can add a user to a database, with no specified roles, and see if this should return an error as result.

    Add test to verify we can set a database to single user mode, and we can confirm the mode it's in.
    Add test to verify we can set a database to multi user mode, and we can confirm the mode it's in.

    Add test to verify we can backup a database and restore it.


    Add test to verify the Does_User_Exist_forDatabase() call works, in both directions Found and not found.
    And, verify how Does_User_Exist_forDatabase() behaves when the database doesn't exist.


     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class MSSQL_Tools_Tests : ProjectTest_Base
    {
        #region Setup

        /// <summary>
        /// This will perform any test setup before the first class tests start.
        /// This exists, because MSTest won't call the class setup method in a base class.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test class setup method of the base.
        /// </summary>
        [ClassInitialize]
        static public void TestClass_Setup(TestContext context)
        {
            TestClassBase_Setup(context);
        }
        /// <summary>
        /// This will cleanup resources after all class tests have completed.
        /// This exists, because MSTest won't call the class cleanup method in a base class.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test class cleanup method of the base.
        /// </summary>
        [ClassCleanup]
        static public void TestClass_Cleanup()
        {
            TestClassBase_Cleanup();
        }

        /// <summary>
        /// Called before each test runs.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test setup method of the base.
        /// </summary>
        [TestInitialize]
        override public void Setup()
        {
            //// Push the TestContext instance that we received at the start of the current test, into the common property of the test base class...
            //Test_Base.TestContext = TestContext;

            base.Setup();

            // Runs before each test. (Optional)

            // Retrieve database server creds...
            this.GetTestDatabaseUserCreds();
        }

        /// <summary>
        /// Called after each test runs.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test cleanup method of the base.
        /// </summary>
        [TestCleanup]
        override public void TearDown()
        {
            // Runs after each test. (Optional)

            base.TearDown();
        }

        #endregion


        //  Test_1_1_1  Verify that we can connect to the test SQL Server database with the test admin creds.
        [TestMethod]
        public async Task Test_1_1_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = new MSSQL_Tools();
                pt.Username = dbcreds.User;
                pt.Service = dbcreds.Service;
                pt.HostName = dbcreds.Host;
                pt.Password = dbcreds.Password;

                var res = pt.TestConnection();
                if (res != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_2  Verify that connection fails to the test SQL Server database with the bad admin creds.
        [TestMethod]
        public async Task Test_1_1_2()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = new MSSQL_Tools();
                pt.Username = dbcreds.User;
                pt.Service = dbcreds.Service;
                pt.HostName = dbcreds.Host;
                pt.Password = dbcreds.Password + "f";

                var res = pt.TestConnection();
                if (res != -1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_3  Verify that a call to TestConnection() closes and disposes its connection.
        [TestMethod]
        public async Task Test_1_1_3()
        {
            MSSQL_Tools ptadmin = null;
            MSSQL_Tools pttest = null;

            try
            {
                // Create the tools instance...
                ptadmin = Get_ToolInstance_forMaster();
                //ptadmin.Cfg_ClearConnectionPoolOnClose = true;

                // Get how many connections are open to master...
                var resm1 = ptadmin.GetConnectionCountforDatabase("master");
                if(resm1.res != 1)
                        Assert.Fail("Wrong Value");
                int countbaseline = resm1.count;


                // Now, create a second tool instance that we will test with...
                pttest = Get_ToolInstance_forMaster();


                // Verify the connection count hasn't changed...
                {
                    var resm2 = ptadmin.GetConnectionCountforDatabase("master");
                    if(resm2.res != 1)
                            Assert.Fail("Wrong Value");

                    // Ensure the connection count hasn't changed...
                    if(resm2.count != countbaseline)
                            Assert.Fail("Wrong Value");
                }


                // Attempt a test connection with the second tool instance...
                var resconntest = pttest.TestConnection();
                    if(resconntest != 1)
                            Assert.Fail("Wrong Value");


                // Verify the connection count hasn't changed...
                {
                    var resm2 = ptadmin.GetConnectionCountforDatabase("master");
                    if(resm2.res != 1)
                            Assert.Fail("Wrong Value");

                    // Ensure the connection count hasn't changed...
                    if(resm2.count != countbaseline)
                            Assert.Fail("Wrong Value");
                }


                // Dispose the test tool instance...
                pttest.Dispose();


                // Verify the connection count hasn't changed...
                {
                    var resm2 = ptadmin.GetConnectionCountforDatabase("master");
                    if(resm2.res != 1)
                            Assert.Fail("Wrong Value");

                    // Ensure the connection count hasn't changed...
                    if(resm2.count != countbaseline)
                            Assert.Fail("Wrong Value");
                }
            }
            finally
            {
                ptadmin?.Dispose();
                pttest?.Dispose();
            }
        }

        //  Test_1_1_4  Verify that a call to TestConnection_toDatabase() closes and disposes its connection.
        [TestMethod]
        public async Task Test_1_1_4()
        {
            MSSQL_Tools ptadmin = null;
            MSSQL_Tools pttest = null;

            try
            {
                // Create the tools instance...
                ptadmin = Get_ToolInstance_forMaster();
                //ptadmin.Cfg_ClearConnectionPoolOnClose = true;


                // Create a test database...
                string dbname = this.GenerateDatabaseName();
                {
                    // Create the test database...
                    var res2 = ptadmin.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = ptadmin.Does_Database_Exist(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }


                // Get how many connections are open to the test database...
                var resm1 = ptadmin.GetConnectionCountforDatabase(dbname);
                if(resm1.res != 1)
                        Assert.Fail("Wrong Value");
                int countbaseline = resm1.count;


                // Now, create a second tool instance that we will test with...
                pttest = Get_ToolInstance_forMaster();


                // Verify the connection count hasn't changed...
                {
                    var resm2 = ptadmin.GetConnectionCountforDatabase(dbname);
                    if(resm2.res != 1)
                            Assert.Fail("Wrong Value");

                    // Ensure the connection count hasn't changed...
                    if(resm2.count != countbaseline)
                            Assert.Fail("Wrong Value");
                }


                // Attempt a test connection with the test database...
                var resconntest = pttest.TestConnection_toDatabase(dbname);
                    if(resconntest != 1)
                            Assert.Fail("Wrong Value");


                // Verify the connection count hasn't changed...
                {
                    var resm2 = ptadmin.GetConnectionCountforDatabase(dbname);
                    if(resm2.res != 1)
                            Assert.Fail("Wrong Value");

                    // Ensure the connection count hasn't changed...
                    if(resm2.count != countbaseline)
                            Assert.Fail("Wrong Value");
                }


                // Dispose the test tool instance...
                pttest.Dispose();


                // Verify the connection count hasn't changed...
                {
                    var resm2 = ptadmin.GetConnectionCountforDatabase(dbname);
                    if(resm2.res != 1)
                            Assert.Fail("Wrong Value");

                    // Ensure the connection count hasn't changed...
                    if(resm2.count != countbaseline)
                            Assert.Fail("Wrong Value");
                }

                // Delete the test database...
                {
                    // Delete the database...
                    var res4 = ptadmin.Drop_Database(dbname);
                    if(res4 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database is no longer present...
                    var res5 = ptadmin.Does_Database_Exist(dbname);
                    if(res5 != 0)
                        Assert.Fail("Wrong Value");
                }
            }
            finally
            {
                ptadmin?.Dispose();
                pttest?.Dispose();
            }
        }

        //  Test_1_2_1  Verify we can query for the owner of a database.
        [TestMethod]
        public async Task Test_1_2_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                string dbname = this.GenerateDatabaseName();

                // Check that the database doesn't exist...
                var res1 = pt.Does_Database_Exist(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Does_Database_Exist(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");


                // Get the database owner...
                var reso = pt.Get_DatabaseOwner(dbname);
                if(reso.res != 1 || string.IsNullOrWhiteSpace(reso.owner))
                    Assert.Fail("Wrong Value");

                // Verify the owner is our user that created it...
                if (reso.owner != dbcreds.User)
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Does_Database_Exist(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_2_2  Verify we can change the owner of a database.
        [TestMethod]
        public async Task Test_1_2_2()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                string dbname = this.GenerateDatabaseName();

                // Check that the database doesn't exist...
                var res1 = pt.Does_Database_Exist(dbname);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Create the test database...
                var res2 = pt.Create_Database(dbname);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database now exists...
                var res3 = pt.Does_Database_Exist(dbname);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");


                // Get the database owner...
                var reso = pt.Get_DatabaseOwner(dbname);
                if(reso.res != 1 || string.IsNullOrWhiteSpace(reso.owner))
                    Assert.Fail("Wrong Value");

                // Verify the owner is our user that created it...
                if (reso.owner != dbcreds.User)
                    Assert.Fail("Wrong Value");


                // Create a second database user...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Transfer ownership to the second user...
                var reschg = pt.ChangeDatabaseOwner(dbname, mortaluser1);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Verify the database owner was changed...
                var resver = pt.Get_DatabaseOwner(dbname);
                if(resver.res != 1 || string.IsNullOrWhiteSpace(resver.owner))
                    Assert.Fail("Wrong Value");
 
                if(resver.owner != mortaluser1)
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Does_Database_Exist(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Remove the user...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_2_3  Verify that a test user can connect to a test database if he is given ownership of it.
        [TestMethod]
        public async Task Test_1_2_3()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a test database...
                var dbname = this.CreateTestDatabase(pt);

                // Create a test user...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Transfer ownership to the test user...
                var reschg = pt.ChangeDatabaseOwner(dbname, mortaluser1);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Verify the database owner was changed...
                {
                    var resver = pt.Get_DatabaseOwner(dbname);
                    if(resver.res != 1 || string.IsNullOrWhiteSpace(resver.owner))
                        Assert.Fail("Wrong Value");
 
                    if(resver.owner != mortaluser1)
                        Assert.Fail("Wrong Value");
                }

                // Wait a tick, to let things catch up...
                await Task.Delay(500);

                // Verify that the user can connect to the test database...
                {
                    var pt1 = new MSSQL_Tools();
                    pt1.HostName = dbcreds.Host;
                    pt1.Service = dbcreds.Service;
                    pt1.Username = mortaluser1;
                    pt1.Password = mortaluser1_password;
                    var restest = pt1.TestConnection_toDatabase(dbname);
                    if(restest != 1)
                        Assert.Fail("Wrong Value");
                    pt1.Dispose();
                    await Task.Delay(500);
                }

                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Does_Database_Exist(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Remove the user...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_4_1  Verify that we can connect to the test a specific database with a test login that is a user of the database.
        [TestMethod]
        public async Task Test_1_4_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();
                pt.Cfg_ClearConnectionPoolOnClose = true;

                // Create a test database...
                var dbname = this.CreateTestDatabase(pt);

                // Create a test user...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Add the user to the test database...
                // NOTE: We don't specify any db roles for the user, here.
                // So, we are simply adding the user to the database.
                var resadd = pt.Add_User_to_Database(mortaluser1, dbname);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Verify that the user can connect to the test database...
                {
                    var pt1 = new MSSQL_Tools();
                    pt1.Username = mortaluser1;
                    pt1.Service = dbcreds.Service;
                    pt1.HostName = dbcreds.Host;
                    pt1.Password = mortaluser1_password;
                    pt1.Cfg_ClearConnectionPoolOnClose = true;
                    var restest = pt1.TestConnection_toDatabase(dbname);
                    if(restest != 1)
                        Assert.Fail("Wrong Value");
                    pt1.Dispose();
                }


                // Before we attempt to delete the database, we need to realize that our tool instance has a specific connection to the test database.
                // This is because the Add_User_to_Database connects to the database during its work.
                // So, we need to dispose and create a new instance...
                pt.Dispose();
                await Task.Delay(500);
                pt = Get_ToolInstance_forMaster();
                pt.Cfg_ClearConnectionPoolOnClose = true;


                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Does_Database_Exist(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Remove the user...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_4_2  Verify that we can connect to the test a specific database with a test login that's not a user of the database.
        [TestMethod]
        public async Task Test_1_4_2()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a test database...
                var dbname = this.CreateTestDatabase(pt);

                // Create a test user...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Verify that the user can NOT connect to the test database...
                {
                    var pt1 = new MSSQL_Tools();
                    pt1.Username = mortaluser1;
                    pt1.Service = dbcreds.Service;
                    pt1.HostName = dbcreds.Host;
                    pt1.Password = mortaluser1_password;
                    var restest = pt1.TestConnection_toDatabase(dbname);
                    if(restest != -1)
                        Assert.Fail("Wrong Value");
                    pt1.Dispose();
                }


                // Delete the database...
                var res4 = pt.Drop_Database(dbname);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = pt.Does_Database_Exist(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Remove the user...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_5_1  Verify that we can create a local test user, and are able to change its password.
        [TestMethod]
        public async Task Test_1_5_1()
        {
            MSSQL_Tools ptadmin = null;
            MSSQL_Tools usertool = null;

            try
            {
                // Create the tools instance...
                ptadmin = Get_ToolInstance_forMaster();
                //ptadmin.Cfg_ClearConnectionPoolOnClose = true;


                // Create a test user...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = ptadmin.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Create a tool instance for the test user...
                usertool = new MSSQL_Tools();
                usertool.HostName = dbcreds.Host;
                usertool.Service = dbcreds.Service;
                usertool.Username = mortaluser1;
                usertool.Password = mortaluser1_password;


                // Verify the user can connect to the SQL host...
                var resconntest = usertool.TestConnection();
                    if(resconntest != 1)
                            Assert.Fail("Wrong Value");


                // Change the user's password...
                string mortaluser1_passwordnew = this.GenerateUserPassword();
                var reschg = ptadmin.ChangeLoginPassword(mortaluser1, mortaluser1_passwordnew);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Recycle the test user's tool instance...
                usertool.Dispose();
                usertool = new MSSQL_Tools();
                usertool.HostName = dbcreds.Host;
                usertool.Service = dbcreds.Service;
                usertool.Username = mortaluser1;
                usertool.Password = mortaluser1_passwordnew;


                // Verify the user can connect to the SQL host with the new password...
                var resconntest2 = usertool.TestConnection();
                if(resconntest2 != 1)
                        Assert.Fail("Wrong Value");

                // Dispose the user's tool instance...
                usertool.Dispose();

                // Remove the user...
                var resdeluser = ptadmin.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                ptadmin?.Dispose();
                usertool?.Dispose();
            }
        }


        //  Test_1_9_1  Verify that we can get the folder path of the data folder.
        [TestMethod]
        public async Task Test_1_9_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = this.Get_ToolInstance_forMaster();

                // Get the data folder path...
                var res = pt.Get_DefaultDataDirectory();
                if(res.res != 1 || res.folderpath == null)
                    Assert.Fail("Wrong Value");

                if(res.folderpath != "E:\\SQLData\\MSSQL14.SQLEXPRESS\\MSSQL\\DATA\\")
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_9_2  Verify that we can get the folder path of the log folder.
        [TestMethod]
        public async Task Test_1_9_2()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = this.Get_ToolInstance_forMaster();

                // Get the data folder path...
                var res = pt.Get_DefaultLogDirectory();
                if(res.res != 1 || res.folderpath == null)
                    Assert.Fail("Wrong Value");

                if(res.folderpath != "E:\\SQLData\\MSSQL14.SQLEXPRESS\\MSSQL\\DATA\\")
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_9_3  Verify that we can get the folder path of a database.
        [TestMethod]
        public async Task Test_1_9_3()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = this.Get_ToolInstance_forMaster();


                // Create a test database...
                string dbname = this.GenerateDatabaseName();
                {
                    // Create the test database...
                    var res2 = pt.Create_Database(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database now exists...
                    var res3 = pt.Does_Database_Exist(dbname);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Get the default path for data...
                string folderpath = "";
                {
                    var resa = pt.Get_DefaultLogDirectory();
                    if(resa.res != 1 || resa.folderpath == null)
                        Assert.Fail("Wrong Value");

                    folderpath = resa.folderpath;
                }


                // Compose an expected path for data and log files...
                var expecteddatapath = System.IO.Path.Combine(folderpath, dbname + ".mdf");
                var expectedlogpath = System.IO.Path.Combine(folderpath, dbname + "_log.ldf");


                // Get the file paths for the database...
                var res = pt.Get_Backend_Filepaths_for_Database(dbname);
                if (res.res != 1 || res.filepaths == null)
                    Assert.Fail("Wrong Value");


                // Verify we found the data and log file paths...
                var discodatapath = res.filepaths.Where(n => n.EndsWith("mdf")).FirstOrDefault();
                if(string.IsNullOrWhiteSpace(discodatapath))
                    Assert.Fail("Wrong Value");
                var discologpath = res.filepaths.Where(n => n.EndsWith("ldf")).FirstOrDefault();
                if(string.IsNullOrWhiteSpace(discologpath))
                    Assert.Fail("Wrong Value");

                if(discodatapath != expecteddatapath)
                    Assert.Fail("Wrong Value");
                if(discologpath != expectedlogpath)
                    Assert.Fail("Wrong Value");


                // Delete the test database...
                {
                    // Delete the database...
                    var res4 = pt.Drop_Database(dbname);
                    if(res4 != 1)
                        Assert.Fail("Wrong Value");

                    // Check that the database is no longer present...
                    var res5 = pt.Does_Database_Exist(dbname);
                    if(res5 != 0)
                        Assert.Fail("Wrong Value");
                }
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_10_1  Verify that we can get a list of tables for a given database.
        [TestMethod]
        public async Task Test_1_10_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                // Get the tool instance...
                pt = Get_ToolInstance_forMaster();


                // Get the data folder path...
                var res = pt.Get_TableList_forDatabase("dbProjectControls", out var tablelist);
                if(res != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_11_1  Verify that we can get a list of databases on the PostgreSQL host.
        [TestMethod]
        public async Task Test_1_11_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Get a list of databases on the host...
                var res = pt.Get_DatabaseList(out var dblist);
                if (res != 1 || dblist == null)
                    Assert.Fail("Wrong Value");

                // Verify the list contains master...
                if (!dblist.Contains("master"))
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }



        /*

                //  Test_1_3_1  Verify we can get the primary key column data for a table.
                [TestMethod]
                public async Task Test_1_3_1()
                {
                    MSSQL_Tools pt = null;

                    try
                    {
                        pt = Get_ToolInstance_forDatabase(dbcreds.Database);

                        string dbname = this.GenerateDatabaseName();

                        // Check that the database doesn't exist...
                        var res1 = pt.Is_Database_Present(dbname);
                        if(res1 != 0)
                            Assert.Fail("Wrong Value");

                        // Create the test database...
                        var res2 = pt.Create_Database(dbname);
                        if(res2 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database now exists...
                        var res3 = pt.Is_Database_Present(dbname);
                        if(res3 != 1)
                            Assert.Fail("Wrong Value");


                        // Create a test table in our test database that has a primary key...
                        string tblname = this.GenerateTableName();
                        {
                            // Swap our connection to the created database...
                            pt.Dispose();
                            await Task.Delay(500);
                            pt = Get_ToolInstance_forDatabase(dbname);

                            // Verify we can access the new database...
                            var res5 = pt.TestConnection();
                            if(res5 != 1)
                                Assert.Fail("Wrong Value");

                            // Create the table definition...
                            var tch = new TableDefinition(tblname, pt.Username);
                            tch.Add_Pk_Column("Id", Postgres.DAL.Model.ePkColTypes.integer);
                            tch.Add_String_Column("IconName", 50, false);

                            // Make the call to create the table...
                            var res6 = pt.Create_Table(tch);
                            if(res6 != 1)
                                Assert.Fail("Wrong Value");

                            // Confirm the table was created...
                            var res7 = pt.DoesTableExist(tblname);
                            if(res7 != 1)
                                Assert.Fail("Wrong Value");
                        }

                        // Query for the primary keys of the table...
                        var respk = pt.Get_PrimaryKeyConstraints_forTable(tblname, out var pklist);
                        if(respk != 1 || pklist == null)
                            Assert.Fail("Wrong Value");

                        // Verify we found the primary key we created...
                        if(pklist.Count != 1)
                            Assert.Fail("Wrong Value");
                        var pkc = pklist.FirstOrDefault(n => n.key_column == "Id");
                        if(pkc == null)
                            Assert.Fail("Wrong Value");
                        if(pkc.table_name != tblname)
                            Assert.Fail("Wrong Value");


                        // To drop the database, we must switch back to the postgres database...
                        {
                            // Swap our connection back to the catalog...
                            pt.Dispose();
                            await Task.Delay(500);
                            pt = new MSSQL_Tools();
                            pt.Hostname = dbcreds.Host;
                            pt.Database = dbcreds.Database;
                            pt.Username = dbcreds.User;
                            pt.Password = dbcreds.Password;

                            // Verify we can access the postgres database...
                            var res6a = pt.TestConnection();
                            if(res6a != 1)
                                Assert.Fail("Wrong Value");
                        }

                        // Delete the database...
                        var res8 = pt.Drop_Database(dbname, true);
                        if(res8 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database is no longer present...
                        var res9 = pt.Is_Database_Present(dbname);
                        if(res9 != 0)
                            Assert.Fail("Wrong Value");
                    }
                    finally
                    {
                        pt?.Dispose();
                    }
                }


                //  Test_1_8_1  Verify that we can create a database whose name doesn't already exist.
                [TestMethod]
                public async Task Test_1_8_1()
                {
                    MSSQL_Tools pt = null;

                    try
                    {
                        pt = Get_ToolInstance_forDatabase(dbcreds.Database);

                        string dbname = this.GenerateDatabaseName();

                        // Check that the database doesn't exist...
                        var res1 = pt.Is_Database_Present(dbname);
                        if(res1 != 0)
                            Assert.Fail("Wrong Value");

                        // Create the test database...
                        var res2 = pt.Create_Database(dbname);
                        if(res2 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database now exists...
                        var res3 = pt.Is_Database_Present(dbname);
                        if(res3 != 1)
                            Assert.Fail("Wrong Value");

                        // Delete the database...
                        var res4 = pt.Drop_Database(dbname);
                        if(res4 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database is no longer present...
                        var res5 = pt.Is_Database_Present(dbname);
                        if(res5 != 0)
                            Assert.Fail("Wrong Value");
                    }
                    finally
                    {
                        pt?.Dispose();
                    }
                }

                //  Test_1_8_2  Verify that we cannot create a database whose name already exists.
                [TestMethod]
                public async Task Test_1_8_2()
                {
                    MSSQL_Tools pt = null;

                    try
                    {
                        pt = Get_ToolInstance_forDatabase(dbcreds.Database);

                        string dbname = this.GenerateDatabaseName();

                        // Check that the database doesn't exist...
                        var res1 = pt.Is_Database_Present(dbname);
                        if(res1 != 0)
                            Assert.Fail("Wrong Value");

                        // Create the test database...
                        var res2 = pt.Create_Database(dbname);
                        if(res2 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database now exists...
                        var res3 = pt.Is_Database_Present(dbname);
                        if(res3 != 1)
                            Assert.Fail("Wrong Value");


                        // Attempt to create the database again...
                        var res2a = pt.Create_Database(dbname);
                        if(res2a != -2)
                            Assert.Fail("Wrong Value");


                        // Delete the database...
                        var res4 = pt.Drop_Database(dbname);
                        if(res4 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database is no longer present...
                        var res5 = pt.Is_Database_Present(dbname);
                        if(res5 != 0)
                            Assert.Fail("Wrong Value");
                    }
                    finally
                    {
                        pt?.Dispose();
                    }
                }

                //  Test_1_8_3  Verify that we can verify if a database exists.
                [TestMethod]
                public async Task Test_1_8_3()
                {
                    MSSQL_Tools pt = null;

                    try
                    {
                        pt = Get_ToolInstance_forDatabase(dbcreds.Database);

                        string dbname = this.GenerateDatabaseName();

                        // Check that the database doesn't exist...
                        var res1 = pt.Is_Database_Present(dbname);
                        if(res1 != 0)
                            Assert.Fail("Wrong Value");

                        // Create the test database...
                        var res2 = pt.Create_Database(dbname);
                        if(res2 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database now exists...
                        var res3 = pt.Is_Database_Present(dbname);
                        if(res3 != 1)
                            Assert.Fail("Wrong Value");

                        // Delete the database...
                        var res4 = pt.Drop_Database(dbname);
                        if(res4 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database is no longer present...
                        var res5 = pt.Is_Database_Present(dbname);
                        if(res5 != 0)
                            Assert.Fail("Wrong Value");
                    }
                    finally
                    {
                        pt?.Dispose();
                    }
                }

                //  Test_1_8_4  Verify that we can delete a database that exists.
                [TestMethod]
                public async Task Test_1_8_4()
                {
                    MSSQL_Tools pt = null;

                    try
                    {
                        pt = Get_ToolInstance_forDatabase(dbcreds.Database);

                        string dbname = this.GenerateDatabaseName();

                        // Check that the database doesn't exist...
                        var res1 = pt.Is_Database_Present(dbname);
                        if(res1 != 0)
                            Assert.Fail("Wrong Value");

                        // Create the test database...
                        var res2 = pt.Create_Database(dbname);
                        if(res2 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database now exists...
                        var res3 = pt.Is_Database_Present(dbname);
                        if(res3 != 1)
                            Assert.Fail("Wrong Value");

                        // Delete the database...
                        var res4 = pt.Drop_Database(dbname);
                        if(res4 != 1)
                            Assert.Fail("Wrong Value");

                        // Check that the database is no longer present...
                        var res5 = pt.Is_Database_Present(dbname);
                        if(res5 != 0)
                            Assert.Fail("Wrong Value");
                    }
                    finally
                    {
                        pt?.Dispose();
                    }
                }

                //  Test_1_8_5  Verify that we cannot delete a database with an unknown name.
                [TestMethod]
                public async Task Test_1_8_5()
                {
                    MSSQL_Tools pt = null;

                    try
                    {
                        pt = Get_ToolInstance_forDatabase(dbcreds.Database);

                        string dbname = this.GenerateDatabaseName();

                        // Check that the database doesn't exist...
                        var res1 = pt.Is_Database_Present(dbname);
                        if(res1 != 0)
                            Assert.Fail("Wrong Value");

                        // Delete the database...
                        var res4 = pt.Drop_Database(dbname);
                        if(res4 != 0)
                            Assert.Fail("Wrong Value");
                    }
                    finally
                    {
                        pt?.Dispose();
                    }
                }

                //  Test_1_8_6  Verify that a user without CreateDB is not allowed to create a database.
                [TestMethod]
                public async Task Test_1_8_6()
                {
                    MSSQL_Tools pt = null;

                    try
                    {
                        pt = Get_ToolInstance_forDatabase(dbcreds.Database);


                        // Create a test user...
                        string mortaluser1 = this.GenerateTestUser();
                        string mortaluser1_password = this.GenerateUserPassword();
                        var resa = pt.CreateUser(mortaluser1, mortaluser1_password);
                        if(resa != 1)
                            Assert.Fail("Wrong Value");


                        // Create the test database name...
                        string dbname = this.GenerateDatabaseName();


                        // Have test user 1 attempt to create the test database...
                        {
                            // Open a connection as test user 1...
                            var pt1 = new Postgres_Tools();
                            pt1.Hostname = dbcreds.Host;
                            pt1.Database = dbcreds.Database;
                            pt1.Username = mortaluser1;
                            pt1.Password = mortaluser1_password;

                            // Attempt to create the test database...
                            var res1a = pt1.Create_Database(dbname);
                            if(res1a != -4)
                                Assert.Fail("Wrong Value");

                            pt1.Dispose();
                        }

                        // Delete the database...
                        var res4 = pt.Drop_Database(dbname);
                        if(res4 != 0)
                            Assert.Fail("Wrong Value");

                        // Check that the database is no longer present...
                        var res5 = pt.Is_Database_Present(dbname);
                        if(res5 != 0)
                            Assert.Fail("Wrong Value");
                    }
                    finally
                    {
                        pt?.Dispose();
                    }
                }
        */

        #region Protected Methods

        #endregion
    }
}
