using Microsoft.VisualStudio.TestTools.UnitTesting;
using OGA.MSSQL;
using OGA.SharedKernel.Process;
using OGA.SharedKernel;
using OGA.Testing.Lib;
using System;
using System.Collections.Generic;
using System.Web;
using OGA.Common.Config.structs;
using OGA.MSSQL.DAL;
using System.Threading.Tasks;
using OGA.MSSQL.DAL_SP_Tests.Helpers;
using System.Linq;
using System.Data;

namespace OGA.MSSQL_Tests
{
    /*  Unit Tests for SQL Server MSSQL_Tools class.
        This set of tests exercises the management of server-level logins.

        //  Test_1_1_1  Verify that we can add a database user.
        //  Test_1_1_2  Verify that we can check if a database user exists.
        //  Test_1_1_3  Verify that we can check if a database user no longer exists.
        //  Test_1_1_4  Verify that we can get a list of database users.
        //  Test_1_1_5  Verify that we can delete a database user.

        //  Test_1_2_1  Verify that we can add and delete a database user.
        //  Test_1_2_2  Verify that no error results from deleting a nonexistant database user.

        //  Test_1_3_1  Verify that we can fully manage a user's membership in each database role.
        //              This includes adding them to the role, verify role-member presence, removing the membership, and verifying it is gone.
        //              This test verifies management of the off-the-shelf database roles, termed fixed-roles.

        //  Test_1_4_1  Verify that we can assign a set of database role memberships for a database user, via Set_User_DatabaseRoles().
        //              This test also verifies functionality of Get_DatabaseRoles_for_User().


FINISH TESTS FROM HERE DOWN...

    Add test to verify we can query for explicit user privileges to a database.
    Add test to verify we can set a particular explicit user privilege for a user of a database.
    Add test to verify we can remove a particular explicit user privilege for a user of a database.

     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class MSSQL_Tools_UserMgmt_Tests : ProjectTest_Base
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

       
        //  Test_1_1_1  Verify that we can add a database user.
        [TestMethod]
        public async Task Test_1_1_1()
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
                var resadd = pt.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                var resd = pt.DeleteUserfromDatabase(dbname, mortaluser1);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is no longer in the database...
                var resexist = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname, true);
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

        //  Test_1_1_2  Verify that we can check if a database user exists.
        [TestMethod]
        public async Task Test_1_1_2()
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
                var resadd = pt.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is in the database...
                var resexist1 = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist1 != 1)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                var resd = pt.DeleteUserfromDatabase(dbname, mortaluser1);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is no longer in the database...
                var resexist = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq2 = pt.GetDatabaseUsers(dbname, out var userlist2);
                if(resq2 != 1 || userlist2 == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is no longer in the list...
                if(userlist2.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname, true);
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

        //  Test_1_1_3  Verify that we can check if a database user no longer exists.
        [TestMethod]
        public async Task Test_1_1_3()
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
                var resadd = pt.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is in the database...
                var resexist1 = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist1 != 1)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                var resd = pt.DeleteUserfromDatabase(dbname, mortaluser1);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is no longer in the database...
                var resexist = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq2 = pt.GetDatabaseUsers(dbname, out var userlist2);
                if(resq2 != 1 || userlist2 == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is no longer in the list...
                if(userlist2.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname, true);
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

        //  Test_1_1_4  Verify that we can get a list of database users.
        [TestMethod]
        public async Task Test_1_1_4()
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
                var resadd = pt.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is in the database...
                var resexist1 = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist1 != 1)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                var resd = pt.DeleteUserfromDatabase(dbname, mortaluser1);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is no longer in the database...
                var resexist = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq2 = pt.GetDatabaseUsers(dbname, out var userlist2);
                if(resq2 != 1 || userlist2 == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is no longer in the list...
                if(userlist2.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname, true);
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

        //  Test_1_1_5  Verify that we can delete a database user.
        [TestMethod]
        public async Task Test_1_1_5()
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
                    {
                        Console.WriteLine($"Username was: \"{(mortaluser1)}\". Password was: \"{(mortaluser1_password)}\".");
                        Assert.Fail("Wrong Value");
                    }
                }


                // Add the user to the test database...
                // NOTE: We don't specify any db roles for the user, here.
                // So, we are simply adding the user to the database.
                var resadd = pt.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is in the database...
                var resexist1 = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist1 != 1)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                var resd = pt.DeleteUserfromDatabase(dbname, mortaluser1);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is no longer in the database...
                var resexist = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq2 = pt.GetDatabaseUsers(dbname, out var userlist2);
                if(resq2 != 1 || userlist2 == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is no longer in the list...
                if(userlist2.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname, true);
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


        //  Test_1_2_1  Verify that we can add and delete a database user.
        [TestMethod]
        public async Task Test_1_2_1()
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
                var resadd = pt.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is in the database...
                var resexist1 = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist1 != 1)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                var resd = pt.DeleteUserfromDatabase(dbname, mortaluser1);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is no longer in the database...
                var resexist = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Query for the list of users in the database...
                var resq2 = pt.GetDatabaseUsers(dbname, out var userlist2);
                if(resq2 != 1 || userlist2 == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is no longer in the list...
                if(userlist2.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname, true);
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

        //  Test_1_2_2  Verify that no error results from deleting a nonexistant database user.
        [TestMethod]
        public async Task Test_1_2_2()
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


                // Verify the user is not in the database...
                var resexist = pt.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                // Verify we get no error...
                var resd = pt.DeleteUserfromDatabase(dbname, mortaluser1);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Delete the database...
                var res4 = pt.Drop_Database(dbname, true);
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


        //  Test_1_3_1  Verify that we can fully manage a user's membership in each database role.
        //              This includes adding them to the role, verify role-member presence, removing the membership, and verifying it is gone.
        //              This test verifies management of the off-the-shelf database roles, termed fixed-roles.
        [TestMethod]
        public async Task Test_2_1_1()
        {
            MSSQL_Tools ptadmin = null;

            try
            {
                // Create the tools instance...
                ptadmin = Get_ToolInstance_forMaster();
                //ptadmin.Cfg_ClearConnectionPoolOnClose = true;


                // Create a test database...
                string dbname = this.CreateTestDatabase(ptadmin);


                // Create a test user...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = ptadmin.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Add the user to the test database...
                var resadd = ptadmin.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Create a list of each permission to test...
                var pl = new List<eSQLRoles>();
                pl.Add(eSQLRoles.db_accessadmin);
                pl.Add(eSQLRoles.db_backupoperator);
                pl.Add(eSQLRoles.db_datareader);
                pl.Add(eSQLRoles.db_datawriter);
                pl.Add(eSQLRoles.db_ddladmin);
                pl.Add(eSQLRoles.db_denydatareader);
                pl.Add(eSQLRoles.db_denydatawriter);
                pl.Add(eSQLRoles.db_owner);
                pl.Add(eSQLRoles.db_securityadmin);


                // Iterate the list of permissions...
                foreach(var p in pl)
                {
                    // Verify the user doesn't have the current role...
                    var resp1 = ptadmin.Does_User_Have_DatabaseRole(dbname, mortaluser1, p);
                    if(resp1 != 0)
                        Assert.Fail("Wrong Value");


                    // Add the user to the role...
                    var resar = ptadmin.Add_User_to_DatabaseRole(dbname, mortaluser1, p);
                    if(resar != 1)
                        Assert.Fail("Wrong Value");


                    // Verify the user has the current role...
                    var resp2 = ptadmin.Does_User_Have_DatabaseRole(dbname, mortaluser1, p);
                    if(resp2 != 1)
                        Assert.Fail("Wrong Value");


                    // Remove the role from the user...
                    var resrr = ptadmin.Drop_User_from_DatabaseRole(dbname, mortaluser1, p);
                    if(resrr != 1)
                        Assert.Fail("Wrong Value");


                    // Verify the user doesn't have the current role...
                    var resp3 = ptadmin.Does_User_Have_DatabaseRole(dbname, mortaluser1, p);
                    if(resp3 != 0)
                        Assert.Fail("Wrong Value");
                }

                // Delete the database...
                var resdel = ptadmin.Drop_Database(dbname, true);
                if(resdel != 1)
                    Assert.Fail("Wrong Value");

                // Remove the user...
                var resdeluser = ptadmin.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                ptadmin?.Dispose();
            }
        }


        //  Test_1_4_1  Verify that we can assign a set of database role memberships for a database user, via Set_User_DatabaseRoles().
        //              This test also verifies functionality of Get_DatabaseRoles_for_User().
        [TestMethod]
        public async Task Test_1_4_1()
        {
            MSSQL_Tools ptadmin = null;

            try
            {
                ptadmin = Get_ToolInstance_forMaster();
                ptadmin.Cfg_ClearConnectionPoolOnClose = true;

                // Create a test database...
                var dbname = this.CreateTestDatabase(ptadmin);

                // Create a test user...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = ptadmin.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Add the user to the test database...
                // NOTE: We don't specify any db roles for the user, here.
                // So, we are simply adding the user to the database.
                var resadd = ptadmin.Add_User_to_Database(dbname, mortaluser1);
                if(resadd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is in the database...
                var resexist1 = ptadmin.Does_User_Exist_forDatabase(dbname, mortaluser1);
                if(resexist1 != 1)
                    Assert.Fail("Wrong Value");


                // Create a list of database level permissions...
                var pl = new List<eSQLRoles>();
                pl.Add(eSQLRoles.db_accessadmin);
                pl.Add(eSQLRoles.db_backupoperator);
                pl.Add(eSQLRoles.db_datareader);
                pl.Add(eSQLRoles.db_datawriter);
                pl.Add(eSQLRoles.db_ddladmin);
                pl.Add(eSQLRoles.db_denydatareader);
                pl.Add(eSQLRoles.db_denydatawriter);
                pl.Add(eSQLRoles.db_owner);
                pl.Add(eSQLRoles.db_securityadmin);


                // Perform the following sequence for a growing list of roles...
                for(int x= 1; x < pl.Count; x++)
                {
                    // Add the number of permission, via Set_User_DatabaseRoles(), and verify it, via Get_DatabaseRoles_for_User().

                    // Create a temporary list of permissions to add...
                    var plc = pl.Select(n=> n).Take(x).ToList();

                    // Add the permissions via Set_User_DatabaseRoles()...
                    var respa = ptadmin.Set_User_DatabaseRoles(dbname, mortaluser1, plc);
                    if(respa != 1)
                        Assert.Fail("Wrong Value");

                    // Get the list of database roles, via Get_DatabaseRoles_for_User()...
                    var respb = ptadmin.Get_DatabaseRoles_for_User(dbname, mortaluser1, out var arl);
                    if(respb != 1 || arl == null)
                        Assert.Fail("Wrong Value");

                    // Verify the added roles are present...
                    foreach (var r in plc)
                    {
                        if(!arl.Contains(r))
                            Assert.Fail("Wrong Value");
                    }

                    // Remove all roles from the database user...
                    var respc = ptadmin.Set_User_DatabaseRoles(dbname, mortaluser1, new List<eSQLRoles>());
                    if(respc != 1)
                        Assert.Fail("Wrong Value");

                    // Get the list of database roles, via Get_DatabaseRoles_for_User()...
                    var respd = ptadmin.Get_DatabaseRoles_for_User(dbname,mortaluser1, out var arl2);
                    if(respd != 1 || arl2 == null)
                        Assert.Fail("Wrong Value");

                    // Verify the all roles are gone...
                    if(arl2.Count != 0)
                        Assert.Fail("Wrong Value");
                }


                // Delete the database...
                var res4 = ptadmin.Drop_Database(dbname, true);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res5 = ptadmin.Does_Database_Exist(dbname);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Remove the user...
                var resdeluser = ptadmin.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                ptadmin?.Dispose();
            }
        }
    }
}
