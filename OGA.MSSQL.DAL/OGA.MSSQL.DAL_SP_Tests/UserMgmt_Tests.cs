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

namespace OGA.MSSQL_Tests
{
    /*  Unit Tests for SQL Server Tools class.
        This set of tests exercise the user management methods.

        //  Test_1_1_1  Verify that we can query for a list of logins of a SQL Host.
        //  Test_1_1_2  Verify that we can query for a list of users of a database.

        //  Test_1_2_1  Verify username validator limits to letters, numbers, and underscores.

        //  Test_1_3_1  Verify that we can add and delete a SQL Host login.
        //  Test_1_3_2  Verify that no error results from deleting a nonexistant SQL Host login.
        //  Test_1_3_3  Verify that we can add and delete a database user.
        //  Test_1_3_4  Verify that no error results from deleting a nonexistant database user.
        //  Test_1_3_5  Verify that we get a specific error when adding a local login with a password that doesn't meet complexity requirements.

        //  Test_1_4_1  Verify that we can change the password of a local login account.
        //  Test_1_4_2  Verify that we get a specific error when changing the password of a local login to a password that doesn't meet complexity requirements.
        //  Test_1_4_3  Verify that a non superuser can change their own password.
        //              NOTE: This test is not possible, since SQL Server doesn't allow logins to change their own password, unless granted ALTER LOGIN or having the systemadmin role.

        //  Test_2_1_1  Verify that we can check a sysadmin user is actually a sysadmin.
        //  Test_2_1_2  Verify that we can check a non sysadmin is not a sysadmin.
        //  Test_2_1_3  Verify that we can promote a user to sysadmin.
        //  Test_2_1_4  Verify that we can demote a user from sysadmin to regular user.

        //  Test_2_2_1  Verify that we can check a diskadmin user has diskadmin.
        //  Test_2_2_2  Verify that we can check a non-admin user does not have diskadmin.
        //  Test_2_2_3  Verify that we can grant diskadmin to a user.
        //  Test_2_2_4  Verify that we can deny diskadmin to a user.
    
        //  Test_2_3_1  Verify that we can check a bulkadmin user has bulkadmin.
        //  Test_2_3_2  Verify that we can check a non-admin user does not have bulkadmin.
        //  Test_2_3_3  Verify that we can grant bulkadmin to a user.
        //  Test_2_3_4  Verify that we can deny bulkadmin to a user.

        //  Test_2_4_1  Verify that we can check a setupadmin user has setupadmin.
        //  Test_2_4_2  Verify that we can check a non-admin user does not have setupadmin.
        //  Test_2_4_3  Verify that we can grant setupadmin to a user.
        //  Test_2_4_4  Verify that we can deny setupadmin to a user.

        //  Test_2_5_1  Verify that we can check a processadmin user has processadmin.
        //  Test_2_5_2  Verify that we can check a non-admin user does not have processadmin.
        //  Test_2_5_3  Verify that we can grant processadmin to a user.
        //  Test_2_5_4  Verify that we can deny processadmin to a user.

        //  Test_2_6_1  Verify that we can check a serveradmin user has serveradmin.
        //  Test_2_6_2  Verify that we can check a non-admin user does not have serveradmin.
        //  Test_2_6_3  Verify that we can grant serveradmin to a user.
        //  Test_2_6_4  Verify that we can deny serveradmin to a user.

        //  Test_2_7_1  Verify that we can check a dbcreator user has dbcreator.
        //  Test_2_7_2  Verify that we can check a non-admin user does not have dbcreator.
        //  Test_2_7_3  Verify that we can grant dbcreator to a user.
        //  Test_2_7_4  Verify that we can deny dbcreator to a user.


FINISH TESTS FROM HERE DOWN...

    Add tests to confirm that we can add all server roles with Set_Login_Roles().
    Add tests to confirm that we can remove all server roles with Set_Login_Roles().


        Add tests to exercise managing users in a specific database, since SQL Server has users at the engine level and database level.


    Move all the user management tests to the UserMgmt_Tests class:

    Add test to verify we can get the configured db roles for a database.
    Add test to verify we can query for user privileges to a database.
    Add test to verify we can set a particular privilege for a user of a database.
    Add tests to verify the addition and presence, before and after, of each db role for a user in a database.

    Add test to verify that we can add a user to a database, with a specific set of roles.
    Add test to verify that we can add a user to a database, with no specified roles, and see if this should return an error as result.

    Add test to verify the Does_User_Exist_forDatabase() call works, in both directions Found and not found.
    And, verify how Does_User_Exist_forDatabase() behaves when the database doesn't exist.


     
     
     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class UserMgmt_Tests : ProjectTest_Base
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

        
        //  Test_1_1_1  Verify that we can query for a list of logins of a SQL Host.
        [TestMethod]
        public async Task Test_1_1_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a test login...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Get a list of system login...
                var res4 = pt.GetLoginList(out var userlist);
                if(res4 != 1 || userlist == null || userlist.Count == 0)
                    Assert.Fail("Wrong Value");

                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");

                // Delete the test login...
                var res7 = pt.DeleteLogin(mortaluser1);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the login is no longer present...
                var res8 = pt.Does_Login_Exist(mortaluser1);
                if(res8 != 0)
                    Assert.Fail("Wrong Value");

                // Get a list of system login...
                var res4a = pt.GetLoginList(out var userlist2);
                if(res4a != 1 || userlist2 == null || userlist2.Count == 0)
                    Assert.Fail("Wrong Value");

                if(userlist2.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_1_2  Verify that we can query for a list of users of a database.
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


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
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


        //  Test_1_2_1  Verify username validator limits to letters, numbers, and underscores.
        [TestMethod]
        public void Test_1_2_1()
        {
            var res1 = MSSQL_Tools.UserNameIsValid("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_");
            if (!res1)
                Assert.Fail("Wrong Value");

            var res2 = MSSQL_Tools.UserNameIsValid("");
            if (res2)
                Assert.Fail("Wrong Value");

            var res3 = MSSQL_Tools.UserNameIsValid(" ");
            if (res3)
                Assert.Fail("Wrong Value");

            var res4 = MSSQL_Tools.UserNameIsValid("a ");
            if (res4)
                Assert.Fail("Wrong Value");

            var res5 = MSSQL_Tools.UserNameIsValid(" a");
            if (res5)
                Assert.Fail("Wrong Value");

            var res6 = MSSQL_Tools.UserNameIsValid("sadfsdf.assdds");
            if (res6)
                Assert.Fail("Wrong Value");

            var res7 = MSSQL_Tools.UserNameIsValid("sadfsdf+assdds");
            if (res7)
                Assert.Fail("Wrong Value");
        }


        //  Test_1_3_1  Verify that we can add and delete a SQL Host login.
        [TestMethod]
        public async Task Test_1_3_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a test login...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");

                // Check that the login is present...
                var res2 = pt.Does_Login_Exist(mortaluser1);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Delete the test login...
                var res7 = pt.DeleteLogin(mortaluser1);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the login is no longer present...
                var res8 = pt.Does_Login_Exist(mortaluser1);
                if(res8 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_3_2  Verify that no error results from deleting a nonexistant SQL Host login.
        [TestMethod]
        public async Task Test_1_3_2()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a test login name that we will NOT add as a login...
                string mortaluser1 = this.GenerateTestUser();

                // Delete the test login...
                var res7 = pt.DeleteLogin(mortaluser1);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_3_3  Verify that we can add and delete a database user.
        [TestMethod]
        public async Task Test_1_3_3()
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


                // Query for the list of users in the database...
                var resq = pt.GetDatabaseUsers(dbname, out var userlist);
                if(resq != 1 || userlist == null)
                    Assert.Fail("Wrong Value");


                // Check that our test user is in the list...
                if(!userlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                var resd = pt.DeleteUserfromDatabase(mortaluser1, dbname);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user is no longer in the database...
                var resexist = pt.Does_User_Exist_forDatabase(mortaluser1, dbname);
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

        //  Test_1_3_4  Verify that no error results from deleting a nonexistant database user.
        [TestMethod]
        public async Task Test_1_3_4()
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
                var resexist = pt.Does_User_Exist_forDatabase(mortaluser1, dbname);
                if(resexist != 0)
                    Assert.Fail("Wrong Value");


                // Delete the user from the database...
                // Verify we get no error...
                var resd = pt.DeleteUserfromDatabase(mortaluser1, dbname);
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

        //  Test_1_3_5  Verify that we get a specific error when adding a local login with a password that doesn't meet complexity requirements.
        [TestMethod]
        public async Task Test_1_3_5()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a test login with a bad password...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = "12345678";
                var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                if(resa != -3)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_4_1  Verify that we can change the password of a local login account.
        [TestMethod]
        public async Task Test_1_4_1()
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

        //  Test_1_4_2  Verify that we get a specific error when changing the password of a local login to a password that doesn't meet complexity requirements.
        [TestMethod]
        public async Task Test_1_4_2()
        {
            MSSQL_Tools ptadmin = null;

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


                // Change the user's password to a bogus one...
                string mortaluser1_passwordnew = "123456";
                var reschg = ptadmin.ChangeLoginPassword(mortaluser1, mortaluser1_passwordnew);
                if(reschg != -3)
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


        //  Test_2_1_1  Verify that we can check a sysadmin user is actually a sysadmin.
        [TestMethod]
        public async Task Test_2_1_1()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.sysadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_1_2  Verify that we can check a non sysadmin is not a sysadmin.
        [TestMethod]
        public async Task Test_2_1_2()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.sysadmin;

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


                // Check if the login does NOT have the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 0)
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

        //  Test_2_1_3  Verify that we can promote a user to sysadmin.
        [TestMethod]
        public async Task Test_2_1_3()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.sysadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_1_4  Verify that we can demote a user from sysadmin to regular user.
        [TestMethod]
        public async Task Test_2_1_4()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.sysadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
                    Assert.Fail("Wrong Value");


                // Remove the role...
                var resd = ptadmin.Drop_Login_Role(mortaluser1, role);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user no longer has the role...
                var reshas2 = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas2 != 0)
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


        //  Test_2_2_1  Verify that we can check a diskadmin user has diskadmin.
        [TestMethod]
        public async Task Test_2_2_1()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.diskadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_2_2  Verify that we can check a non-admin user does not have diskadmin.
        [TestMethod]
        public async Task Test_2_2_2()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.diskadmin;

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


                // Check if the login does NOT have the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 0)
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

        //  Test_2_2_3  Verify that we can grant diskadmin to a user.
        [TestMethod]
        public async Task Test_2_2_3()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.diskadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_2_4  Verify that we can deny diskadmin to a user.
        [TestMethod]
        public async Task Test_2_2_4()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.diskadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
                    Assert.Fail("Wrong Value");


                // Remove the role...
                var resd = ptadmin.Drop_Login_Role(mortaluser1, role);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user no longer has the role...
                var reshas2 = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas2 != 0)
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
    
        //  Test_2_3_1  Verify that we can check a bulkadmin user has bulkadmin.
        [TestMethod]
        public async Task Test_2_3_1()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.bulkadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_3_2  Verify that we can check a non-admin user does not have bulkadmin.
        [TestMethod]
        public async Task Test_2_3_2()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.bulkadmin;

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


                // Check if the login does NOT have the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 0)
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

        //  Test_2_3_3  Verify that we can grant bulkadmin to a user.
        [TestMethod]
        public async Task Test_2_3_3()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.bulkadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_3_4  Verify that we can deny bulkadmin to a user.
        [TestMethod]
        public async Task Test_2_3_4()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.bulkadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
                    Assert.Fail("Wrong Value");


                // Remove the role...
                var resd = ptadmin.Drop_Login_Role(mortaluser1, role);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user no longer has the role...
                var reshas2 = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas2 != 0)
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

        //  Test_2_4_1  Verify that we can check a setupadmin user has setupadmin.
        [TestMethod]
        public async Task Test_2_4_1()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.setupadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_4_2  Verify that we can check a non-admin user does not have setupadmin.
        [TestMethod]
        public async Task Test_2_4_2()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.setupadmin;

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


                // Check if the login does NOT have the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 0)
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

        //  Test_2_4_3  Verify that we can grant setupadmin to a user.
        [TestMethod]
        public async Task Test_2_4_3()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.setupadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_4_4  Verify that we can deny setupadmin to a user.
        [TestMethod]
        public async Task Test_2_4_4()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.setupadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
                    Assert.Fail("Wrong Value");


                // Remove the role...
                var resd = ptadmin.Drop_Login_Role(mortaluser1, role);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user no longer has the role...
                var reshas2 = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas2 != 0)
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

        //  Test_2_5_1  Verify that we can check a processadmin user has processadmin.
        [TestMethod]
        public async Task Test_2_5_1()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.processadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_5_2  Verify that we can check a non-admin user does not have processadmin.
        [TestMethod]
        public async Task Test_2_5_2()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.processadmin;

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


                // Check if the login does NOT have the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 0)
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

        //  Test_2_5_3  Verify that we can grant processadmin to a user.
        [TestMethod]
        public async Task Test_2_5_3()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.processadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_5_4  Verify that we can deny processadmin to a user.
        [TestMethod]
        public async Task Test_2_5_4()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.processadmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
                    Assert.Fail("Wrong Value");


                // Remove the role...
                var resd = ptadmin.Drop_Login_Role(mortaluser1, role);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user no longer has the role...
                var reshas2 = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas2 != 0)
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

        //  Test_2_6_1  Verify that we can check a serveradmin user has serveradmin.
        [TestMethod]
        public async Task Test_2_6_1()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.serveradmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_6_2  Verify that we can check a non-admin user does not have serveradmin.
        [TestMethod]
        public async Task Test_2_6_2()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.serveradmin;

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


                // Check if the login does NOT have the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 0)
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

        //  Test_2_6_3  Verify that we can grant serveradmin to a user.
        [TestMethod]
        public async Task Test_2_6_3()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.serveradmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_6_4  Verify that we can deny serveradmin to a user.
        [TestMethod]
        public async Task Test_2_6_4()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.serveradmin;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
                    Assert.Fail("Wrong Value");


                // Remove the role...
                var resd = ptadmin.Drop_Login_Role(mortaluser1, role);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user no longer has the role...
                var reshas2 = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas2 != 0)
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

        //  Test_2_7_1  Verify that we can check a dbcreator user has dbcreator.
        [TestMethod]
        public async Task Test_2_7_1()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.dbcreator;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_7_2  Verify that we can check a non-admin user does not have dbcreator.
        [TestMethod]
        public async Task Test_2_7_2()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.dbcreator;

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


                // Check if the login does NOT have the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 0)
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

        //  Test_2_7_3  Verify that we can grant dbcreator to a user.
        [TestMethod]
        public async Task Test_2_7_3()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.dbcreator;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check if the login has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
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

        //  Test_2_7_4  Verify that we can deny dbcreator to a user.
        [TestMethod]
        public async Task Test_2_7_4()
        {
            MSSQL_Tools ptadmin = null;
            eSQLRoles role = eSQLRoles.dbcreator;

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


                // Give the user the role...
                var reschg = ptadmin.Add_Login_Role(mortaluser1, role);
                if(reschg != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user has the role...
                var reshas = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas != 1)
                    Assert.Fail("Wrong Value");


                // Remove the role...
                var resd = ptadmin.Drop_Login_Role(mortaluser1, role);
                if(resd != 1)
                    Assert.Fail("Wrong Value");


                // Check that the user no longer has the role...
                var reshas2 = ptadmin.Does_Login_HaveRole(mortaluser1, role);
                if(reshas2 != 0)
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

/*




        //  Test_1_6_2  Verify that we can check a user does not have CreateDB.
        [TestMethod]
        public async Task Test_1_6_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user user does not have CreateDB...
                var res1 = pt.HasDBCreate(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res2 = pt.DeleteUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_6_3  Verify that we can grant CreateDB to a user.
        [TestMethod]
        public async Task Test_1_6_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user does not have DBCreate role...
                var res1 = pt.HasDBCreate(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Add DBCreate to the test user...
                var res2 = pt.GrantDBCreate(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has DBCreate...
                var res3 = pt.HasDBCreate(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res4 = pt.DeleteUser(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_6_4  Verify that we can deny CreateDB to a user.
        [TestMethod]
        public async Task Test_1_6_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user doesn't have DBCreate...
                var res1 = pt.HasDBCreate(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Give the test user DBCreate...
                var res2 = pt.GrantDBCreate(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has DBCreate...
                var res3 = pt.HasDBCreate(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Remove DBCreate from the test user...
                var res4 = pt.DenyDBCreate(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it no longer has DBCreate...
                var res5 = pt.HasDBCreate(username);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res6 = pt.DeleteUser(username);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_7_1  Verify that we can check a user has CreateRole.
        [TestMethod]
        public async Task Test_1_7_1()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Check that the postgres user has CreateRole...
                var res1 = pt.HasCreateRole("postgres");
                if(res1 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_7_2  Verify that we can check a user does not have CreateRole.
        [TestMethod]
        public async Task Test_1_7_2()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user user does not have CreateRole...
                var res1 = pt.HasCreateRole(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res2 = pt.DeleteUser(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_7_3  Verify that we can grant CreateRole to a user.
        [TestMethod]
        public async Task Test_1_7_3()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user does not have CreateRole role...
                var res1 = pt.HasCreateRole(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Add CreateRole to the test user...
                var res2 = pt.GrantCreateRole(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has CreateRole...
                var res3 = pt.HasCreateRole(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res4 = pt.DeleteUser(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_7_4  Verify that we can deny CreateRole to a user.
        [TestMethod]
        public async Task Test_1_7_4()
        {
            Postgres_Tools pt = null;

            try
            {
                pt = new Postgres_Tools();
                pt.Hostname = dbcreds.Host;
                pt.Database = dbcreds.Database;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Attempt to add a test user...
                string username = "testuser" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                var res = pt.CreateUser(username);
                if(res != 1)
                    Assert.Fail("Wrong Value");

                // Check that the test user does not have CreateRole...
                var res1 = pt.HasCreateRole(username);
                if(res1 != 0)
                    Assert.Fail("Wrong Value");

                // Give the test user CreateRole...
                var res2 = pt.GrantCreateRole(username);
                if(res2 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it now has CreateRole...
                var res3 = pt.HasCreateRole(username);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Remove CreateRole from the test user...
                var res4 = pt.DenyCreateRole(username);
                if(res4 != 1)
                    Assert.Fail("Wrong Value");

                // Verify it no longer has CreateRole...
                var res5 = pt.HasCreateRole(username);
                if(res5 != 0)
                    Assert.Fail("Wrong Value");

                // Attempt to delete user...
                var res6 = pt.DeleteUser(username);
                if(res6 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

*/

        #region Private Methods

        #endregion
    }
}
