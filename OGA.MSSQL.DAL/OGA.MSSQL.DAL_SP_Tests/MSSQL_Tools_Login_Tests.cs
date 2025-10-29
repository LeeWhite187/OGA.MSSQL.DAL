using Microsoft.VisualStudio.TestTools.UnitTesting;
using OGA.MSSQL.DAL_SP_Tests.Helpers;
using OGA.Testing.Lib;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;

namespace OGA.MSSQL.DAL_SP_Tests
{
    /*  Unit Tests for SQL Server MSSQL_Tools class.
        This set of tests exercises the management of server-level logins.

        //  Test_1_1_1  Verify that we can add a SQL host local login.
        //  Test_1_1_2  Verify that we can check if a SQL host login exists.
        //  Test_1_1_3  Verify that we can check if a SQL host login no longer exists.
        //  Test_1_1_4  Verify that we can get a list of SQL host logins.
        //  Test_1_1_5  Verify that we can delete a SQL host login.

        //  Test_1_2_1  Verify that we can query for a list of logins of a SQL Host.

        //  Test_1_3_1  Verify username validator limits to letters, numbers, and underscores.dww

        //  Test_1_4_1  Verify that we can add and delete a SQL Host login.
        //  Test_1_4_2  Verify that no error results from deleting a nonexistant SQL Host login.
        //  Test_1_4_3  Verify that we get a specific error when adding a local login with a password that doesn't meet complexity requirements.
     
        //  Test_1_5_1  Verify that we can change the password of a local login account.
        //  Test_1_5_2  Verify that we get a specific error when changing the password of a local login to a password that doesn't meet complexity requirements.
        //  Test_1_5_3  Verify that a non superuser can change their own password.
        //              NOTE: This test is not possible, since SQL Server doesn't allow logins to change their own password, unless granted ALTER LOGIN or having the systemadmin role.
     
        //  Test_1_6_1  Verify that we can fully manage a login's membership in each server role.
        //              This includes adding them to the role, verify role-member presence, removing the membership, and verifying it is gone.
        //              This test verifies management of the off-the-shelf server roles, termed fixed-roles.

        //  Test_1_7_1  Verify that we can assign a set of role memberships for a SQL login, via Set_Login_Roles().
        //              This test also verifies functionality of Get_SQlHostRoles_for_Login().

     */


    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class MSSQL_Tools_Login_Tests : ProjectTest_Base
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

        //  Test_1_1_1  Verify that we can add a SQL host local login.
        [TestMethod]
        public async Task Test_1_1_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a SQL host login...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Verify the login is present...
                var reschk1 = pt.Does_Login_Exist(mortaluser1);
                if(reschk1 != 1)
                    Assert.Fail("Wrong Value");


                // Remove the SQL host login...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");


                // Verify the login is no longer present...
                var reschk2 = pt.Does_Login_Exist(mortaluser1);
                if(reschk2 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_2  Verify that we can check if a SQL host login exists.
        [TestMethod]
        public async Task Test_1_1_2()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a SQL host login...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Verify the login is present...
                var reschk1 = pt.Does_Login_Exist(mortaluser1);
                if(reschk1 != 1)
                    Assert.Fail("Wrong Value");


                // Remove the SQL host login...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");


                // Verify the login is no longer present...
                var reschk2 = pt.Does_Login_Exist(mortaluser1);
                if(reschk2 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_3  Verify that we can check if a SQL host login no longer exists.
        [TestMethod]
        public async Task Test_1_1_3()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();

                // Verify the login doesn't exist...
                var reschk0 = pt.Does_Login_Exist(mortaluser1);
                if(reschk0 != 0)
                    Assert.Fail("Wrong Value");


                // Create the SQL host login...
                var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");


                // Verify the login is present...
                var reschk1 = pt.Does_Login_Exist(mortaluser1);
                if(reschk1 != 1)
                    Assert.Fail("Wrong Value");


                // Remove the SQL host login...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");


                // Verify the login is no longer present...
                var reschk2 = pt.Does_Login_Exist(mortaluser1);
                if(reschk2 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_4  Verify that we can get a list of SQL host logins.
        [TestMethod]
        public async Task Test_1_1_4()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                // Create a SQL host login...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Create a second SQL host login...
                string mortaluser2 = this.GenerateTestUser();
                string mortaluser2_password = this.GenerateUserPassword();
                {
                    var resa2 = pt.Add_LocalLogin(mortaluser2, mortaluser2_password);
                    if(resa2 != 1)
                        Assert.Fail("Wrong Value");
                }


                // Get a list of logins...
                var resll = pt.GetLoginList(out var loginlist);
                if(resll != 1 || loginlist == null)
                    Assert.Fail("Wrong Value");


                // Check that each login is in the retrieved list...
                if(!loginlist.Contains(mortaluser1))
                    Assert.Fail("Wrong Value");
                if(!loginlist.Contains(mortaluser2))
                    Assert.Fail("Wrong Value");


                // Remove the SQL host logins...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
                var resdeluser2 = pt.DeleteLogin(mortaluser2);
                if(resdeluser2 != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_5  Verify that we can delete a SQL host login.
        [TestMethod]
        public async Task Test_1_1_5()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = Get_ToolInstance_forMaster();

                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();

                // Verify the login doesn't exist...
                var reschk0 = pt.Does_Login_Exist(mortaluser1);
                if(reschk0 != 0)
                    Assert.Fail("Wrong Value");


                // Create the SQL host login...
                var resa = pt.Add_LocalLogin(mortaluser1, mortaluser1_password);
                if(resa != 1)
                    Assert.Fail("Wrong Value");


                // Verify the login is present...
                var reschk1 = pt.Does_Login_Exist(mortaluser1);
                if(reschk1 != 1)
                    Assert.Fail("Wrong Value");


                // Remove the SQL host login...
                var resdeluser = pt.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");


                // Verify the login is no longer present...
                var reschk2 = pt.Does_Login_Exist(mortaluser1);
                if(reschk2 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        //  Test_1_2_1  Verify that we can query for a list of logins of a SQL Host.
        [TestMethod]
        public async Task Test_1_2_1()
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


        //  Test_1_3_1  Verify username validator limits to letters, numbers, and underscores.
        [TestMethod]
        public async Task Test_1_3_1()
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


        //  Test_1_4_1  Verify that we can add and delete a SQL Host login.
        [TestMethod]
        public async Task Test_1_4_1()
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

        //  Test_1_4_2  Verify that no error results from deleting a nonexistant SQL Host login.
        [TestMethod]
        public async Task Test_1_4_2()
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

        //  Test_1_4_3  Verify that we get a specific error when adding a local login with a password that doesn't meet complexity requirements.
        [TestMethod]
        public async Task Test_1_4_3()
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


        //  Test_1_5_1  Verify that we can change the password of a local login account.
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

        //  Test_1_5_2  Verify that we get a specific error when changing the password of a local login to a password that doesn't meet complexity requirements.
        [TestMethod]
        public async Task Test_1_5_2()
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


        //  Test_1_6_1  Verify that we can fully manage a login's membership in each server role.
        //              This includes adding them to the role, verify role-member presence, removing the membership, and verifying it is gone.
        //              This test verifies management of the off-the-shelf server roles, termed fixed-roles.
        [TestMethod]
        public async Task Test_1_6_1()
        {
            MSSQL_Tools ptadmin = null;

            try
            {
                // Create the tools instance...
                ptadmin = Get_ToolInstance_forMaster();
                //ptadmin.Cfg_ClearConnectionPoolOnClose = true;


                // Create a test login...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = ptadmin.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Create a list of each permission to test...
                var pl = new List<eSQLRoles>();
                pl.Add(eSQLRoles.sysadmin);
                pl.Add(eSQLRoles.diskadmin);
                pl.Add(eSQLRoles.bulkadmin);
                pl.Add(eSQLRoles.setupadmin);
                pl.Add(eSQLRoles.processadmin);
                pl.Add(eSQLRoles.serveradmin);
                pl.Add(eSQLRoles.dbcreator);


                // Iterate the list of permissions...
                foreach(var p in pl)
                {
                    // Verify the login doesn't have the current role...
                    var resp1 = ptadmin.Does_Login_HaveRole(mortaluser1, p);
                    if(resp1 != 0)
                        Assert.Fail("Wrong Value");


                    // Add the login to the role...
                    var resar = ptadmin.Add_Login_Role(mortaluser1, p);
                    if(resar != 1)
                        Assert.Fail("Wrong Value");


                    // Verify the login has the current role...
                    var resp2 = ptadmin.Does_Login_HaveRole(mortaluser1, p);
                    if(resp2 != 1)
                        Assert.Fail("Wrong Value");


                    // Remove the login from the user...
                    var resrr = ptadmin.Drop_Login_Role(mortaluser1, p);
                    if(resrr != 1)
                        Assert.Fail("Wrong Value");


                    // Verify the login doesn't have the current role...
                    var resp3 = ptadmin.Does_Login_HaveRole(mortaluser1, p);
                    if(resp3 != 0)
                        Assert.Fail("Wrong Value");
                }

                // Remove the login...
                var resdeluser = ptadmin.DeleteLogin(mortaluser1);
                if(resdeluser != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                ptadmin?.Dispose();
            }
        }


        //  Test_1_7_1  Verify that we can assign a set of role memberships for a SQL login, via Set_Login_Roles().
        //              This test also verifies functionality of Get_SQlHostRoles_for_Login().
        [TestMethod]
        public async Task Test_1_7_1()
        {
            MSSQL_Tools ptadmin = null;

            try
            {
                // Create the tools instance...
                ptadmin = Get_ToolInstance_forMaster();
                //ptadmin.Cfg_ClearConnectionPoolOnClose = true;


                // Create a test login...
                string mortaluser1 = this.GenerateTestUser();
                string mortaluser1_password = this.GenerateUserPassword();
                {
                    var resa = ptadmin.Add_LocalLogin(mortaluser1, mortaluser1_password);
                    if(resa != 1)
                        Assert.Fail("Wrong Value");
                }


                // Verify the login has no privileges, via Get_SQlHostRoles_for_Login()...


                // Create a list of server level permissions...
                var pl = new List<eSQLRoles>();
                pl.Add(eSQLRoles.sysadmin);
                pl.Add(eSQLRoles.diskadmin);
                pl.Add(eSQLRoles.bulkadmin);
                pl.Add(eSQLRoles.setupadmin);
                pl.Add(eSQLRoles.processadmin);
                pl.Add(eSQLRoles.serveradmin);
                pl.Add(eSQLRoles.dbcreator);


                // Perform the following sequence for a growing list of roles...
                for(int x= 1; x < pl.Count; x++)
                {
                    // Add the number of permission, via Set_Login_Roles(), and verify it, via Get_SQlHostRoles_for_Login().

                    // Create a temporary list of permissions to add...
                    var plc = pl.Select(n=> n).Take(x).ToList();

                    // Add the permissions via Set_Login_Roles()...
                    var respa = ptadmin.Set_Login_Roles(mortaluser1, plc);
                    if(respa != 1)
                        Assert.Fail("Wrong Value");

                    // Get the list of server roles, via Get_SQlHostRoles_for_Login()...
                    var respb = ptadmin.Get_SQlHostRoles_for_Login(mortaluser1, out var arl);
                    if(respb != 1 || arl == null)
                        Assert.Fail("Wrong Value");

                    // Verify the added roles are present...
                    foreach (var r in plc)
                    {
                        if(!arl.Contains(r))
                            Assert.Fail("Wrong Value");
                    }

                    // Remove all roles from the login...
                    var respc = ptadmin.Set_Login_Roles(mortaluser1, new List<eSQLRoles>());
                    if(respc != 1)
                        Assert.Fail("Wrong Value");

                    // Get the list of server roles, via Get_SQlHostRoles_for_Login()...
                    var respd = ptadmin.Get_SQlHostRoles_for_Login(mortaluser1, out var arl2);
                    if(respd != 1 || arl2 == null)
                        Assert.Fail("Wrong Value");

                    // Verify the all roles are gone...
                    if(arl2.Count != 0)
                        Assert.Fail("Wrong Value");
                }


                // Remove the login...
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
