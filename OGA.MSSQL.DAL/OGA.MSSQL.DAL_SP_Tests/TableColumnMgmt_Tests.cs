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
using System.Linq;
using OGA.MSSQL.DAL.Model;
using OGA.MSSQL.DAL_SP.Model;
using OGA.MSSQL.DAL_SP_Tests.Helpers;

namespace OGA.MSSQL_Tests
{
    /*  Unit Tests for MSSQL Tools class.
        This set of tests exercise the method, Get_ColumnList_forTable().

        //  Test_1_1_1  Verify that we can query column names for a table.
        //  Test_1_1_2  Verify that we can query column info for a table.

     
     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class TableColumnMgmt_Tests : ProjectTest_Base
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

        
        [TestMethod]
        public async Task Test_1_1_0()
        {
            string tablename = "tbl_Icons";
            var tch = new TableDefinition(tablename);

            var res1 = tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
            if (res1 != 1)
                Assert.Fail("Wrong Value");

            var res2 = tch.Add_String_Column("IconName", 50, true);
            if (res2 != 1)
                Assert.Fail("Wrong Value");

            var res3 = tch.Add_Numeric_Column("Height", MSSQL.DAL.Model.eNumericColTypes.integer, true);
            if (res3 != 1)
                Assert.Fail("Wrong Value");

            var res4 = tch.Add_Numeric_Column("Width", MSSQL.DAL.Model.eNumericColTypes.integer, true);
            if (res4 != 1)
                Assert.Fail("Wrong Value");

            var res5 = tch.Add_String_Column("Path", 255, true);
            if (res5 != 1)
                Assert.Fail("Wrong Value");


            var sql = tch.CreateSQLCmd();

            int x = 0;
        }


        //  Test_1_1_1  Verify that we can query column names for a table.
        [TestMethod]
        public async Task Test_1_1_1()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = new MSSQL_Tools();
                pt.HostName = dbcreds.Host;
                pt.Service = dbcreds.Service;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we can test with...
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col1 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col2 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col3 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col4 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Create a test database...
                    var res1 = pt.Create_Database(dbname);
                    if(res1 != 1)
                        Assert.Fail("Wrong Value");

                    //// Swap our connection to the created database...
                    //pt.Dispose();
                    //await Task.Delay(500);
                    //pt = new MSSQL_Tools();
                    //pt.HostName = dbcreds.Host;
                    //pt.Service = dbcreds.Service;
                    //pt.Username = dbcreds.User;
                    //pt.Password = dbcreds.Password;

                    // Verify we can access the target database...
                    var res2 = pt.TestConnection_toDatabase(dbname);
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Create the table definition...
                    var tch = new TableDefinition(tblname);
                    tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                    tch.Add_Guid_Column(col1, false);
                    tch.Add_UTCDateTime_Column(col2, false);
                    tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                    tch.Add_String_Column(col4, 50, false);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(dbname, tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(dbname, tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }


                // Query for column names of the table...
                var res4 = pt.Get_Columns_for_Table(dbname, tblname, out var collist);
                if(res4 != 1 || collist == null || collist.Count == 0)
                    Assert.Fail("Wrong Value");


                // Verify the list has all of our column names...
                if(!collist.Contains("Id"))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col1))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col2))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col3))
                    Assert.Fail("Wrong Value");
                if(!collist.Contains(col4))
                    Assert.Fail("Wrong Value");


                // To drop the database, we must dispose out tool instance, to close the target database connection...
                {
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = Get_ToolInstance_forMaster();

                    // Verify we can access the databaes...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Drop connections to the database...
                var res10 = pt.Set_Database_toSingleUser(dbname);
                if(res10 != 1)
                    Assert.Fail("Wrong Value");

                // Delete the database...
                var res7 = pt.Drop_Database(dbname);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res8 = pt.Does_Database_Exist(dbname);
                if(res8 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        //  Test_1_1_2  Verify that we can query column info for a table.
        [TestMethod]
        public async Task Test_1_1_2()
        {
            MSSQL_Tools pt = null;

            try
            {
                pt = new MSSQL_Tools();
                pt.HostName = dbcreds.Host;
                pt.Service = dbcreds.Service;
                pt.Username = dbcreds.User;
                pt.Password = dbcreds.Password;

                // Create a test database with a table we can test with...
                string dbname = "testdb" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string tblname = "testtbl" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col1 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col2 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col3 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                string col4 = "testcol" + Nanoid.Nanoid.Generate(size: 10, alphabet:"abcdefghijklmnopqrstuvwxyz01234567890");
                {
                    // Create a test database...
                    var res1 = pt.Create_Database(dbname);
                    if(res1 != 1)
                        Assert.Fail("Wrong Value");

                    //// Swap our connection to the created database...
                    //pt.Dispose();
                    //await Task.Delay(500);
                    //pt = new MSSQL_Tools();
                    //pt.HostName = dbcreds.Host;
                    //pt.Service = dbcreds.Service;
                    //pt.Database = dbname;
                    //pt.Username = dbcreds.User;
                    //pt.Password = dbcreds.Password;

                    // Verify we can access the new database...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");

                    // Create the table definition...
                    var tch = new TableDefinition(tblname);
                    tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                    tch.Add_Guid_Column(col1, false);
                    tch.Add_UTCDateTime_Column(col2, true);
                    tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, true);
                    tch.Add_String_Column(col4, 50, true);

                    // Make the call to create the table...
                    var res3 = pt.Create_Table(dbname, tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = pt.DoesTableExist(dbname, tblname);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");
                }


                // Query for column info of the table...
                var res4 = pt.Get_ColumnInfo_forTable(dbname, tblname, out var coldata);
                if(res4 != 1 || coldata == null || coldata.Count == 0)
                    Assert.Fail("Wrong Value");


                // Verify the list has the correct column data...
                var c1 = coldata.FirstOrDefault(m => m.name == "Id");
                if(c1 == null)
                    Assert.Fail("Wrong Value");
                if(c1.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c1.dataType != SQL_Datatype_Names.CONST_SQL_int)
                    Assert.Fail("Wrong Value");
                if(c1.isNullable != false)
                    Assert.Fail("Wrong Value");
                if(c1.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c1.ordinal != 1)
                    Assert.Fail("Wrong Value");

                var c2 = coldata.FirstOrDefault(m => m.name == col1);
                if(c2 == null)
                    Assert.Fail("Wrong Value");
                if(c2.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c2.dataType != SQL_Datatype_Names.CONST_SQL_uniqueidentifier)
                    Assert.Fail("Wrong Value");
                if(c2.isNullable != false)
                    Assert.Fail("Wrong Value");
                if(c2.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c2.ordinal != 2)
                    Assert.Fail("Wrong Value");

                var c3 = coldata.FirstOrDefault(m => m.name == col2);
                if(c3 == null)
                    Assert.Fail("Wrong Value");
                if(c3.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c3.dataType != SQL_Datatype_Names.CONST_SQL_datetimeoffset)
                    Assert.Fail("Wrong Value");
                if(c3.isNullable != true)
                    Assert.Fail("Wrong Value");
                if(c3.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c3.ordinal != 3)
                    Assert.Fail("Wrong Value");

                var c4 = coldata.FirstOrDefault(m => m.name == col3);
                if(c4 == null)
                    Assert.Fail("Wrong Value");
                if(c4.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c4.dataType != SQL_Datatype_Names.CONST_SQL_bigint)
                    Assert.Fail("Wrong Value");
                if(c4.isNullable != true)
                    Assert.Fail("Wrong Value");
                if(c4.maxlength != null)
                    Assert.Fail("Wrong Value");
                if(c4.ordinal != 4)
                    Assert.Fail("Wrong Value");

                var c5 = coldata.FirstOrDefault(m => m.name == col4);
                if(c5 == null)
                    Assert.Fail("Wrong Value");
                if(c5.isIdentity != false)
                    Assert.Fail("Wrong Value");
                if(c5.dataType != SQL_Datatype_Names.CONST_SQL_nvarchar)
                    Assert.Fail("Wrong Value");
                if(c5.isNullable != true)
                    Assert.Fail("Wrong Value");
                if(c5.maxlength != 50)
                    Assert.Fail("Wrong Value");
                if(c5.ordinal != 5)
                    Assert.Fail("Wrong Value");


                // To drop the database, we must dispose out tool instance, to close the target database connection...
                {
                    pt.Dispose();
                    await Task.Delay(500);
                    pt = Get_ToolInstance_forMaster();

                    // Verify we can access the databaes...
                    var res2 = pt.TestConnection();
                    if(res2 != 1)
                        Assert.Fail("Wrong Value");
                }

                // Drop connections to the database...
                var res10 = pt.Set_Database_toSingleUser(dbname);
                if(res10 != 1)
                    Assert.Fail("Wrong Value");

                // Delete the database...
                var res7 = pt.Drop_Database(dbname);
                if(res7 != 1)
                    Assert.Fail("Wrong Value");

                // Check that the database is no longer present...
                var res8 = pt.Does_Database_Exist(dbname);
                if(res8 != 0)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }


        #region Private Methods

        #endregion
    }
}
