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

        //  Test_1_2_1  Verify that Get_TableList_forDatabase() returns a -1 for a missing database.
        //  Test_1_2_2  Verify that Get_TableList_forDatabase() returns an empty table list for a fresh database.
        //  Test_1_2_3  Verify that Get_TableList_forDatabase() returns the list of table names for a database with two tables present.

        //  Test_1_3_1  Verify that Get_RowCount_for_Tables() returns a -1 for a missing database.
        //  Test_1_3_2  Verify that Get_RowCount_for_Tables() returns an empty list for a database without any tables.
        //  Test_1_3_3  Verify that Get_RowCount_for_Tables() returns a list of zeroes for a database with empty tables in it.
        //  Test_1_3_4  Verify that Get_RowCount_for_Tables() returns the list of rows added to a database with tables with test data rows.

CREATE TESTS FROM HERE DOWN...

        //  Test_1_4_1  Verify that Get_TableSize() returns a -1 for a missing database.
        //  Test_1_4_2  Verify that Get_TableSize() returns a -1 for a missing table.
        //  Test_1_4_3  Verify that Get_TableSize() returns a positive value for a database with a table containing sample data.
     
        //  Test_1_5_1  Verify that Is_Table_in_Database() returns a -1 for a missing database.
        //  Test_1_5_2  Verify that Is_Table_in_Database() returns a 0 for a missing table name.

        //  Test_1_6_1  Verify that DoesTableExist() returns a -1 for a missing database.
        //  Test_1_6_2  Verify that DoesTableExist() returns a 0 for a missing table name.

        //  Test_1_7_1  Verify that Create_Table() returns a -1 for a missing database.
        //  Test_1_7_2  Verify that Create_Table() returns a -2 for a bad table definition.
        //  Test_1_7_3  Verify that Create_Table() successfully created a database table for a given table definition.

        //  Test_1_8_1  Verify that Drop_Table() returns a -1 for a missing database.
        //  Test_1_8_2  Verify that Drop_Table() returns success if the table is not present.
        //  Test_1_8_3  Verify that Drop_Table() returns success for deleting a data table that was present.

        //  Test_2_1_1  Verify that Get_PrimaryKeyConstraints_forTable() returns a -1 for a missing database.
        //  Test_2_1_2  Verify that Get_PrimaryKeyConstraints_forTable() returns a 0 for a missing table.
        //  Test_2_1_3  Verify that Get_PrimaryKeyConstraints_forTable() returns an empty list of key constraints for a table with no columns.
        //  Test_2_1_4  Verify that Get_PrimaryKeyConstraints_forTable() returns an empty list of key constraints for a table with columns, but no defined primary key.
        //  Test_2_1_5  Verify that Get_PrimaryKeyConstraints_forTable() returns a list of key constraints for a table with columns, but one primary key.
     
        //  Test_2_2_1  Verify that Get_Columns_for_Table() returns a -1 for a missing database.
        //  Test_2_2_2  Verify that Get_Columns_for_Table() returns a 0 for a missing table.
        //  Test_2_2_3  Verify that Get_Columns_for_Table() returns an empty list of columns for a data table with zero columns.
        //  Test_2_2_4  Verify that Get_Columns_for_Table() returns a list of columns for a data table with 2 columns.

        //  Test_2_3_1  Verify that Get_ColumnInfo_forTable() returns a -1 for a missing database.
        //  Test_2_3_2  Verify that Get_ColumnInfo_forTable() returns a 0 for a missing table.
        //  Test_2_3_3  Verify that Get_ColumnInfo_forTable() returns an empty list of columns for a data table with zero columns.
        //  Test_2_3_4  Verify that Get_ColumnInfo_forTable() returns a list of columns for a data table with 2 columns.

        //  Test_2_4_1  Verify that Create_Table() can create datatable with no defined columns.
        //  Test_2_4_2  Verify that Create_Table() can create datatable with a boolean column that can be null.
        //  Test_2_4_3  Verify that Create_Table() can create datatable with a boolean column that can not be null.

        //  Test_2_5_1  Verify that Create_Table() can create datatable with a PK column of Int type and no identity behavior.
        //  Test_2_5_2  Verify that Create_Table() can create datatable with a PK column of Int type and generate identity behavior.
        //  Test_2_5_3  Verify that Create_Table() can create datatable with a PK column of UUID type and no identity behavior.
        //  Test_2_5_4  Verify that Create_Table() can create datatable with a PK column of UUID type and generate identity behavior.
        //  Test_2_5_5  Verify that Create_Table() can create datatable with a PK column of bigint type and no identity behavior.
        //  Test_2_5_6  Verify that Create_Table() can create datatable with a PK column of bigint type and generate identity behavior.
        //  Test_2_5_7  Verify that Create_Table() can create datatable with a PK column of varchar type and length of 10 and no identity behavior.

        //  Test_3_1_1  Verify that Create_Table() can create datatable with a non-UTC datetime column that can be null.
        //  Test_3_1_2  Verify that Create_Table() can create datatable with a non-UTC datetime column that can not be null.

        //  Test_3_2_1  Verify that Create_Table() can create datatable with a UTC DateTime column that can be null.
        //  Test_3_2_2  Verify that Create_Table() can create datatable with a UTC DateTime column that can not be null.

        //  Test_4_1_1  Verify that Create_Table() can create datatable with a GUID column that can be null.
        //  Test_4_1_2  Verify that Create_Table() can create datatable with a GUID column that can not be null.
     
        //  Test_5_1_1  Verify that Create_Table() can create datatable with a Int column that can be null.
        //  Test_5_1_2  Verify that Create_Table() can create datatable with a Int column that can not be null.

        //  Test_5_2_1  Verify that Create_Table() can create datatable with a bigint column that can be null.
        //  Test_5_2_2  Verify that Create_Table() can create datatable with a bigint column that can not be null.
     
        //  Test_5_3_1  Verify that Create_Table() can create datatable with a real column that can be null.
        //  Test_5_3_2  Verify that Create_Table() can create datatable with a real column that can not be null.
     
        //  Test_5_4_1  Verify that Create_Table() can create datatable with a doubleprecision column that can be null.
        //  Test_5_4_2  Verify that Create_Table() can create datatable with a doubleprecision column that can not be null.

        //  Test_5_5_1  Verify that Create_Table() can create datatable with a numeric column that can be null.
        //  Test_5_5_2  Verify that Create_Table() can create datatable with a numeric column that can not be null.
     
        //  Test_6_1_1  Verify that Create_Table() can create datatable with a 10-character string column that can be null.
        //  Test_6_1_2  Verify that Create_Table() can create datatable with a 10-character string column that can not be null.

        //  Test_6_2_1  Verify that Create_Table() can create datatable with a 100-character string column that can be null.
        //  Test_6_2_2  Verify that Create_Table() can create datatable with a 100-character string column that can not be null.

        //  Test_6_3_1  Verify that Create_Table() can create datatable with a max-length (length=0) string column that can be null.
        //  Test_6_3_2  Verify that Create_Table() can create datatable with a max-length (length=0) string column that can not be null.

     */

    [TestCategory(Test_Types.Unit_Tests)]
    [TestClass]
    public class TableColumnMgmt_Tests : ProjectTest_Base
    {
        private string _dbname;
        private MSSQL_Tools pt;

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

            // Create the test database...
            pt = Get_ToolInstance_forMaster();
            this._dbname = GenerateDatabaseName();
            var res = pt.Create_Database(this._dbname);
            if (res != 1)
                Assert.Fail("Wrong Value");
        }

        /// <summary>
        /// Called after each test runs.
        /// Be sure this method exists in your top-level test class, and that it calls the corresponding test cleanup method of the base.
        /// </summary>
        [TestCleanup]
        override public void TearDown()
        {
            // Runs after each test. (Optional)

            this.DeleteDatabase();

            this.pt?.Dispose();

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
            // Create a test table we can test with...
            string tblname = this.GenerateTableName();
            string col1 = this.GenerateColumnName();
            string col2 = this.GenerateColumnName();
            string col3 = this.GenerateColumnName();
            string col4 = this.GenerateColumnName();
            {
                // Create the table definition...
                var tch = new TableDefinition(tblname);
                tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                tch.Add_Guid_Column(col1, false);
                tch.Add_UTCDateTime_Column(col2, false);
                tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                tch.Add_String_Column(col4, 50, false);

                // Make the call to create the table...
                var res3 = this.pt.Create_Table(this._dbname, tch);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Confirm the table was created...
                var res3a = this.pt.DoesTableExist(this._dbname, tblname);
                if(res3a != 1)
                    Assert.Fail("Wrong Value");
            }


            // Query for column names of the table...
            var res4 = this.pt.Get_Columns_for_Table(this._dbname, tblname, out var collist);
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
        }

        //  Test_1_1_2  Verify that we can query column info for a table.
        [TestMethod]
        public async Task Test_1_1_2()
        {
            // Create a test table we can test with...
            string tblname = this.GenerateTableName();
            string col1 = this.GenerateColumnName();
            string col2 = this.GenerateColumnName();
            string col3 = this.GenerateColumnName();
            string col4 = this.GenerateColumnName();
            {
                // Create the table definition...
                var tch = new TableDefinition(tblname);
                tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                tch.Add_Guid_Column(col1, false);
                tch.Add_UTCDateTime_Column(col2, true);
                tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, true);
                tch.Add_String_Column(col4, 50, true);

                // Make the call to create the table...
                var res3 = pt.Create_Table(this._dbname, tch);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Confirm the table was created...
                var res3a = pt.DoesTableExist(this._dbname, tblname);
                if(res3a != 1)
                    Assert.Fail("Wrong Value");
            }


            // Query for column info of the table...
            var res4 = pt.Get_ColumnInfo_forTable(this._dbname, tblname, out var coldata);
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
        }


        //  Test_1_2_1  Verify that Get_TableList_forDatabase() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_1_2_1()
        {
            // Create a bogus database name...
            string bogusdbname = this.GenerateDatabaseName();

            // Attempt to get a table list for a missing database...
            var res = pt.Get_TableList_forDatabase(bogusdbname, out var tl);
            if(res != -1 || tl == null || tl.Count != 0)
                Assert.Fail("Wrong Value");
        }

        //  Test_1_2_2  Verify that Get_TableList_forDatabase() returns an empty table list for a database without any tables.
        [TestMethod]
        public async Task Test_1_2_2()
        {
            // Attempt to get a table list for the empty test database...
            var res = pt.Get_TableList_forDatabase(this._dbname, out var tl);
            if(res != 1 || tl == null)
                Assert.Fail("Wrong Value");

            // Verify the table list is empty...
            if(tl.Count != 0)
                Assert.Fail("Wrong Value");
        }

        //  Test_1_2_3  Verify that Get_TableList_forDatabase() returns the list of table names for a database with two tables present.
        [TestMethod]
        public async Task Test_1_2_3()
        {
            // Create a table...
            string tblname1 = this.GenerateTableName();
            {
                string col1 = this.GenerateColumnName();
                string col2 = this.GenerateColumnName();
                string col3 = this.GenerateColumnName();
                string col4 = this.GenerateColumnName();

                // Create the table definition...
                var tch = new TableDefinition(tblname1);
                tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                tch.Add_Guid_Column(col1, false);
                tch.Add_UTCDateTime_Column(col2, false);
                tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                tch.Add_String_Column(col4, 50, false);

                // Make the call to create the table...
                var res3 = this.pt.Create_Table(this._dbname, tch);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Confirm the table was created...
                var res3a = this.pt.DoesTableExist(this._dbname, tblname1);
                if(res3a != 1)
                    Assert.Fail("Wrong Value");
            }

            // Create another table...
            string tblname2 = this.GenerateTableName();
            {
                string col1 = this.GenerateColumnName();
                string col2 = this.GenerateColumnName();
                string col3 = this.GenerateColumnName();
                string col4 = this.GenerateColumnName();

                // Create the table definition...
                var tch = new TableDefinition(tblname2);
                tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                tch.Add_Guid_Column(col1, false);
                tch.Add_UTCDateTime_Column(col2, false);
                tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                tch.Add_String_Column(col4, 50, false);

                // Make the call to create the table...
                var res3 = this.pt.Create_Table(this._dbname, tch);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Confirm the table was created...
                var res3a = this.pt.DoesTableExist(this._dbname, tblname2);
                if(res3a != 1)
                    Assert.Fail("Wrong Value");
            }


            // Attempt to get a table list for the empty test database...
            var res = pt.Get_TableList_forDatabase(this._dbname, out var tl);
            if(res != 1 || tl == null)
                Assert.Fail("Wrong Value");

            // Verify the table list has both tables in it...
            if(tl.Count != 2)
                Assert.Fail("Wrong Value");
            if(!tl.Contains(tblname1))
                Assert.Fail("Wrong Value");
            if(!tl.Contains(tblname2))
                Assert.Fail("Wrong Value");
        }


        //  Test_1_3_1  Verify that Get_RowCount_for_Tables() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_1_3_1()
        {
            // Create a bogus database name...
            string bogusdbname = this.GenerateDatabaseName();

            // Attempt to get a list of row counts for tables...
            var res = pt.Get_RowCount_for_Tables(bogusdbname, out var rcl);
            if(res != -1 || rcl != null)
                Assert.Fail("Wrong Value");
        }
            
        //  Test_1_3_2  Verify that Get_RowCount_for_Tables() returns an empty list for a database without any tables.
        [TestMethod]
        public async Task Test_1_3_2()
        {
            // Attempt to get a list of row counts for tables...
            var res = pt.Get_RowCount_for_Tables(this._dbname, out var rcl);
            if(res != 1 || rcl == null)
                Assert.Fail("Wrong Value");

            // Verify the row count list is empty...
            if(rcl.Count != 0)
                Assert.Fail("Wrong Value");
        }

        //  Test_1_3_3  Verify that Get_RowCount_for_Tables() returns a list of zeroes for a database with empty tables in it.
        [TestMethod]
        public async Task Test_1_3_3()
        {
            // Create a table...
            string tblname1 = this.GenerateTableName();
            {
                string col1 = this.GenerateColumnName();
                string col2 = this.GenerateColumnName();
                string col3 = this.GenerateColumnName();
                string col4 = this.GenerateColumnName();

                // Create the table definition...
                var tch = new TableDefinition(tblname1);
                tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                tch.Add_Guid_Column(col1, false);
                tch.Add_UTCDateTime_Column(col2, false);
                tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                tch.Add_String_Column(col4, 50, false);

                // Make the call to create the table...
                var res3 = this.pt.Create_Table(this._dbname, tch);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Confirm the table was created...
                var res3a = this.pt.DoesTableExist(this._dbname, tblname1);
                if(res3a != 1)
                    Assert.Fail("Wrong Value");
            }

            // Create another table...
            string tblname2 = this.GenerateTableName();
            {
                string col1 = this.GenerateColumnName();
                string col2 = this.GenerateColumnName();
                string col3 = this.GenerateColumnName();
                string col4 = this.GenerateColumnName();

                // Create the table definition...
                var tch = new TableDefinition(tblname2);
                tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                tch.Add_Guid_Column(col1, false);
                tch.Add_UTCDateTime_Column(col2, false);
                tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                tch.Add_String_Column(col4, 50, false);

                // Make the call to create the table...
                var res3 = this.pt.Create_Table(this._dbname, tch);
                if(res3 != 1)
                    Assert.Fail("Wrong Value");

                // Confirm the table was created...
                var res3a = this.pt.DoesTableExist(this._dbname, tblname2);
                if(res3a != 1)
                    Assert.Fail("Wrong Value");
            }

            // Attempt to get a list of row counts for tables...
            var res = pt.Get_RowCount_for_Tables(this._dbname, out var rcl);
            if(res != 1 || rcl == null)
                Assert.Fail("Wrong Value");

            // Verify the row count list has entries for the two tables...
            if(rcl.Count != 2)
                Assert.Fail("Wrong Value");

            var e1 = rcl.Where(m => m.Key == tblname1).FirstOrDefault();
            if(e1.Value != 0)
                Assert.Fail("Wrong Value");
            var e2 = rcl.Where(m => m.Key == tblname2).FirstOrDefault();
            if(e2.Value != 0)
                Assert.Fail("Wrong Value");
        }

        //  Test_1_3_4  Verify that Get_RowCount_for_Tables() returns the list of rows added to a database with tables with test data rows.
        [TestMethod]
        public async Task Test_1_3_4()
        {
            // We need a DAL to push data...
            var dal = new MSSQL_DAL();
            dal.host = this.pt.HostName;
            dal.service = this.pt.Service;
            dal.database = this._dbname;
            dal.password = this.pt.Password;
            dal.username = this.pt.Username;

            try
            {
                // Create a table...
                string tblname1 = this.GenerateTableName();
                var tbl1_count = 200; // OGA.Testing.Helpers.RandomValueGenerators.CreateRandomInt();
                {
                    string col1 = this.GenerateColumnName();
                    string col2 = this.GenerateColumnName();
                    string col3 = this.GenerateColumnName();
                    string col4 = this.GenerateColumnName();

                    // Create the table definition...
                    var tch = new TableDefinition(tblname1);
                    tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                    tch.Add_Guid_Column(col1, false);
                    tch.Add_UTCDateTime_Column(col2, false);
                    tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                    tch.Add_String_Column(col4, 50, false);

                    // Make the call to create the table...
                    var res3 = this.pt.Create_Table(this._dbname, tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = this.pt.DoesTableExist(this._dbname, tblname1);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");

                    // Add data to table 1...
                    for(int x = 1; x <= tbl1_count; x++)
                    {
                        // Create an insert for the current record...
                        string sql = @$"INSERT INTO [dbo].[{tblname1}]([id], [{col1}], [{col2}], [{col3}], [{col4}])
                                       VALUES (
                                         {x.ToString()}
                                        ,'{Guid.NewGuid().ToString()}'
                                        ,'{OGA.Testing.Helpers.RandomValueGenerators.CreateRandomDateTime().ToString()}'
                                        ,'{OGA.Testing.Helpers.RandomValueGenerators.CreateRandomInt()}'
                                        ,'{this.GenerateDatabaseName()}')";

                        var resi = dal.Execute_NonQuery(sql);
                        if (resi.res != 1)
                            Assert.Fail("Wrong Value");
                    }
                }

                // Create another table...
                string tblname2 = this.GenerateTableName();
                var tbl2_count = 100; // OGA.Testing.Helpers.RandomValueGenerators.CreateRandomInt();
                {
                    string col1 = this.GenerateColumnName();
                    string col2 = this.GenerateColumnName();
                    string col3 = this.GenerateColumnName();
                    string col4 = this.GenerateColumnName();

                    // Create the table definition...
                    var tch = new TableDefinition(tblname2);
                    tch.Add_Pk_Column("Id", MSSQL.DAL.Model.ePkColTypes.integer);
                    tch.Add_Guid_Column(col1, false);
                    tch.Add_UTCDateTime_Column(col2, false);
                    tch.Add_Numeric_Column(col3, MSSQL.DAL.Model.eNumericColTypes.bigint, false);
                    tch.Add_String_Column(col4, 50, false);

                    // Make the call to create the table...
                    var res3 = this.pt.Create_Table(this._dbname, tch);
                    if(res3 != 1)
                        Assert.Fail("Wrong Value");

                    // Confirm the table was created...
                    var res3a = this.pt.DoesTableExist(this._dbname, tblname2);
                    if(res3a != 1)
                        Assert.Fail("Wrong Value");

                    // Add data to table 1...
                    for(int x = 1; x <= tbl2_count; x++)
                    {
                        // Create an insert for the current record...
                        string sql = @$"INSERT INTO [dbo].[{tblname2}]([id], [{col1}], [{col2}], [{col3}], [{col4}])
                                       VALUES (
                                         {x.ToString()}
                                        ,'{Guid.NewGuid().ToString()}'
                                        ,'{OGA.Testing.Helpers.RandomValueGenerators.CreateRandomDateTime().ToString()}'
                                        ,'{OGA.Testing.Helpers.RandomValueGenerators.CreateRandomInt()}'
                                        ,'{this.GenerateDatabaseName()}')";

                        var resi = dal.Execute_NonQuery(sql);
                        if (resi.res != 1)
                            Assert.Fail("Wrong Value");
                    }
                }


                // Attempt to get a list of row counts for tables...
                var res = pt.Get_RowCount_for_Tables(this._dbname, out var rcl);
                if(res != 1 || rcl == null)
                    Assert.Fail("Wrong Value");

                // Verify the row count list has entries for the two tables...
                if(rcl.Count != 2)
                    Assert.Fail("Wrong Value");

                var e1 = rcl.Where(m => m.Key == tblname1).FirstOrDefault();
                if(e1.Value != tbl1_count)
                    Assert.Fail("Wrong Value");
                var e2 = rcl.Where(m => m.Key == tblname2).FirstOrDefault();
                if(e2.Value != tbl2_count)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                dal.Dispose();
            }
        }


        //  Test_1_4_1  Verify that Get_TableSize() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_1_4_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_4_2  Verify that Get_TableSize() returns a -1 for a missing table.
        [TestMethod]
        public async Task Test_1_4_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_4_3  Verify that Get_TableSize() returns a positive value for a database with a table containing sample data.
        [TestMethod]
        public async Task Test_1_4_3()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_1_5_1  Verify that Is_Table_in_Database() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_1_5_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_5_2  Verify that Is_Table_in_Database() returns a 0 for a missing table name.
        [TestMethod]
        public async Task Test_1_5_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_1_6_1  Verify that DoesTableExist() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_1_6_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_6_2  Verify that DoesTableExist() returns a 0 for a missing table name.
        [TestMethod]
        public async Task Test_1_6_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_1_7_1  Verify that Create_Table() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_1_7_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_7_2  Verify that Create_Table() returns a -2 for a bad table definition.
        [TestMethod]
        public async Task Test_1_7_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_7_3  Verify that Create_Table() successfully created a database table for a given table definition.
        [TestMethod]
        public async Task Test_1_7_3()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_1_8_1  Verify that Drop_Table() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_1_8_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_8_2  Verify that Drop_Table() returns success if the table is not present.
        [TestMethod]
        public async Task Test_1_8_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_1_8_3  Verify that Drop_Table() returns success for deleting a data table that was present.
        [TestMethod]
        public async Task Test_1_8_3()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_2_1_1  Verify that Get_PrimaryKeyConstraints_forTable() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_2_1_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_1_2  Verify that Get_PrimaryKeyConstraints_forTable() returns a 0 for a missing table.
        [TestMethod]
        public async Task Test_2_1_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_1_3  Verify that Get_PrimaryKeyConstraints_forTable() returns an empty list of key constraints for a table with no columns.
        [TestMethod]
        public async Task Test_2_1_3()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_1_4  Verify that Get_PrimaryKeyConstraints_forTable() returns an empty list of key constraints for a table with columns, but no defined primary key.
        [TestMethod]
        public async Task Test_2_1_4()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_1_5  Verify that Get_PrimaryKeyConstraints_forTable() returns a list of key constraints for a table with columns, but one primary key.
        [TestMethod]
        public async Task Test_2_1_5()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_2_2_1  Verify that Get_Columns_for_Table() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_2_2_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_2_2  Verify that Get_Columns_for_Table() returns a 0 for a missing table.
        [TestMethod]
        public async Task Test_2_2_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_2_3  Verify that Get_Columns_for_Table() returns an empty list of columns for a data table with zero columns.
        [TestMethod]
        public async Task Test_2_2_3()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_2_4  Verify that Get_Columns_for_Table() returns a list of columns for a data table with 2 columns.
        [TestMethod]
        public async Task Test_2_2_4()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_2_3_1  Verify that Get_ColumnInfo_forTable() returns a -1 for a missing database.
        [TestMethod]
        public async Task Test_2_3_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_3_2  Verify that Get_ColumnInfo_forTable() returns a 0 for a missing table.
        [TestMethod]
        public async Task Test_2_3_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_3_3  Verify that Get_ColumnInfo_forTable() returns an empty list of columns for a data table with zero columns.
        [TestMethod]
        public async Task Test_2_3_3()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_3_4  Verify that Get_ColumnInfo_forTable() returns a list of columns for a data table with 2 columns.
        [TestMethod]
        public async Task Test_2_3_4()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_2_4_1  Verify that Create_Table() can create datatable with no defined columns.
        [TestMethod]
        public async Task Test_2_4_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_4_2  Verify that Create_Table() can create datatable with a boolean column that can be null.
        [TestMethod]
        public async Task Test_2_4_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_4_3  Verify that Create_Table() can create datatable with a boolean column that can not be null.
        [TestMethod]
        public async Task Test_2_4_3()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_2_5_1  Verify that Create_Table() can create datatable with a PK column of Int type and no identity behavior.
        [TestMethod]
        public async Task Test_2_5_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_5_2  Verify that Create_Table() can create datatable with a PK column of Int type and generate identity behavior.
        [TestMethod]
        public async Task Test_2_5_2()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_5_3  Verify that Create_Table() can create datatable with a PK column of UUID type and no identity behavior.
        [TestMethod]
        public async Task Test_2_5_3()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_5_4  Verify that Create_Table() can create datatable with a PK column of UUID type and generate identity behavior.
        [TestMethod]
        public async Task Test_2_5_4()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_5_5  Verify that Create_Table() can create datatable with a PK column of bigint type and no identity behavior.
        [TestMethod]
        public async Task Test_2_5_5()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_5_6  Verify that Create_Table() can create datatable with a PK column of bigint type and generate identity behavior.
        [TestMethod]
        public async Task Test_2_5_6()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_2_5_7  Verify that Create_Table() can create datatable with a PK column of varchar type and length of 10 and no identity behavior.
        [TestMethod]
        public async Task Test_2_5_7()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_3_1_1  Verify that Create_Table() can create datatable with a non-UTC datetime column that can be null.
        [TestMethod]
        public async Task Test_3_1_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_3_1_2  Verify that Create_Table() can create datatable with a non-UTC datetime column that can not be null.
        [TestMethod]
        public async Task Test_3_1_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_3_2_1  Verify that Create_Table() can create datatable with a UTC DateTime column that can be null.
        [TestMethod]
        public async Task Test_3_2_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_3_2_2  Verify that Create_Table() can create datatable with a UTC DateTime column that can not be null.
        [TestMethod]
        public async Task Test_3_2_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_4_1_1  Verify that Create_Table() can create datatable with a GUID column that can be null.
        [TestMethod]
        public async Task Test_4_1_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_4_1_2  Verify that Create_Table() can create datatable with a GUID column that can not be null.
        [TestMethod]
        public async Task Test_4_1_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_5_1_1  Verify that Create_Table() can create datatable with a Int column that can be null.
        [TestMethod]
        public async Task Test_5_1_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_5_1_2  Verify that Create_Table() can create datatable with a Int column that can not be null.
        [TestMethod]
        public async Task Test_5_1_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_5_2_1  Verify that Create_Table() can create datatable with a bigint column that can be null.
        [TestMethod]
        public async Task Test_5_2_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_5_2_2  Verify that Create_Table() can create datatable with a bigint column that can not be null.
        [TestMethod]
        public async Task Test_5_2_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_5_3_1  Verify that Create_Table() can create datatable with a real column that can be null.
        [TestMethod]
        public async Task Test_5_3_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_5_3_2  Verify that Create_Table() can create datatable with a real column that can not be null.
        [TestMethod]
        public async Task Test_5_3_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_5_4_1  Verify that Create_Table() can create datatable with a doubleprecision column that can be null.
        [TestMethod]
        public async Task Test_5_4_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_5_4_2  Verify that Create_Table() can create datatable with a doubleprecision column that can not be null.
        [TestMethod]
        public async Task Test_5_4_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_5_5_1  Verify that Create_Table() can create datatable with a numeric column that can be null.
        [TestMethod]
        public async Task Test_5_5_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_5_5_2  Verify that Create_Table() can create datatable with a numeric column that can not be null.
        [TestMethod]
        public async Task Test_5_5_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_6_1_1  Verify that Create_Table() can create datatable with a 10-character string column that can be null.
        [TestMethod]
        public async Task Test_6_1_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_6_1_2  Verify that Create_Table() can create datatable with a 10-character string column that can not be null.
        [TestMethod]
        public async Task Test_6_1_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_6_2_1  Verify that Create_Table() can create datatable with a 100-character string column that can be null.
        [TestMethod]
        public async Task Test_6_2_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_6_2_2  Verify that Create_Table() can create datatable with a 100-character string column that can not be null.
        [TestMethod]
        public async Task Test_6_2_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        //  Test_6_3_1  Verify that Create_Table() can create datatable with a max-length (length=0) string column that can be null.
        [TestMethod]
        public async Task Test_6_3_1()
        {
            Assert.Fail("NEEDS TEST");
        }

        //  Test_6_3_2  Verify that Create_Table() can create datatable with a max-length (length=0) string column that can not be null.
        [TestMethod]
        public async Task Test_6_3_2()
        {
            Assert.Fail("NEEDS TEST");
        }


        #region Private Methods

        private void DeleteDatabase()
        {
            if (string.IsNullOrWhiteSpace(_dbname)) return;

            var pt = Get_ToolInstance_forMaster();
            try
            {
                var res = pt.Drop_Database(_dbname, true);
                if (res != 1)
                    Assert.Fail("Wrong Value");
            }
            finally
            {
                pt?.Dispose();
            }
        }

        #endregion
    }
}
