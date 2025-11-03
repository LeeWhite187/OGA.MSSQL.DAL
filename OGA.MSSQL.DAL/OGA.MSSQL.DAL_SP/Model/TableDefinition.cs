using OGA.MSSQL.DAL.Model;
using OGA.MSSQL.DAL.CreateVerify.Model;
using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.Data.Common;
using System.Text;
using OGA.MSSQL.DAL_SP.Model;

namespace OGA.MSSQL.DAL
{
    public class TableDefinition
    {
        private List<TableColumnDef> columnlist;

        public string tablename { get; private set; }

        public string schemaname { get; set; } = "dbo";

        /// <summary>
        /// Set this flag if you want the generated SQL to include a drop command.
        /// Off by default.
        /// </summary>
        public bool Cfg_IncludeDrop_IfExists { get; set; } = false;

        /// <summary>
        /// Number of defined columns in table definition.
        /// </summary>
        public int ColumnCount { get => this.columnlist?.Count ?? 0; }


        public TableDefinition(string tablename)
        {
            columnlist = new List<TableColumnDef>();

            if (string.IsNullOrWhiteSpace(tablename))
                throw new Exception("Invalid table name");

            this.tablename = tablename;
        }


        /// <summary>
        /// Adds a boolean column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_Boolean_Column(string colname, bool canbenull)
        {
            return this.Add_Column(colname, SQL_Datatype_Names.CONST_SQL_bit, canbenull);
        }

        /// <summary>
        /// Call this method to add a primary key column to the table.
        /// Accepts an optional varchar length parameter, for primary keys of varchar type.
        /// Accepts an optional identity behavior parameter (identitybehavior), that is set if the column will generate its own sequence identifiers.
        /// NOTE: identitybehavior is ONLY for datatypes of bigint, integer, and uuid. All other datatypes will fail validation.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="datatype"></param>
        /// <param name="identitybehavior"></param>
        /// <param name="varcharlength"></param>
        /// <returns></returns>
        public int Add_Pk_Column(string colname, ePkColTypes datatype, eIdentityBehavior identitybehavior = eIdentityBehavior.UNSET, int? varcharlength = null)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                    $"Column name is empty.");

                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.ColumnExists(colname))
            {
                // Column name already exists.
                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                    $"Column name already exists.");

                return -1;
            }

            // Ensure a pk doesn't already exist...
            if(this.columnlist.Exists(m=>m.IsPk == true))
            {
                // A primary key column already exists.
                // Cannot add another one.

                OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                    $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                    $"A Primary Key column already exists.");

                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = true;
            cd.Collate = "";
            cd.CanBeNull = false;

            // Set the datatype...
            if (datatype == ePkColTypes.uuid)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_uniqueidentifier;
            else if (datatype == ePkColTypes.integer)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_int;
            else if (datatype == ePkColTypes.bigint)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_bigint;
            else if (datatype == ePkColTypes.varchar)
            {
                // The caller wants to add a varchar primary key.
                // We will require it to have a defined max length.
                if(varcharlength == null)
                {
                    // Caller failed to give us a length for the varchar.
                    OGA.SharedKernel.Logging_Base.Logger_Ref?.Error(
                        $"{nameof(TableDefinition)}:-:{nameof(Add_Pk_Column)} - " +
                        $"Cannot create varchar primary key without max length.");

                    return -2;
                }

                cd.ColType = SQL_Datatype_Names.CONST_SQL_nvarchar + $"({varcharlength.Value.ToString()})";
            }

            // Set identity behavior...
            {
                // Start as unset, and update below if required...
                cd.IdentityBehavior = eIdentityBehavior.UNSET;

                // Set identity behavior clause...
                // We ignore it for invalid types.
                if(datatype == ePkColTypes.bigint ||
                    datatype == ePkColTypes.integer)
                {
                    // Numeric datatypes are valid for identity behavior usage.

                    if (identitybehavior == eIdentityBehavior.GenerateByDefault)
                        cd.IdentityBehavior = eIdentityBehavior.GenerateByDefault;
                    if (identitybehavior == eIdentityBehavior.GenerateAlways)
                        cd.IdentityBehavior = eIdentityBehavior.GenerateAlways;
                }
                else if(datatype == ePkColTypes.uuid)
                {
                    // UUID datatype is valid for default value generation.

                    if (identitybehavior == eIdentityBehavior.GenerateByDefault)
                        cd.IdentityBehavior = eIdentityBehavior.GenerateByDefault;
                }
            }

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a non-UTC datetime column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_DateTime_Column(string colname, bool canbenull)
        {
            return this.Add_Column(colname, SQL_Datatype_Names.CONST_SQL_datetime2, canbenull);
        }

        /// <summary>
        /// Adds a UTC datetime column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_UTCDateTime_Column(string colname, bool canbenull)
        {
            return this.Add_Column(colname, SQL_Datatype_Names.CONST_SQL_datetimeoffset, canbenull);
        }

        /// <summary>
        /// Adds a Guid (UUID) column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_Guid_Column(string colname, bool canbenull)
        {
            return this.Add_Column(colname, SQL_Datatype_Names.CONST_SQL_uniqueidentifier, canbenull);
        }

        /// <summary>
        /// Adds a numeric column to the table schema.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="datatype"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_Numeric_Column(string colname, eNumericColTypes datatype, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.ColumnExists(colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "";
            cd.CanBeNull = canbenull;

            if (datatype == eNumericColTypes.integer)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_int;
            else if (datatype == eNumericColTypes.bigint)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_bigint;
            else if (datatype == eNumericColTypes.real)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_real;
            else if (datatype == eNumericColTypes.double_precision)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_float;
            else if (datatype == eNumericColTypes.numeric)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_decimal + "(18,2)";

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Adds a string column to the table schema.
        /// If length is zero, datatype is 'text'.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="length"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        public int Add_String_Column(string colname, int length, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.ColumnExists(colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.IsPk = false;
            cd.Collate = "COLLATE SQL_Latin1_General_CP1_CI_AS";
            cd.CanBeNull = canbenull;

            if(length <= 0)
                cd.ColType = SQL_Datatype_Names.CONST_SQL_nvarchar + "(max)";
            else
                cd.ColType = SQL_Datatype_Names.CONST_SQL_nvarchar + $"({length.ToString()})";

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// This method will compose the CREATE command to add the database table.
        /// </summary>
        /// <returns></returns>
        public string CreateSQLCmd()
        {
            StringBuilder b = new StringBuilder();

            string indent = "";

            if (this.Cfg_IncludeDrop_IfExists)
            {
                b.AppendLine($"IF OBJECT_ID('{this.schemaname}.{this.tablename}', 'U') IS NULL");
                b.AppendLine("BEGIN");

                indent = "\t";
            }

            // Create the table...
            b.AppendLine(indent + $"CREATE TABLE [{this.schemaname}].[{this.tablename}]");
            b.AppendLine(indent + $"(");

            // Add its columns...
            {
                for (int i = 0; i < this.columnlist.Count; i++)
                {
                    // Localise the current column entry...
                    var c = this.columnlist[i];

                    // Give each column the tablename, in case it's needed for a column constraint name...
                    c.TableName = this.tablename;

                    // Append the column's definition...
                    string line = "\t" + c.ToString();

                    // Append a comma if the last column...
                    if (i < this.columnlist.Count - 1 || !this.columnlist.Exists(x => x.IsPk))
                        line += ",";

                    b.AppendLine(indent + line);
                }

                // Add a primary key constraint if needed...
                try
                {
                    var pkc = this.columnlist.Find(n => n.IsPk == true);
                    if(pkc != null)
                    {
                        // Build a constraint string for it...
                        // Be sure to add a leading comma to it, since it comes after the column list, but is still in the block where the column list goes.
                        b.AppendLine(indent + $"\t, CONSTRAINT [PK_{this.tablename}] PRIMARY KEY ([{pkc.ColName}])");
                    }
                } catch(Exception) { }
            }

            // Close the column list...
            b.AppendLine(indent + $")");

            // Append an end, if we added a drop...
            if(this.Cfg_IncludeDrop_IfExists)
                b.AppendLine("END;");

            return b.ToString();
        }


        /// <summary>
        /// Centralized method for handling most column adds.
        /// </summary>
        /// <param name="colname"></param>
        /// <param name="coltype"></param>
        /// <param name="canbenull"></param>
        /// <returns></returns>
        private int Add_Column(string colname, string coltype, bool canbenull)
        {
            if(string.IsNullOrWhiteSpace(colname))
            {
                // Invalid column name.
                return -1;
            }

            // Ensure the column name doesn't already exist...
            if(this.ColumnExists(colname))
            {
                // Column name already exists.
                return -1;
            }

            var cd = new TableColumnDef();
            cd.ColName = colname;
            cd.CanBeNull = canbenull;
            cd.ColType = coltype;
            cd.IsPk = false;
            cd.Collate = "";

            this.columnlist.Add(cd);

            return 1;            
        }

        /// <summary>
        /// Centralized method that checks if the column exists in our table definition.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private bool ColumnExists(string name)
        {
            return this.columnlist.Exists(c => c.ColName.Equals(name, StringComparison.OrdinalIgnoreCase));
        }
    }
}
