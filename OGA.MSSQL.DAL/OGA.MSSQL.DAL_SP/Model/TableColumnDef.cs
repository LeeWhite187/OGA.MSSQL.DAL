using OGA.MSSQL.DAL.CreateVerify.Model;
using OGA.MSSQL.DAL_SP.Model;
using System;
using System.Collections.Generic;
using System.Text;

namespace OGA.MSSQL.DAL.Model
{
    public class TableColumnDef
    {
        /// <summary>
        /// Populate this value with the table name, if the generated SQL requires.
        /// Examples that require the table name are:
        ///     The column is a UUID and is set to auto-generate values.
        /// </summary>
        public string TableName { get; set; }

        public string ColName { get; set; }

        public string ColType { get; set; }

        public string Collate { get; set; }

        public bool IsPk { get; set; }

        public bool CanBeNull { get; set; }

        /// <summary>
        /// This property is ONLY valid to be set for bigint and integer datatypes.
        /// Leave as default for all other types.
        /// </summary>
        public eIdentityBehavior IdentityBehavior { get; set; } = eIdentityBehavior.UNSET;


        public override string ToString()
        {
            var sb = new StringBuilder();

            // Start with the column name and type...
            sb.Append($"[{this.ColName}] {this.ColType}");

            // Add the collate clause, if needed...
            // Only char types can take COLLATE; skip numeric types.
            if (!string.IsNullOrWhiteSpace(this.Collate) &&
                this.ColType.StartsWith(SQL_Datatype_Names.CONST_SQL_nchar, StringComparison.OrdinalIgnoreCase) == false &&
                this.ColType.StartsWith(SQL_Datatype_Names.CONST_SQL_nvarchar, StringComparison.OrdinalIgnoreCase) == false)
            {
                sb.Append($" {this.Collate}");
            }

            // Declare the column as non-nullable, if set...
            if (!this.CanBeNull)
                sb.Append(" NOT NULL");

            // Add identity sequencing if needed...
            // SQL Server only supports IDENTITY(x,y) 
            if (this.IdentityBehavior == eIdentityBehavior.GenerateByDefault)
            {
                // The column expects some rule about identity creation.

                // Specify the correct identity method by column type...
                if(this.ColType == SQL_Datatype_Names.CONST_SQL_int || this.ColType == SQL_Datatype_Names.CONST_SQL_bigint)
                {
                    sb.Append(" IDENTITY(1,1)");
                }
                if(this.ColType == SQL_Datatype_Names.CONST_SQL_uniqueidentifier)
                {
                    sb.Append($" CONSTRAINT [DF_{this.TableName}_{this.ColName}] DEFAULT NEWSEQUENTIALID()");
                }
            }

            return sb.ToString();
        }
    }

    public enum ePkColTypes
    {
        /// <summary>
        /// UUID primary key
        /// </summary>
        uuid = 1,
        /// <summary>
        /// 32-bit integer primary key
        /// </summary>
        integer = 2,
        /// <summary>
        /// 64-bit integer primary key
        /// </summary>
        bigint = 3,
        /// <summary>
        /// Varchar primary key
        /// </summary>
        varchar = 4,
    }

    public enum eNumericColTypes
    {
        /// <summary>
        /// Covers .NET Int32 type
        /// </summary>
        integer = 1,
        /// <summary>
        /// Covers .NET Int64 type
        /// </summary>
        bigint = 2,
        /// <summary>
        /// Covers .NET float type
        /// </summary>
        real = 3,
        /// <summary>
        /// Covers .NET double type
        /// </summary>
        double_precision = 4,
        /// <summary>
        /// Covers .NET decimal type
        /// </summary>
        numeric = 5,
    }
}
