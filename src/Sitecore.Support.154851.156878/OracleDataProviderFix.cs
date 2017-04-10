using System;
using Sitecore.Data;
using Sitecore.Data.DataProviders;
using Sitecore.Data.DataProviders.Sql;
using Sitecore.Data.Items;
using Sitecore.Data.Oracle;
using Sitecore.Diagnostics;
using Sitecore.Text;

namespace Sitecore.Support.Data.Oracle
{
    public class OracleDataProvider : Sitecore.Data.Oracle.OracleDataProvider
    {
        public OracleDataProvider(string connectionString) : base(connectionString)
        {
        }
        public override bool AddToPublishQueue(ID itemID, string action, DateTime date, string language, CallContext context)
        {
            OracleDataApi oracleDataApi = base.Api as OracleDataApi;
            Assert.IsNotNull(oracleDataApi, "Invalid Data API");
            //string sql = "INSERT INTO {0}PublishQueue{1} (     {0}ItemID{1}, {0}Language{1}, {0}Version{1}, {0}Date{1}, {0}Action{1}   )   VALUES(     {2}itemID{3}, {2}language{3}, {2}version{3}, {2}date{3}, {2}action{3}   )";
            string sql = "DECLARE cnt NUMBER;\nBEGIN  SELECT COUNT(*) INTO cnt    FROM {0}PublishQueue{1} WHERE ({0}ItemID{1} = {2}itemID{3} AND {0}Language{1} = {2}language{3} AND {0}Date{1} = {2}date{3} AND {0}Action{1} = {2}action{3});\n  IF( cnt = 0 )  THEN   INSERT INTO {0}PublishQueue{1} (     {0}ItemID{1}, {0}Language{1}, {0}Version{1}, {0}Date{1}, {0}Action{1}   ) VALUES(     {2}itemID{3}, {2}language{3}, {2}version{3}, {2}date{3}, {2}action{3}   );\n  END IF;\nEND;\n";
            oracleDataApi.Execute(sql, new object[]
            {
        "itemID",
        itemID,
        "language",
        language,
        "version",
        0,
        "date",
        date,
        "action",
        action
            });
            return true;
        }
        protected override void WriteVersionedField(ID itemId, FieldChange change, DateTime now, bool fieldsAreEmpty)
        {
            string sql;
            if (!fieldsAreEmpty)
            {
                sql = "\r\nDECLARE\r\n  vString {0}VersionedFields{1}.{0}Value{1}%Type;\r\n                BEGIN\r\n                vString :={2}value{3};\r\nMERGE INTO {0}VersionedFields{1} F\r\nUSING\r\n(SELECT {2}itemId{3} IId,\r\n        {2}fieldId{3} FId,\r\n        {2}language{3} L,\r\n        {2}version{3} V\r\n  FROM DUAL) S\r\nON (    F.{0}ItemId{1} = S.IId\r\n    AND F.{0}FieldId{1} = S.FId\r\n    AND (F.{0}Language{1} = S.L OR (F.{0}Language{1} IS NULL AND S.L IS NULL))\r\n    AND F.{0}Version{1} = S.V)\r\nWHEN MATCHED THEN UPDATE\r\n  SET F.{0}Value{1} = vString, F.{0}Updated{1} = {2}now{3}\r\nWHEN NOT MATCHED THEN INSERT\r\n    (F.{0}ItemId{1}, F.{0}Language{1}, F.{0}Version{1}, F.{0}FieldId{1}, F.{0}Value{1}, F.{0}Created{1}, F.{0}Updated{1})\r\n  VALUES\r\n    ({2}itemId{3}, {2}language{3}, {2}version{3}, {2}fieldId{3}, vString, {2}now{3}, {2}now{3});\r\nEND;";
            }
            else
            {
                sql = "INSERT INTO {0}VersionedFields{1} ( \r\n                    {0}ItemId{1}, {0}Language{1}, {0}Version{1}, {0}FieldId{1}, {0}Value{1}, {0}Created{1}, {0}Updated{1}\r\n                  )\r\n                  VALUES (\r\n                    {2}itemId{3}, {2}language{3}, {2}version{3}, {2}fieldId{3}, {2}value{3}, {2}now{3}, {2}now{3}\r\n                  )";
            }
            BigString bigString = (change.Value != string.Empty) ? new BigString(change.Value) : new BigString("__#!$No value$!#__");
            base.Api.Execute(sql, new object[]
            {
        "itemId",
        itemId,
        "language",
        change.Language,
        "version",
        change.Version,
        "fieldId",
        change.FieldID,
        "value",
        bigString,
        "now",
        now
            });
        }
        protected override void WriteUnversionedField(ID itemId, FieldChange change, DateTime now, bool fieldsAreEmpty)
        {
            string sql;
            if (!fieldsAreEmpty)
            {
                sql = "\r\nDECLARE\r\n  vString {0}UnversionedFields{1}.{0}Value{1}%Type;\r\n                BEGIN\r\n                vString :={2}value{3};\r\nMERGE INTO {0}UnversionedFields{1} F\r\nUSING (\r\n  SELECT {2}itemId{3} IId, \r\n         {2}fieldId{3} FId,\r\n         {2}language{3} L\r\n  FROM DUAL) S\r\nON (F.{0}ItemId{1} = S.IId AND F.{0}FieldId{1} = S.FId\r\n   AND (F.{0}Language{1} = S.L OR (F.{0}Language{1} IS NULL AND S.L IS NULL)))\r\nWHEN MATCHED THEN UPDATE\r\n    SET F.{0}Value{1} = vString,\r\n        F.{0}Updated{1} = {2}now{3}\r\nWHEN NOT MATCHED THEN INSERT\r\n      (F.{0}ItemId{1}, F.{0}Language{1}, F.{0}FieldId{1}, F.{0}Value{1}, F.{0}Created{1}, F.{0}Updated{1})\r\n    VALUES \r\n      ({2}itemId{3}, {2}language{3}, {2}fieldId{3}, vString, {2}now{3}, {2}now{3});\r\nEND;";
            }
            else
            {
                sql = " INSERT INTO {0}UnversionedFields{1} (   {0}ItemId{1}, {0}Language{1}, {0}FieldId{1}, {0}Value{1}, {0}Created{1}, {0}Updated{1} ) VALUES (   {2}itemId{3}, {2}language{3}, {2}fieldId{3}, {2}value{3}, {2}now{3}, {2}now{3} )";
            }
            BigString bigString = (change.Value != string.Empty) ? new BigString(change.Value) : new BigString("__#!$No value$!#__");
            base.Api.Execute(sql, new object[]
            {
        "itemId",
        itemId,
        "language",
        change.Language,
        "fieldId",
        change.FieldID,
        "value",
        bigString,
        "now",
        now
            });
        }
        protected override void WriteSharedField(ID itemId, FieldChange change, DateTime now, bool fieldsAreEmpty)
        {
            string sql;
            if (!fieldsAreEmpty)
            {
                sql = "\r\nDECLARE\r\n  vString {0}SharedFields{1}.{0}Value{1}%Type;\r\n                BEGIN\r\n                vString :={2}value{3};\r\nMERGE INTO {0}SharedFields{1} F\r\nUSING (\r\n  SELECT \r\n    {2}itemId{3} IId,\r\n    {2}fieldId{3} FId\r\n  FROM DUAL) S\r\nON (F.{0}ItemId{1} = S.IId AND F.{0}FieldId{1} = S.FId)\r\nWHEN MATCHED THEN UPDATE\r\n  SET {0}Value{1} = vString, {0}Updated{1} = {2}now{3}\r\nWHEN NOT MATCHED THEN INSERT\r\n    (F.{0}ItemId{1}, F.{0}FieldId{1}, F.{0}Value{1}, F.{0}Created{1}, F.{0}Updated{1})\r\n  VALUES\r\n    ({2}itemId{3}, {2}fieldId{3}, vString, {2}now{3}, {2}now{3});\r\nEND;";
            }
            else
            {
                sql = " INSERT INTO {0}SharedFields{1} (\r\n                     {0}ItemId{1}, {0}FieldId{1}, {0}Value{1}, {0}Created{1}, {0}Updated{1}\r\n                   )\r\n                   VALUES (\r\n                     {2}itemId{3}, {2}fieldId{3}, {2}value{3}, {2}now{3}, {2}now{3}\r\n                   )";
            }
            BigString bigString = (change.Value != string.Empty) ? new BigString(change.Value) : new BigString("__#!$No value$!#__");
            base.Api.Execute(sql, new object[]
            {
        "itemId",
        itemId,
        "fieldId",
        change.FieldID,
        "value",
        bigString,
        "now",
        now
            });
        }
    }
}