package org.dbfloader.app.db

import org.dbfloader.app.reader.Field


object SQLBulder {


  def createFieldBlock(fields: List[Field],f: String => String,sep:String = ",") =
    fields.foldLeft("")((str, field) => s"$str${f(field.name)}${if (field.index != fields.length-1) sep else ""}")

  def generateSqlInsert(tableName:String, fields:List[Field], existsId:Boolean) = {

    val keyColumns = if (existsId) ",ID" else ""
    val keyValues = if (existsId) ",:ID" else ""

    s"""
       |insert into $tableName
       |(${createFieldBlock(fields,str => str)}$keyColumns)
       |values
       |(${createFieldBlock(fields,str => s":$str")}$keyValues)
     """.stripMargin
  }
  
  def generateSqlCheckExistsTable(tableName:String) =
    s"select t.table_name from user_tables t where t.table_name = '$tableName'"
  
  def generateSqlCreateTable(tableName:String, fields:List[Field]) = {
    s"""
       |create table $tableName
       |(
       |${createFieldBlock(fields,f => s"$f varchar2(500)",",\n")}
       |)
     """.stripMargin
  }


}
