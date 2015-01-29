package org.dbfloader.app.db

import grizzled.slf4j.Logging
import org.dbfloader.app.reader.{DBIndex, Field}


object SQLBulder extends Logging{

  def createFieldBlock(fields: List[Field],f: Field => String,sep:String = ",") =
    fields.foldLeft("")((str, field) => s"$str${f(field)}${if (field.index != fields.length-1) sep else ""}")

  def generateSqlInsert(tableName:String, fields:List[Field], existsId:Boolean) = {

    val keyColumns = if (existsId) ",ID" else ""
    val keyValues = if (existsId) ",:ID" else ""

    s"""
       |insert into $tableName
       |(${createFieldBlock(fields,field => field.name)}$keyColumns,CODE_BASE)
       |values
       |(${createFieldBlock(fields,(f:Field) => s":${f.name}")}$keyValues,:CODE_BASE)
     """.stripMargin
  }
  
  def generateSqlCheckExistsTable =
    s"select t.table_name from user_tables t where t.table_name = upper(?)"
  
  def generateSqlCreateTable(tableName:String, fields:List[Field]) = {
    s"""
       |create table $tableName
       |(
       |${createFieldBlock(fields,f => s"${f.name} varchar2(500)",",\n")}
       |)
     """.stripMargin
  }

  def generateSqlCreateTableWithTypes(tableName:String, fields:List[Field]) = {
    s"""
       |create table $tableName
       |(
       |${createFieldBlock(fields,f => s"${f.name} ${f.typeField}${if (f.nullable == "N") " not null" else ""}",",\n")}
       |)
     """.stripMargin
  }

  def generateInsertForCopy(tableSource:String,tableTarget:String,fields:List[Field], codeBase:String ) = {

    val fieldsStr = createFieldBlock(fields,field => field.name)

    s"""
       |insert into $tableTarget
       |($fieldsStr,CODE_BASE)
       |select
       |$fieldsStr,'$codeBase' as codeBase
       |  from $tableSource
     """.stripMargin

  }

  def generateGetIndex = {
    s"""
       |select i.index_name,
       |       decode(i.uniqueness, 'UNIQUE', 'unique', '') as uniqueness
       |  from user_indexes i
       | where i.table_name = :tableName
     """.stripMargin
  }

  def generateGetIndexColumns = {
    s"""
       |select t.column_name, ie.column_expression
       |  from user_ind_columns t,
       |       user_ind_expressions ie
       | where t.index_name = :indexName
       |   and ie.index_name(+) = t.index_name
       |   and ie.column_position(+) = t.column_position
       | order by t.column_position
     """.stripMargin
  }

  def generateCreateIndex(tableName:String,index:DBIndex,columns:List[String], transformName:(String)=>String) = {
    val columnsStr = columns.foldLeft("")((str,column) => s"$str${if (str != "") ", " else ""}$column" )
    info(s"columns: $columns; sql: $columnsStr")
    s"create ${index.uniqueness} index ${transformName(index.name)}_ on ${tableName} ($columnsStr)"
  }


  def generateSelectMax(tables: List[String]) = {

    def addUnion = {
      s"""
         | union all""".stripMargin
    }

    def generate(tables: List[String]) = {
      tables.foldRight("")((tableName, sql) =>
        s""" |$sql ${if (sql != "") addUnion else ""}
         |select id
         |  from ${tableName}""".stripMargin)
    }

    s"""|select max(id) from
      |(${generate(tables)}
      |)""".stripMargin
  }
}
