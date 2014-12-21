package org.dbfloader.app.db

import org.dbfloader.app.reader.Field


object SQLBulder {


  def createFieldBlock(fields: List[Field],f: String => String) =
    fields.foldLeft("")((str, field) => s"$str${if (field.index != 0) "," else ""}${f(field.name)}")

  def generateSql(tableName:String, fields:List[Field], existsId:Boolean) = {

    val keyColumns = if (existsId) ",ID" else ""
    val keyValues = if (existsId) ",:ID" else ""

    s"""
       |insert into $tableName
       |(${createFieldBlock(fields,str => str)}$keyColumns)
       |values
       |(${createFieldBlock(fields,str => s":$str")}$keyValues)
     """.stripMargin
  }

}
