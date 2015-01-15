package org.dbfloader.app.db

import java.util

import grizzled.slf4j.Logging
import org.dbfloader.app.LoadUtl
import org.dbfloader.app.reader.Field
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.stereotype.Repository
import org.springframework.transaction.{TransactionStatus, TransactionDefinition}
import org.springframework.transaction.support.DefaultTransactionDefinition

import scala.util.Try

@Repository("outReader")
class JDBCUtl extends Logging{

  val sqlExistsId =
    s"""
       |select 1
       |  from user_tab_columns t
       | where t.table_name = ?
       |   and t.column_name = upper(?)
     """.stripMargin

  @Autowired
  protected var jdbcTemplate:JdbcTemplate = _

  @Autowired
  protected var transactionManager:DataSourceTransactionManager = _

//  def existsColumns(tableName:String, fields:List[Field]) = {
//    def existsColumn(field:Field): String = {
//      if (jdbcTemplate.queryForList(sqlExistsId,tableName,field.name) == 0) s"Error!!! In table $tableName not exists column ${field.name} \n"
//      else ""
//    }
//
//    fields.foldLeft("")((message:String,field:Field) => message + )
//
//  }

  def existsTable(tableName:String) = {
    jdbcTemplate.queryForList(SQLBulder.generateSqlCheckExistsTable,tableName).size != 0
  }

  def createTable(tableName:String,fields:List[Field]) = {
    val sqlCreateTable: String = SQLBulder.generateSqlCreateTable(tableName, fields)

    info(
      s"""
         |Create table $tableName
         |SQL:
         |$sqlCreateTable
       """.stripMargin)

    jdbcTemplate.execute(sqlCreateTable)
  }


  def existsId(tableName:String) = {
    val l = jdbcTemplate.queryForList(sqlExistsId,tableName,"id")
    l.size == 1
  }

  def loadToDB(records:util.List[Array[Object]],sqlInsert:String,tableName:String) = {

    info(s"start load $tableName")

    val transactionDefinition = new DefaultTransactionDefinition
    val transactionStatus = transactionManager.getTransaction(transactionDefinition)

    try {
      if (LoadUtl.writeToDb) {
        jdbcTemplate.update(s"delete $tableName")
        jdbcTemplate.batchUpdate(sqlInsert, records)
        transactionManager.commit(transactionStatus)
      }
    } catch {
      case e:Exception => {
        transactionManager.rollback(transactionStatus)
        error(s"tableName = $tableName, error = ${e.toString}")
        throw e
      }
    }
  }

  def test(str:String) = {
    for { v <- Try{str.toInt}} yield v
  }

}
