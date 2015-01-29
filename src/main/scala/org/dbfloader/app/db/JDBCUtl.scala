package org.dbfloader.app.db

import java.sql.ResultSet
import java.util

import grizzled.slf4j.Logging
import org.dbfloader.app.LoadUtl
import org.dbfloader.app.reader.{DBIndex, Field}
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.{RowMapper, JdbcTemplate}
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

  def createTable(tableName:String,fields:List[Field], sqlBuilder: (String,List[Field]) => String) = {

    val sqlCreateTable = sqlBuilder(tableName, fields).trim

    info(
      s"""
          |SQL:<
          |$sqlCreateTable
       """.stripMargin)

    jdbcTemplate.execute(sqlCreateTable)

    jdbcTemplate.execute(s"create index I_CB_$tableName on $tableName (CODE_BASE)");

  }


  def getFields(tableName:String):List[Field] = {

    val sqlQueryColumn =
      s"""|select  t.column_name,
          |      case t.data_type
          |        when 'VARCHAR2' then
          |         t.data_type || '(' || t.data_length || ')'
          |        else
          |         t.data_type
          |      end as data_type,
          |      column_id - 1 as column_id,
          |      t.nullable
          |  from user_tab_columns t
          | where t.table_name = '$tableName'
          | order by t.column_id""".stripMargin

    val rm = new RowMapper[Field] {
      override def mapRow(rs: ResultSet, rowNum: Int): Field = {
        Field(rs.getString(1),rs.getString(2),rs.getInt(3), rs.getString(4))
      }
    }
    
    jdbcTemplate.query(sqlQueryColumn,rm).toArray(Array[Field]()).toList
  }


  def existsId(tableName:String) = {
    val l = jdbcTemplate.queryForList(sqlExistsId,tableName,"id")
    l.size == 1
  }

  def getMaxId(sql:String):Int = {
    jdbcTemplate.queryForObject(sql,classOf[java.lang.Integer])
  }

  def loadToDB(records:util.List[Array[Object]],sqlInsert:String,tableName:String,codeBase:String) = {

    info(s"start load $tableName codeBase: $codeBase")

    val transactionDefinition = new DefaultTransactionDefinition
    val transactionStatus = transactionManager.getTransaction(transactionDefinition)

    try {
      if (LoadUtl.writeToDb) {
        jdbcTemplate.update(s"delete $tableName where code_base = :CODE_BASE",codeBase)
        jdbcTemplate.batchUpdate(sqlInsert, records)
        transactionManager.commit(transactionStatus)
        info(s"table $tableName   ...........OK")
      }
    } catch {
      case e:Exception => {
        transactionManager.rollback(transactionStatus)
        error(s"tableName = $tableName, error!!!! = ${e.toString}")
        //throw e
      }
    }
  }

  def copyData(tableSource:String,tableTarget:String,fields:List[Field], codeBase:String) = {
    val sqlInsert = SQLBulder.generateInsertForCopy(tableSource,tableTarget,fields,codeBase)
    jdbcTemplate.update(sqlInsert)
  }

  def getIndex(tableName:String) = {
    val rm = new RowMapper[DBIndex] {
      override def mapRow(rs: ResultSet, rowNum: Int): DBIndex = {
        val uniq = rs.getString(2)
        DBIndex(rs.getString(1),if (uniq == null) "" else uniq)
      }
    }
    jdbcTemplate.query(SQLBulder.generateGetIndex,rm,tableName).toArray(Array[DBIndex]()).toList
  }

  def getIndexColumns(indexName:String) = {
    val rm = new RowMapper[String] {
      override def mapRow(rs: ResultSet, rowNum: Int): String = {
        val expr = rs.getString(2)
        if (expr == null)
          rs.getString(1)
        else
          expr//.getSubString(1,30).trim
      }
    }
    jdbcTemplate.query(SQLBulder.generateGetIndexColumns,rm,indexName).toArray(Array[String]()).toList
  }

  def createIndex(tableName:String,index:DBIndex,columns:List[String], transformName:(String)=>String) = {
    val sql: String = SQLBulder.generateCreateIndex(tableName, index, columns, transformName)
    info(sql)
    jdbcTemplate.execute(sql)
  }
}
