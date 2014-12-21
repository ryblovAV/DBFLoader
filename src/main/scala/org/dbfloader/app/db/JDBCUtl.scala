package org.dbfloader.app.db

import java.util

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.stereotype.Repository

@Repository("outReader")
class JDBCUtl {

  val sqlExistsId =
    s"""
       |select 1
       |  from user_tab_columns t
       | where t.table_name = ?
       |   and t.column_name = 'ID'
     """.stripMargin

  @Autowired
  protected var jdbcTemplate:JdbcTemplate = _

  def existsId(tableName:String) = {
    val l = jdbcTemplate.queryForList(sqlExistsId,tableName)
    l.size == 1
  }

  def delete(tableName:String) = {
    jdbcTemplate.update(s"delete $tableName")
  }

  def butchInsert(records:util.List[Array[Object]],sqlInsert:String) = {
    jdbcTemplate.batchUpdate(sqlInsert,records)
  }

}
