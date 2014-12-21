package org.dbfloader.app

import java.text.SimpleDateFormat
import java.util

import grizzled.slf4j.Logger

object Transformation {

  val logger = Logger("org.dbfloader.app.Transformation")

  val formatter = new SimpleDateFormat("dd.MM.yyyy");

  //transform number, date to String
  private def transformValues(records:List[Array[Object]]):List[Array[Object]] = {

    def objectToStr(value:Object):String = {

      def dateToStr(dt:java.util.Date):String = {
        formatter.format(dt)
      }

      value match {
        case "  .  .      " => ""
        case null => ""
        case dt: util.Date => dateToStr(dt)
        case _ => value.toString
      }
    }

    def arrayObjectToArrayString(a:Array[Object]):Array[Object] =
      a.map(objectToStr(_))

    records.map(arrayObjectToArrayString)
  }

  //scala.List to java.util.Arraylist for JDBCTemplates
  private def transformToListRecords(records:List[Array[Object]]):util.ArrayList[Array[Object]] = {
    records.foldLeft(new util.ArrayList[Array[Object]])((l,a) => {l.add(a); l})
  }

  def addIdToList(lRecords:java.util.List[Array[Object]], start:Int):Unit = {
    for (i <- 0 to lRecords.size-1) {
      val array = lRecords.get(i)
      val newArray:Array[Object] = array :+ new Integer(start + i + 1)
      lRecords.set(i,newArray)
    }
  }

  def transform(records:List[Array[Object]]):java.util.List[Array[Object]] =
    transformToListRecords(transformValues(records))

}
