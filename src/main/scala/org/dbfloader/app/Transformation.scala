package org.dbfloader.app

import java.text.SimpleDateFormat
import java.util


object Transformation {

  val formatter = new SimpleDateFormat("dd.MM.yyyy");

  //transform number, date to String
  private def transformValues(records:List[Array[Object]]):List[Array[Object]] = {

    def objectToStr(value:Object):String = {

      def delToZero(str: String) = if (str.length > 2) {
        if (str.substring(str.length - 2) == ".0") str.substring(0, str.length - 2) else str
      }
      else str

      def dateToStr(dt:java.util.Date):String = {
        formatter.format(dt)
      }

      value match {
        case "  .  .      " => ""
        case null => ""
        case dt: util.Date => dateToStr(dt)
        case _ => delToZero(value.toString.trim)
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

  def addIdToList(lRecords:java.util.List[Array[Object]], startIdValue:Int):Unit = {
    for (i <- 0 to lRecords.size-1) {
      val array = lRecords.get(i)
      val newArray:Array[Object] = array :+ new Integer(startIdValue + i + 1)
      lRecords.set(i,newArray)
    }
  }

  def addCodeBaseToList(lRecords:java.util.List[Array[Object]], codeBase:String):Unit = {
    for (i <- 0 to lRecords.size-1) {
      val array = lRecords.get(i)
      val newArray:Array[Object] = array :+ new String(codeBase)
      lRecords.set(i,newArray)
    }
  }


  def transform(records:List[Array[Object]]):java.util.List[Array[Object]] =
    transformToListRecords(transformValues(records))

}
