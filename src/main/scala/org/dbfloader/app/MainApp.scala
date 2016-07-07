package org.dbfloader.app

import grizzled.slf4j.Logging
import org.dbfloader.app.reader.{SourceFile, FileReader}

object MainApp extends App with Logging {

  def getLeskCodeBase(fileName: String) = fileName.substring(0, 2)

  def getGeskCodeBase(fileName: String) = fileName.replace(".DBF", "").takeRight(4).take(2)

  def getLeskEntityName(fileName: String) = fileName.replace(".DBF", "").substring(3)

  def getGeskEntityName(fileName: String) = fileName.replace(".DBF", "").dropRight(4)

  def loadLesk =
    FileReader.groupFilesByEntity(
    path = "//Users//user//data//pokaz_22",
    prefix = "LESK_JUR_ADD_",
    getEntityName = ((fileName) => fileName.toUpperCase.replace(".DBF", "").substring(3)),
    getCodeBase = getLeskCodeBase)

  def loadFias =
    FileReader.groupFilesByEntity(
      path = "//Users//user//data//import//1",
      prefix = "CM_",
      getEntityName = ((fileName) => fileName.replace(".DBF", "")),
      getCodeBase = (x) => "0")


  def loadLeskChGor =
    FileReader.groupFilesByEntity(
      path = "//Users//user//data//ch_g",
      prefix = "LESK_JUR_ADD_",
      getEntityName = ((fileName) => fileName.replace(".DBF", "").substring(3)),
      getCodeBase = getLeskCodeBase)

  def loadLeskPok =
    FileReader.groupFilesByEntity(
      path = "//Users//user//data//pok_18_02_2016",
      prefix = "LESK_JUR_ADD_",
      getEntityName = ((fileName) => fileName.replace(".DBF", "").substring(3).dropRight(2)),
      getCodeBase = getLeskCodeBase)


  def loadLeskPokaz =
    FileReader.groupFilesByEntity(
    path = "//Users//user//data//pokaz_old",
    prefix = "LESK_JUR_OLD_ADD_",
    getEntityName = (f => "pokaz"),
    getCodeBase = getLeskCodeBase)

  def load =
    FileReader.groupFilesByEntity(
      path = LoadUtl.path,
      prefix = "GESK_PH_",
      getEntityName = ((n) => n.replace(".dbf","").substring(0,n.indexOf("_"))),
      getCodeBase = (n) => n.replace(".dbf","").substring(n.indexOf("_")+1))


  def loadGesk = FileReader.groupFilesByEntity(
    path = "//Users//a123//data//dataGeskJune",
    prefix = "GESK_JUR_",
    getEntityName = ((fileName) => fileName.toLowerCase.replace(".dbf","")),
    getCodeBase = ((fileName) => "00"))

  def loadMKD = FileReader.groupFilesByEntity(
    path = "///Users/user/data/data_mkd",
    prefix = "CM_MKD_",
    getEntityName = ((fileName) => fileName.toLowerCase.substring(3).replace(".dbf","")),
    getCodeBase = ((fileName) => fileName.take(2)))


  def loadVerification = FileReader.groupFilesByEntity(
    path = LoadUtl.pathVerification,
    prefix = "CHECK_JUR_",
    getEntityName = ((fileName) => "VERIFICATION"),
    getCodeBase = ((fileName) => fileName.take(2)))

//  val entityList = loadLeskPokaz
  val entityList = loadLesk

//  val entityList = List("06","07","08","09","10","11","12").map(s => FileReader.groupFilesByEntity(
//    path = s"//Users//user//data//pokaz//$s",
//    prefix = "LESK_JUR_ADD_",
//    getEntityName = ((fileName) => fileName.replace(".DBF", "").substring(3)),
//    getCodeBase = getLeskCodeBase))

//  println("entityList = " + entityList)

  LoadUtl.truncateAll(entityList)
  LoadUtl.loadAll(entityList)

//  entityList.foreach(LoadUtl.loadAll(_))


//    val entityList = FileReader.getListEntity(LoadUtl.path).filter(_ == "tuchD")
//    entityList.foreach(LoadUtl.createCopyTable(_))

  logger.info("finish")
}
