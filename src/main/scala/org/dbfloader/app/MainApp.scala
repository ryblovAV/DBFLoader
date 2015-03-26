package org.dbfloader.app

import grizzled.slf4j.Logging
import org.dbfloader.app.reader.FileReader

object MainApp extends App with Logging {

  def getLeskCodeBase(fileName: String) = fileName.substring(0, 2)

  def getGeskCodeBase(fileName: String) = "15"

  def getLeskEntityName(fileName: String) = fileName.replace(".DBF", "").substring(3)

  def getGeskEntityName(fileName: String) = fileName.replace(".DBF", "")

  def loadLesk =
    FileReader.groupFilesByEntity(
    path = LoadUtl.path,
    prefix = "LESK_JUR_",
    getEntityName = getLeskEntityName,
    getCodeBase = getLeskCodeBase)

  def load =
    FileReader.groupFilesByEntity(
      path = LoadUtl.path,
      prefix = "GESK_PH_",
      getEntityName = ((n) => n.replace(".dbf","").substring(0,n.indexOf("_"))),
      getCodeBase = (n) => n.replace(".dbf","").substring(n.indexOf("_")+1))


  def loadGesk = FileReader.groupFilesByEntity(
    path = LoadUtl.path,
    prefix = "GESK_JUR_",
    getEntityName = getGeskEntityName,
    getCodeBase = getGeskCodeBase) //.filter((a)=>a._1 == "opla")

  LoadUtl.loadAll(load)


  //  val entityList = FileReader.getListEntity(LoadUtl.path).filter(_ == "tuchD")
  //  entityList.foreach(LoadUtl.createCopyTable(_))

  logger.info("finish")
}
