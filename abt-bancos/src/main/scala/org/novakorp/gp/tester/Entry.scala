package org.novakorp.gp.tester

object Entry extends SparkSessionWrapper with DefinedFunctions {

  def main(args: Array[String]): Unit = {
    val fecha_proceso: String = args(0)
//    val generateDataBER = new GenerateABTDataBER()
//    val dfBER = generateDataBER.generateDataFrame()
//    generateDataBER.generateCSV(dfBER)
//    generateDataBER.fillABT(dfBER,fecha_proceso)

    val generateDataBSF = new GenerateABTDataBSF()
    val dfBSF = generateDataBSF.generateDataFrame()
    generateDataBSF.generateCSV(dfBSF)
    generateDataBSF.fillABT(dfBSF, fecha_proceso)

  }

}
