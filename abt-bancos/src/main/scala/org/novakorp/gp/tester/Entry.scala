package org.novakorp.gp.tester

object Entry extends SparkSessionWrapper with DefinedFunctions {

  def main(args: Array[String]): Unit = {

    val generateDataBER = new GenerateABTDataBER()
    val dfBER = generateDataBER.generateDataFrame()
    generateDataBER.generateCSV(dfBER)
    generateDataBER.fillABT(dfBER)

    val generateDataBSF = new GenerateABTDataBSF()
    val dfBSF = generateDataBSF.generateDataFrame()
    generateDataBSF.generateCSV(dfBSF)
    generateDataBSF.fillABT(dfBSF)

  }

}
