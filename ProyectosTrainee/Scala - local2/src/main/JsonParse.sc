import scala.util.parsing.json.JSON
import scala.collection.immutable.ListMap
val dato = """
{
  "nombre": "gonza",
  "edad": 21,
  "trabajo": "ingeniero de datos"
}
"""

val resultado = JSON.parseFull(dato)
resultado match {
  case Some(map: Map[String, Any]) => val map2 = ListMap(map.toSeq.sortBy(_._1):_*)
    println(map2)
  case None => println("Error al parsear")
}
