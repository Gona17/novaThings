import org.json4s._
import org.json4s.native.JsonMethods._

//implicit val formats = DefaultFormats
implicit val formats: DefaultFormats.type = DefaultFormats
val dato = """
{
  "nombre": "gonza",
  "edad": 21,
  "trabajo": "ingeniero de datos"
}
"""

val json = parse(dato)
json.extract[Map[String,Any]]
print(json)