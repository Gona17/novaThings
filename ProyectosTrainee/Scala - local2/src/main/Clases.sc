class Jugador{
  var name=""
  var arma=""
}

class Amigo extends Jugador

class Enemigo extends Jugador{
  var vida = 5

  def atacar(): Unit ={
    vida-=1
    while (vida>0) {
      println(s"${name} te ha disparado con ${arma} y ahora tenes ${vida} vidas")
      vida-=1
    }
    println("You are dead!")
    }
  }

  val obj = new Enemigo
  obj.name = "Gonza"
  obj.arma= "tu vieja"
  obj.atacar()

