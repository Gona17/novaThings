val lista10 = List(2,5,8,1,6,9,69,84,14,0)
/*
def max(lista: List[Int]): Unit ={
  var posicion: Int = 1
  var maximo: Int = lista.head
  var contador: Int = 0
  while (contador <= lista.length && posicion<=9) {
    if (maximo < lista(posicion))
    {
      maximo = lista(posicion)
    }
    else
    {
      posicion = posicion + 1
      contador = contador + 1
    }
  }
  println(s"El máximo de tu lista es ${maximo}")
}

max(lista10)
*/
/*
def max(lista:List[Int])= {
  var maximo: Int = lista.head
  for (x <- lista){
    if(maximo < x){
      maximo = x
    }
  }
  println(maximo)
}
