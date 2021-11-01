val lista = Seq(2,5,0,1,6,9,69,84,14,1)

for(i <- lista){
  if( i == 0 && (i+1 != 0)){
    lista.indexOf(i)
  }
}