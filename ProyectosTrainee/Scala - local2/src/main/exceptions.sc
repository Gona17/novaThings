import java.io.IOException
try {
  val a = 2/0
}
catch{

    // Catch block contain cases.
    case i: IOException =>
    {
      println("IOException occurred.")
    }
    case b : ArithmeticException =>
    {
      println("Arithmetic Exception occurred.")
    }
}
finally
{
  // Finally block will execute
  println("This is final block.")
}





