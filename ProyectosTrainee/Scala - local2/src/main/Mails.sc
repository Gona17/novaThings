object AutoMail extends App {

  import java.util.Properties
  import javax.mail.{Message, Session}
  import javax.mail.internet.{InternetAddress, MimeMessage}
  import scala.io.Source

  val host = "smtp.gmail.com"
  val port = "25"
  val address = "thomaspadilla.tp@gmail.com"
  val username = "informacion.pipelines@gmail.com"
  val password = "Sampo2020"
  def sendEmail(mailSubject: String, mailContent: String): Unit = {
    val properties = new Properties()
    properties.put("mail.smtp.port", port)
    properties.put("mail.smtp.auth", "true")
    properties.put("mail.smtp.starttls.enable", "true")
    val session = Session.getDefaultInstance(properties, null)
    val message = new MimeMessage(session)
    message.addRecipient(Message.RecipientType.TO, new InternetAddress(address))
    message.setSubject(mailSubject)
    message.setContent(mailContent, "text/html")
    val transport = session.getTransport("smtp")
    transport.connect(host, username, password)
    transport.sendMessage(message, message.getAllRecipients)
  }

  val message = "mensaje"
  val subject = "nombre del mail"

  sendEmail(subject, message)

}