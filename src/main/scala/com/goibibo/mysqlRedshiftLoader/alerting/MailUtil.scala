/**
  * Created by rama on 13/10/16.
  */
package com.goibibo.mysqlRedshiftLoader.alerting

import java.util.Properties
import javax.mail.Message.RecipientType
import javax.mail.internet.{InternetAddress, MimeMessage, _}
import javax.mail.{Session, Transport}

import com.goibibo.mysqlRedshiftLoader._
import org.slf4j.{Logger, LoggerFactory}

class MailUtil(mailParams: MailParams) {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val session = Session.getDefaultInstance(new Properties() {
        put("mail.smtp.host", mailParams.host)
    })


    /** Send email message.
      *
      * @param obj From
      *            // @param tos Recipients
      *            // @param ccs CC Recipients
      *            // @param subject Subject
      *            //@param text Text
      *            //@throws MessagingException
      */
    def send(obj: List[AppConfiguration]): Unit = {
        val from = "noreply_etl@ibibogroup.com"
        logger.info("Mail from: {}", from)
        var subject = "Mysql to Redshift Load info"
        var text = "<html><body><table border='1' style='width:100%' bgcolor='#F5F5F5'><tr> <th size=6>Mysql schema</th><th size=6>Mysql table_name </th><th size=6>Redshift schema</th><th size=6>Status</th><th size=6>Error</th></tr>"


        val tos: List[String] = mailParams.to.split(",").toList
        val ccs: List[String] = mailParams.cc.split(",").toList
        logger.info("Mail to: {} and cc: {}", tos, ccs)

        var errorCnt = 0
        var successCnt = 0
        for (i <- obj) {

            text += "<tr><td bgcolor='#FFE4C4'>" + i.mysqlConf.db + "</td><td bgcolor='#E0FFFF'>" + i.mysqlConf.tableName + "</td><td bgcolor='#F5F5DC'>" + i.redshiftConf.schema + "</td><td bgcolor='#E0FFFF'>" + i.status.get.isSuccessful + "</td><td bgcolor='#F0FFFF'>" + i.status.get.log + "</td></tr>"

            if (i.status.get.isSuccessful) {
                successCnt += 1
            }
            else {
                errorCnt += 1
            }
        }

        subject += " Success " + successCnt.toString + " Failed " + errorCnt.toString
        text += "</table></body></html>"
        logger.info("Subject: {}", subject)

        val message = new MimeMessage(session)
        message.setFrom(new InternetAddress(from))
        for (to <- tos)
            message.addRecipient(RecipientType.TO, new InternetAddress(to))
        for (cc <- ccs)
            message.addRecipient(RecipientType.TO, new InternetAddress(cc))
        message.setSubject(subject)
        message.setText(text)

        val mimeBdyPart = new MimeBodyPart();

        mimeBdyPart.setContent(text, "text/html; charset=utf-8")

        val multiPart = new MimeMultipart()

        logger.info("Sending message...")
        multiPart.addBodyPart(mimeBdyPart)
        message.setContent(multiPart)
        Transport.send(message)
    }
}