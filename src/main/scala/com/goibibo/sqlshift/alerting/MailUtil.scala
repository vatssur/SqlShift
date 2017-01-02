package com.goibibo.sqlshift.alerting

import java.util.Properties

import com.goibibo.sqlshift.models.Params.MailParams

import scala.collection.JavaConverters._

/**
  * Project: sqlshift
  * Author: shivamsharma
  * Date: 1/3/17.
  */
object MailUtil {

    def getMailParams(prop: Properties): MailParams = {
        val scalaProp = prop.asScala
        var mailParams: MailParams = MailParams(host = scalaProp("alert.host"),
            username = null,
            to = scalaProp.getOrElse("alert.to", ""),
            cc = scalaProp.getOrElse("alert.cc", ""),
            subject = scalaProp.getOrElse("alert.subject", "")
        )

        mailParams = scalaProp.get("alert.port") match {
            case Some(port) => mailParams.copy(port = port.toInt)
            case None => mailParams
        }

        mailParams = scalaProp.get("alert.username") match {
            case Some(username) => mailParams.copy(username = username)
            case None => mailParams
        }

        mailParams = scalaProp.get("alert.password") match {
            case Some(pass) => mailParams.copy(password = Some(pass))
            case None => mailParams
        }
        mailParams
    }

}
