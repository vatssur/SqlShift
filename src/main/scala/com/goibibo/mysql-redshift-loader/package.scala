package com.goibibo

package object mysqlRedshiftLoader {
        case class DBConfiguration(database:String, db:String, schema:String, tableName:String, hostname:String, portNo:Int, userName:String,password:String)
        case class S3Config(sqoopedDataKey:String, accessKey:String, secretKey:String)
        case class DBField(fieldName:String, fieldType:String, javaType:Option[String] = None)
        case class TableDetails(validFields:Seq[DBField], invalidFields:Seq[DBField],
        				sortKeys:Seq[String], distributionKey:Option[String] )
}

