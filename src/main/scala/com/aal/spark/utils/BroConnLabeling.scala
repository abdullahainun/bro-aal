package com.aal.spark.utils

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import scala.collection.mutable.HashMap

object BroConnLabeling{
    val ip_normal_app_host: HashMap[String, String] = HashMap(
        ("192.168.50.19", "dropbox" ),
        ("192.168.50.50", "avast"),
        ("192.168.50.51", "adobe_reader"),
        ("192.168.50.52", "Adobe_Software_Suite"),
        ("192.168.50.54", "Chrome"),
        ("192.168.50.55", "Firefox"),
        ("192.168.50.56", "Malwarebyte"),
        ("192.168.50.57", "WPS_office"),
        ("192.168.50.58", "Windows_update"),
        ("192.168.50.59", "bittorent"),
        ("192.168.50.60", "audacity"),
        ("192.168.50.61", "Bytefence"),
        ("192.168.50.63", "Thunderbird"),
        ("192.168.50.64", "avast"),
        ("192.168.50.65", "Skype"),
        ("192.168.50.66", "Facebook_massager"),
        ("192.168.50.67", "CCleaner"),
        ("192.168.50.68", "Windows_update"),
        ("192.168.50.69", "Hitmanpro")
    )

    val ip_botnet_app_host: HashMap[String, String] = HashMap(
        ("192.168.50.14", "zyklon"),
        ("192.168.50.15", "blue"),
        ("192.168.50.16", "liphyra"),
        ("192.168.50.17", "gaudox"),
        ("192.168.50.18", "blackout"),
        ("192.168.50.30", "citadel"),
        ("192.168.50.31", "citadel"),
        ("192.168.50.32", "black_energy"),
        ("192.168.50.34", "zeus")
    )

    val labeling = udf((ip_src: String) => {
        var label = ""
        if((ip_botnet_app_host contains ip_src) == true){
             label  = "malicious"
        }else if((ip_normal_app_host contains ip_src) == true){
            label  = "normal"
        }else{
            label  = "normal"
        }

        label
    })  
}
