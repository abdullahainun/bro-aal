package com.aal.spark.utils

/**
  * Created by aal on 1/1/19.
  * rumus - rumus feature extraction pada conn log bro - ids
  */
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

object BroConnFeatureExtractionFormula{
    val px = udf((origPkts: Int, respPkts: Int) => {
        var result = 0
        result = origPkts + respPkts 

        result
    })

    // rumus nnp
    val nnp = udf((px: Int) => {
        var result = 0
        if( px == 0 ){
          result =  1;
        }else{
          result =  0;
        }

        result
    })

    // rumus nsp
    val nsp = udf((px: Int) => {
        var result = 0

        if(px >= 63 && px <= 400 ){
            result = 1;
        }else{
            result = 0;
        }
        result
    })

    // // rumus psp
    val psp = udf((nsp:Double, px: Double) => {
        var result = 0.0
        if(px == 0.0){
            result = 0.0
        }else{
            result = nsp / px;
        }
        result
    })

    // rumus iopr
    val iopr = udf((origPkts:Int, respPkts:Int) => {
        var result = 0.0
        if(respPkts != 0){
            result = origPkts / respPkts;
        }else{
            result = 0.0
        }
        result
    })

    // rumus reconnect
    val reconnect = udf((history:String) => {
        var result = 0
        // rumus reconnect
        var temp = history.take(2)
        if (temp == "Sr"){
            result = 1
        }else{
            result = 0
        }
        result
    })

    // rumus fps
    val fps = udf((origIpBytes:Int, origPkts:Int) => {
        var result = 0
        if(origPkts !=0 ){
          result = origIpBytes / origPkts                    
        }else{
          result = 0
        }

        result
    })

    // rumus tbt
    val tbt = udf((origIpBytes:Int, respIpBytes:Int) => origIpBytes + respIpBytes )

    val apl = udf((px:Int, origIpBytes:Int, respIpBytes:Int) => {
        var result = 0
        if(px == 0){
            result = 0             
        }else{
            result = (origIpBytes + respIpBytes )/px
        }
        result
    })
    // val dpl = udf(() => {
    //     var result = 0

    // })
    // val pv =  udf(() => {
    //     var result = 0
    // }
    // val bs = udf(() => {
    //     var result = 0

    // })
    // val ps = udf(() => {
    //     var result = 0

    // })
    // val ait = udf(() => {
    //     var result = 0

    // })
    // val pps = udf((duration:Double, origPktsd:Int, respPkts:Int) => {
    //     var result = 0.0
    //     var temp = 0.0
    //     if(px != 0){            
    //         result = ((origPkts + respPkts) / duration).asInstanceOf[Double]
    //     }else{
    //         result = 0.0
    //     }
    //     result
    // })

    val pps = udf((duration:Double) => {
        var result = duration
        result
    })
}