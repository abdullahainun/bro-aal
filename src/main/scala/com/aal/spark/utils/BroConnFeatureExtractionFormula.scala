package com.aal.spark.utils

/**
  * Created by aal on 1/1/19.
  * rumus - rumus feature extraction pada conn log bro - ids
  */
import org.apache.spark.sql.functions.udf

object BroConnFeatureExtractionFormula{
    val px = udf((origPkts: Integer, respPkts: Integer) => {
        var result : Integer = 0
        result = origPkts + respPkts 

        result
    })
    
    // rumus nnp
    val nnp = udf((px: Integer) => {
        var result = 0
        if( px == 0 ){
          result =  1;
        }else{
          result =  0;
        }

        result
    })

    // rumus nsp
    val nsp = udf((px: Integer) => {
        var result = 0

        if(px >= 63 && px <= 400 ){
            result = 1;
        }else{
            result = 0;
        }
        result
    })

    // rumus psp
    val psp = udf((nsp:Integer, px: Integer) => {
        var result = px

        if(!(px == 0)){
            result = nsp/px
        }
    })

    // rumus iopr
    val iopr = udf((origPkts:Integer, respPkts:Integer) => {
        var result = 0
        if(respPkts != 0){
            result = origPkts / respPkts;
        }else{
            result = 0
        }
        result
    })

    // rumus reconnect
    val reconnect = udf((history:String) => {
        var result = 0
        // rumus reconnect
        var temp = history  contains "Sr" 
        if (temp == true){
            result = 1
        }else{
            result = 0
        }
        result
    })

    // rumus fps
    val fps = udf((origIpBytes:Integer, origPkts:Integer) => {
        var result = 0
        if(origPkts !=0 ){
          result = origIpBytes / origPkts                    
        }else{
          result = 0
        }

        result
    })

    // rumus tbt
    val tbt = udf((origIpBytes:Integer, respIpBytes:Integer) => origIpBytes + respIpBytes )

    val apl = udf((px:Integer, origIpBytes:Integer, respIpBytes:Integer) => {
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
    val pps = udf((duration:Double, origPkts:Integer, respPkts:Integer) => {
        var result = 0.0
        if(px != 0){
            result = (origPkts + respPkts ) / duration
        }else{
            result = 0.0  
        }
        result
    })
}