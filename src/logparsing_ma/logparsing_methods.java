/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package logparsing_ma;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;

/**
 *
 * @author Laura
 */
public class logparsing_methods {
    
    static String readFile(String path) throws IOException {
        
        try (FileInputStream stream = new FileInputStream(new File( path ))) {
              /* Instead of using default, pass in a decoder. */
              return Charset.defaultCharset().decode(bb).toString();
            }
       }
    
    static void writeFile(String path, String row) throws FileNotFoundException {
     
//                System.out.println(row);
                
                output.println(row);
                
                output.close();
                
                
       
    }
    
    static String getSubString(String str, String search, String end){
        
        int     first		
            ,   last
            ;
                
        String  sub
            ,   result
            ;    
//        System.out.println(str.indexOf(search));
        first           =       str.indexOf(search);
        sub		=	str.substring(first, str.length() ); //ermittelt substring um endposition rauszufinen
        
        last 		=	sub.indexOf(end);		
        result  	=	str.substring(first, first+last);			
        
        return result;
        
    }
}
