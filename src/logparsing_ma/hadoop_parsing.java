/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package logparsing_ma;

import java.io.*;

/**
 *
 * @author Laura
 */
class hadoop_parsing {
        
    public static void openHadoop() throws IOException {

        String  timestamp      
            ,   range_filter
            ,   term_filter    
            ,   group          
            ,   sorting        
            ,   values
            ,   input
            ;
            
        
        PrintWriter output     =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_hadoop.csv");
                
//                System.out.println(row);

         try (BufferedReader hadoop = new BufferedReader(new FileReader("/Volumes/HD/htdocs/GitHub/log_stuff/log_hadoop")))
            {
                String sCurrentLine;

                while ((sCurrentLine = hadoop.readLine()) != null) {
          
                    if((sCurrentLine.contains("WQuery")) ) {
                        
                        
                        if(sCurrentLine.contains("[WRangeFilter") && sCurrentLine.contains("e)], [WTermFilter (Field" )) {
                            
                            
                                                    ;
                            range_filter    =   range_filter+";";
                            
                        }
                        else 
                        {	/*Wenn im RangeFIlter kein TermFilter enthalten ist*/	
                            
                                                            .replace( ")(IncludeUpper false)]", "");
                            
                        } 
                    
                            
                                            .replace( "[", "")
                                                .replace( "]", " ");
                    
                            
                    
                    
                    values          =   logparsing_methods.getSubString(sCurrentLine, "(ValueFields"	, "])" ) ;
                    
                    values          =	values.replace( "(ValueFields [", "");
 												
                    
//                    System.out.println("'HADOOP';"+timestamp+";"+range_filter+";"+term_filter+";"+group+";"+sorting+";'"+values+"'");
                    input   =   "'HADOOP';"+timestamp+";"+range_filter+";"+term_filter+";"+group+";"+sorting+";'"+values+"'";
                    
                    output.println(input);
                    
                    } 
                    
                }
                 
            } 
         
        catch (IOException e) 
            {
                e.printStackTrace();
       
            }
                
        output.close();
    }
}
