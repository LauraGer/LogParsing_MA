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
                        
                        
                                                .replace( ",", ":");
                        
                        if(sCurrentLine.contains("[WRangeFilter") && sCurrentLine.contains("e)], [WTermFilter (Field" )) {
                            
                            
                                                        .replace(  ")(IncludeLower true)(UpperTerm", ";")
                                                                .replace(  ")(IncludeUpper false)]", "")
                                                                        .replace( ", [WTermFilter (Field", ";")
                                                                                .replace(  ")(Version 7)(Values [", " : ")
                                                    ;
                            range_filter    =   range_filter+";";
                            
                        }
                        else 
                        {	/*Wenn im RangeFIlter kein TermFilter enthalten ist*/	
                            
                                                            .replace( ")(IncludeUpper false)]", "");
                            
                        } 
                    
                    
                            
                                            .replace( "[", "")
                                                .replace( "]", " ");
                    
                            
                    
                    
//                   System.out.println(sorting);
                     
                    
 												
                    
                    
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
