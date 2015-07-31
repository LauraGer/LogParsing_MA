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
    
                output.println("'System';TimeStamp;RangeFilterFrom;RangeFilterTo;TermFilter;Group;Sorting;Order;Values");

                while ((sCurrentLine = hadoop.readLine()) != null) {
          
                    if((sCurrentLine.contains("WQuery")) ) {

                        timestamp   =   logparsing_methods.getSubString( sCurrentLine, "2", "INFO") ;
                        
                        timestamp   =   timestamp.replace(" ", "")
                                            .replace( ",", ":");
                        
                        if(sCurrentLine.contains("[WRangeFilter") && sCurrentLine.contains("e)], [WTermFilter (Field" )) {
                            

                            range_filter    =   logparsing_methods.getSubString( sCurrentLine, "[WRangeFilter ", "])") ;
                            
                            range_filter    =   range_filter.replace( "[WRangeFilter (Field TIME_STAMP)(Version 7)(LowerTerm ", "")
                                                    .replace(  ")(IncludeLower true)(UpperTerm", ";")
                                                        .replace(  ")(IncludeUpper false)]", "")
                                                            .replace( ", [WTermFilter (Field", ";")
                                                                .replace(  ")(Version 7)(Values [", " : ")
                                                    ;
                            
                        }
                        else 
                        {	/*Wenn im RangeFIlter kein TermFilter enthalten ist*/	
                            range_filter    =   logparsing_methods.getSubString( sCurrentLine, "[WRangeFilter "	, "])") ;
                            
                            range_filter    =   range_filter.replace( "[WRangeFilter (Field TIME_STAMP)(Version 7)(LowerTerm ", "")
                                                        .replace(")(IncludeLower true)(UpperTerm", ";")
                                                            .replace( ")(IncludeUpper false)]", "");
                            
                        } 
//                    HIER IST IRGENDWO NOCH EIN PROBLEM....WEIL MANCHMAL BEIM GROUP NOCH EIN TERMFILTER VORHANDEN IST!!!!!
                   
                    term_filter =   logparsing_methods.getSubString( sCurrentLine, "[WTermFilter", ")]" ) ;
                            
                    term_filter =   term_filter.replace( ")(Version 7)(Values [", " : ")
                                        .replace( "WTermFilter (Field ", "") 
                                            .replace( "[", "")
                                                .replace( "]", " ");
                    
                    group       =   logparsing_methods.getSubString( sCurrentLine,  "(GroupFields"	, "])" );        
                            
                    group       =   group.replace( "(GroupFields [", "");
                    
                    
                    
                    sorting     =   logparsing_methods.getSubString( sCurrentLine,"[WSort " , "])" );
//                    System.out.println(sorting);
                    sorting     =   sorting.replace( "[WSort (Version 7)(", "")
                                            .replace( "DESC", ";DESC")
                                                .replace( "ASC", ";ASC").replace( ")", "").replace( "[WSort (Version 7", "");

                    values          =   logparsing_methods.getSubString(sCurrentLine, "(ValueFields"	, "])" ) ;
                    
                    values          =	values.replace( "(ValueFields [", "");
                    
// 				System.out.println(values);								
                    
//                    System.out.println("  GROUP"+term_filter);
                    input   =   "'HADOOP';"+timestamp+";"+range_filter+";"+term_filter+""+group+";"+sorting+";'"+values+"'";
                    
                    output.println(input);
                    
                    } 
                    
                }
                 
            } 
         
        catch (IOException e) 
            {
                e.printStackTrace();
       
            }    
        output.close();
        System.out.println("Hadoop done!");    
    }
}
