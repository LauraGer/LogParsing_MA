/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package logparsing_ma;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static logparsing_ma.Regex.PATTERNS;

/**
 *
 * @author Laura
 */
class hadoop_parsing {
        //(USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)
    static String[]     REGEXS      = new String[] {    "(.+) INFO .+ (USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)\\)\\(Version 7\\)\\(Values \\[(\\d+)\\].+TIME_STAMP\\)\\(Version 7\\)\\(LowerTerm (\\d+)\\)\\(IncludeLower true\\)\\(UpperTerm (\\d+).*\\(GroupFields \\[(.+)\\]\\).*\\(ValueFields \\[(.+).*\\(Sort \\[WSort \\(Version 7\\)\\((.+)([ DESC| ASC])\\)\\]\\)\\(.*"
                                                    ,   "(.+) INFO .+ (USER_ID|TIME_STAMP|WEBSITE_ID|PROG_ID|TCAT_ID)\\)\\(Version 7\\)\\(Values \\[(\\d+)\\].+TIME_STAMP\\)\\(Version 7\\)\\(LowerTerm (\\d+)\\)\\(IncludeLower true\\)\\(UpperTerm (\\d+).*\\(GroupFields \\[(.+)\\]\\).*\\(ValueFields \\[(.+).*\\(Sort \\[WSort \\(Version 7\\)\\]\\)\\(.*"};
    static Pattern[]    PATTERNS    = new Pattern[REGEXS.length];
    
    static {
        for (int i = 0; i < REGEXS.length; i++){
            PATTERNS[i] = Pattern.compile(REGEXS[i]);
        }
    }
    
    public static void openHadoop() throws IOException {

        String  timestamp      
            ,   range_filter_f
            ,   range_filter_t
            ,   term_filter    
            ,   group  
            ,   order
            ,   sorting        
            ,   values
            ,   input
            ;
            
        
        PrintWriter output     =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_hadoop.csv");
        
        PrintWriter reg_output     =   new PrintWriter("/Volumes/HD/X/LOGS/NOT_REGEX_hadoop.csv");
                
//                System.out.println(row);

         try (BufferedReader hadoop = new BufferedReader(new FileReader("/Volumes/HD/htdocs/GitHub/log_stuff/log_hadoop")))
            {
                String  sCurrentLine
                    ,   INPUT_STRING
                    ;
    
                output.println("System;TimeStamp;RangeFilterFrom;RangeFilterTo;TermFilter;Group;Sorting;Order;Values");

                while ((sCurrentLine = hadoop.readLine()) != null) {
                        
                    if((sCurrentLine.contains("WQuery")) ) {
                        
                        INPUT_STRING    =   sCurrentLine;
                        
                        for (Pattern pattern : PATTERNS) {
                        
                        Matcher matcher = pattern.matcher(INPUT_STRING);
                        
                        if (matcher.matches()){
                               
                            if(matcher.groupCount() == 9)  {
                        // Wenn pattern nicht gefunden, dann sysoutprtln (um neues regex zu bauen)
                                 timestamp       =   matcher.group(1);
                                 range_filter_f  =   matcher.group(4);
                                 range_filter_t  =   matcher.group(5);
                                 term_filter     =   matcher.group(2) +" : "+ matcher.group(3);
                                 group           =   matcher.group(6);
                                 sorting         =   matcher.group(8);
                                 order           =   matcher.group(9);
                                 values          =   matcher.group(7);

                            }
                            else{ //(matcher.groupCount() == 7){
                                timestamp       =   matcher.group(1);
                                range_filter_f  =   matcher.group(4);
                                range_filter_t  =   matcher.group(5);
                                term_filter     =   matcher.group(2) +" : "+ matcher.group(3);
                                group           =   matcher.group(6);
                                sorting         =   "";
                                order           =   "";
                                values          =   matcher.group(7);
                            }
                           input   =   "HADOOP;"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";"+order+";"+values+"";
                                 output.println(input); 
                        }
                        else{
//                            System.out.println(INPUT_STRING);
                             reg_output.println(INPUT_STRING);
                        }
                        
//                        timestamp   =   logparsing_methods.getSubString( sCurrentLine, "2", "INFO") ;
//                        
//                        timestamp   =   timestamp.replace(" ", "")
//                                            .replace( ",", ":"); // SimpleDateFormat
//                        
//                        if(sCurrentLine.contains("[WRangeFilter") && sCurrentLine.contains("e)], [WTermFilter (Field" )) {
//                            
//
//                            range_filter    =   logparsing_methods.getSubString( sCurrentLine, "[WRangeFilter ", "])") ;
//                            
//                            range_filter    =   range_filter.replace( "[WRangeFilter (Field TIME_STAMP)(Version 7)(LowerTerm ", "")
//                                                    .replace(  ")(IncludeLower true)(UpperTerm", ";")
//                                                        .replace(  ")(IncludeUpper false)]", "")
//                                                            .replace( ", [WTermFilter (Field", ";")
//                                                                .replace(  ")(Version 7)(Values [", " : ")
//                                                    ;
//                            
//                        }
//                        else 
//                        {	/*Wenn im RangeFIlter kein TermFilter enthalten ist*/	
//                            range_filter    =   logparsing_methods.getSubString( sCurrentLine, "[WRangeFilter "	, "])") ;
//                            
//                            range_filter    =   range_filter.replace( "[WRangeFilter (Field TIME_STAMP)(Version 7)(LowerTerm ", "")
//                                                        .replace(")(IncludeLower true)(UpperTerm", ";")
//                                                            .replace( ")(IncludeUpper false)]", "");
//                            
//                        } 
////                    HIER IST IRGENDWO NOCH EIN PROBLEM....WEIL MANCHMAL BEIM GROUP NOCH EIN TERMFILTER VORHANDEN IST!!!!!
//                   
//                    term_filter =   logparsing_methods.getSubString( sCurrentLine, "[WTermFilter", ")]" ) ;
//                            
//                    term_filter =   term_filter.replace( ")(Version 7)(Values [", " : ")
//                                        .replace( "WTermFilter (Field ", "") 
//                                            .replace( "[", "")
//                                                .replace( "]", " ");
//                    
//                    group       =   logparsing_methods.getSubString( sCurrentLine,  "(GroupFields"	, "])" );        
//                            
//                    group       =   group.replace( "(GroupFields [", "");
//                    
//                    
//                    
//                    sorting     =   logparsing_methods.getSubString( sCurrentLine,"[WSort " , "])" );
////                    System.out.println(sorting);
//                    sorting     =   sorting.replace( "[WSort (Version 7)(", "")
//                                            .replace( "DESC", ";DESC")
//                                                .replace( "ASC", ";ASC").replace( ")", "").replace( "[WSort (Version 7", "");
//
//                    values          =   logparsing_methods.getSubString(sCurrentLine, "(ValueFields"	, "])" ) ;
//                    
//                    values          =	values.replace( "(ValueFields [", "");
//                    
//// 				System.out.println(values);								
//                    
////                    System.out.println("  GROUP"+term_filter);
//                    input   =   "'HADOOP';"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";"+order+";'"+values+"'";
//                    
//                    output.println(input);
                        }
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
