/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package logparsing_ma;

import java.io.*;
import java.util.Arrays;
/**
 *
 * @author Laura
 */
class awin_parsing {
    
       public static void openAwin() throws IOException {
            
           PrintWriter output     =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_awin.csv");
           
                String  replacedTxt  =   logparsing_methods.readFile("/Volumes/HD/htdocs/GitHub/log_stuff/log_awin.txt")
                                            .replaceAll(" ", "")
                                                .replaceAll("[\\n\\r]", "")
                                                    .replaceAll("\"", "'")
                                                        .replaceAll("',", "','")
                                                            .replaceAll(",'", "','")
                                                                .replaceAll("54\\d{5}|[0-9]{7}", "\r$0'");

                logparsing_methods.writeFile("/Volumes/HD/X/LOGS/awin_edit.csv", replacedTxt);
                

        try (BufferedReader awin = new BufferedReader(new FileReader("/Volumes/HD/X/LOGS/awin_edit.csv")))
        {
                String sCurrentLine;
                    
                while ((sCurrentLine = awin.readLine()) != null) {
 
                    String[] parts          =   sCurrentLine.split("','");
                    
                    String  timestamp       =   parts[2]
                        ,   range_filter_f  =   parts[3]
                        ,   range_filter_t  =   parts[4]
                        ,   term_filter     =   parts[6].replaceAll("(affiliate.)([0-9]{1,9})","$2")
                        ,   group           =   parts[7]
                        ,   sorting         =   parts[5].replaceAll("DESC", "';'DESC").replaceAll("ASC", "';'ASC")
                        ,   values          =   parts[9]
                        ,   input
                        ;
                    
                    int     value_index     =   values.indexOf("FROM")+1;
                    
                    values      =   values.substring(0,value_index);
                        
                    input       =   ("'AWIN';"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";'"+values+"'");
                    
                    output.println(input);
//                    System.out.println(input);
//                    System.out.println(Arrays.toString(parts));
                }

        } catch (IOException e) {
        } 
        output.close();
    }
}
