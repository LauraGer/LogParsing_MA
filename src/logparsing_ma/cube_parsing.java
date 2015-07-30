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
class cube_parsing {

    static void openCube() throws IOException {
        
        PrintWriter output     =   new PrintWriter("/Volumes/HD/X/LOGS/FINAL_cube.csv");
        
        try (BufferedReader cube = new BufferedReader(new FileReader("/Volumes/HD/htdocs/GitHub/log_stuff/log_cube.csv")))
            {
                String sCurrentLine;

                while ((sCurrentLine = cube.readLine()) != null) {
                    
                    String  edit;    
                            
                            edit    =	sCurrentLine.replace( ",,,", ";\"\";")
                                            .replace( ",,,", ";\";\";")
						.replace( "\",\"", "\";\"")
                                                    .replace( ",", "\";\"");
                    
                    String[] parts          =   edit.split("\";\"");
                    
                    String  timestamp       =   parts[1]
                        ,   range_filter_f  =   parts[2]
                        ,   range_filter_t  =   parts[3]
                        ,   term_filter     =   parts[4]
                        ,   group           =   parts[5]
                        ,   sorting         =   parts[7]
                        ,   values          =   parts[8]
                        ,   input
                        ;
//                   
                    if((range_filter_f.length()==0)   ) {
                         term_filter  = "CHECK";
//                        if(term_filter.matches("[0-9]{9}"))
//                        {
//                            term_filter  = "CHECK";
//                        }
                    }
////                    
                    input       =   ("'CUBE';"+timestamp+";"+range_filter_f+";"+range_filter_t+";"+term_filter+";"+group+";"+sorting+";'"+values+"'");
//                    System.out.println(Arrays.toString(parts));
                    System.out.println(term_filter);
//                    output.println(input);
                }
            }
        catch (IOException e) 
            {
                e.printStackTrace();
       
            }
                
        output.close();
    }
    
}
