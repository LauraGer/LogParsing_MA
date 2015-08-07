/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package logparsing_ma;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Laura
 */
public class LogParsing_MA {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            // TODO code application logic here
            
            hadoop_parsing.openHadoop();
//
//            awin_parsing.openAwin();
            
//            cube_parsing.openCube();
            
            
            
        } catch (IOException ex) {
            Logger.getLogger(LogParsing_MA.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    static Object readFile(String volumesHDhtdocsGitHublog_stufflog_awintxt) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
   
    
}
