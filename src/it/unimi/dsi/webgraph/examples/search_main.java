/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unimi.dsi.webgraph.examples;

import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.labelling.ArcLabelledImmutableGraph;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;

/**
 *
 * @author SCARS Lapi
 */
public class search_main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception{
        // TODO code application logic here
        ArcLabelledImmutableGraph[] graphlist = new_search.graphloader();
        System.out.println("Loaded the graphs");
        int graphbounds[][] = new_search.get_graph_boundaries();
        System.out.println("Loaded the graphbounds");
        //BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String str="A";
        FileInputStream fstream = new FileInputStream("Database_sorted.txt");
        DataInputStream in = new DataInputStream(fstream);
        BufferedWriter bw = new BufferedWriter(new FileWriter(new File("notfound.txt"), true));
        BufferedReader br = new BufferedReader( new InputStreamReader( in ) );
        int number_iterations=0;
        while((str= br.readLine())!=null){
            //System.out.println("Please give the word you want to search for");
            int ans = new_search.find_string(graphlist,str,graphbounds);
            if(ans ==0){
                //System.out.println("String not found");
                bw.write(str);
                bw.newLine();
                bw.flush();
                
            }
            number_iterations++;
            if(number_iterations%10000==0)
                System.out.println("No. of iterations completed "+number_iterations);
        }
        bw.close();
    }
     
    
}