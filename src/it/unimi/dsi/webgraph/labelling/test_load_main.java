/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unimi.dsi.webgraph.labelling;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.ImmutableGraph.LoadMethod;
/**
 *
 * @author SCARS Lapi
 */
public class test_load_main {
   // public enum E {STANDARD};
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
        final ProgressLogger pl = new ProgressLogger();
        CharSequence basename = "grdb.txt";
 //       LoadMethod loadmethod = ImmutableGraph.LoadMethod.valueOf("STANDARD");
        try{
            BitStreamArcLabelledImmutableGraph my_graph = BitStreamArcLabelledImmutableGraph.load(LoadMethod.STANDARD, basename ,pl);
            System.out.println("No exception till here");
        }
        catch(Exception e){
            System.out.println("still some problem");
        }
    }
    
    /*public class LoadingBSALIM extends BitStreamArcLabelledImmutableGraph{
    
    }*/
}