/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package it.unimi.dsi.webgraph.examples;

//import it.unimi.dsi.fastutil.ints.IntArrayFIFOQueue;
//import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.logging.ProgressLogger;
//import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
//import it.unimi.dsi.webgraph.LazyIntIterator;
//import it.unimi.dsi.webgraph.labelling.BitStreamArcLabelledImmutableGraph;
//import it.unimi.dsi.webgraph.labelling.Label;
import java.util.Stack;

//import java.io.IOException;
//import java.lang.reflect.InvocationTargetException;

/*import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;
import it.unimi.dsi.fastutil.io.FastMultiByteArrayInputStream;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.webgraph.labelling.ArcLabelledNodeIterator.LabelledArcIterator;
import java.lang.Integer;
import java.util.ArrayList;
import java.util.List;*/
/**
 *
 * @author SCARS Lapi
 */
public class search {
    
    public static int full_match(CharSequence basename, String S, boolean method) throws Exception{
                final ProgressLogger pl = new ProgressLogger();
		pl.logInterval = ProgressLogger.DEFAULT_LOG_INTERVAL;
		final ImmutableGraph graph = ImmutableGraph.load(basename, pl);

		final int maxDist = Integer.MAX_VALUE;
		final int start = 0;
                final int stop = S.length();
		final int n = graph.numNodes();
                System.out.println("The number of nodes is "+n);
                int nodenum=52;
		
		int curr = 52;
		
		pl.start( "Starting visit..." );
		pl.itemsName = "nodes";
                int nodevalue;
                int charint;
		for( int i = start; i < stop; i++ ) {			
                    int successors[];
                    successors = graph.successorArray(curr);
                    int m = graph.outdegree(curr);
                    charint = S.charAt(i);
                    if (charint>64 & charint <91)
                        charint = charint - 65;
                    else if (charint>96 & charint < 123)
                        charint = charint - 97;
                    else
                        return 52;
                    boolean match = false;
                    int searchstart;
                    if (curr==52)
                        searchstart = 0;
                    else
                        searchstart = 1;
                    for (int j=searchstart ; j<m & match==false  ; j++){
                        nodenum = successors[j];
                        int nodesuccessors[];
                        nodesuccessors = graph.successorArray(nodenum);
                        nodevalue = nodesuccessors[0];
                        if (charint == nodevalue || charint+26 ==nodevalue){
                            System.out.println("The value is "+nodevalue);
                            match = true;
                            curr = nodenum;
                        }
                    }
                    if (match == false)
                        return 52;
                    //System.out.println("\nreach 1\n");
                    pl.update();
		}
		pl.done();
                
                System.out.println("The given string exists and terminates at "+nodenum);
                
                if (method==true)
                    return nodenum;
                final Stack<Integer> stack = new Stack<Integer>();
                final Stack<Integer> numstack = new Stack<Integer>();
                int m = graph.outdegree(nodenum);
                int nodesuccessors[] = graph.successorArray(nodenum);
                numstack.push(m-2);
                /*for (int i=0;i<m;i++){
                    System.out.println("The "+i+"th outlet of 53 is" +nodesuccessors[i]);
                }*/
                for (int i=m-1; i>1 ; i--){
                    stack.push((int)nodesuccessors[i]);
                    //System.out.println("At "+i+"th iteration, the top of stack is "+stack.peek());
                }
                //System.out.println("The outdegree at 53 is "+m);
                //System.out.println("The last successor at 53 is "+stack.peek());
                curr = nodesuccessors[1];
                //int size = stack.size();
                //System.out.println("The size is "+size);
                String current_string = S;
                int current_num;
                int Stringlength = S.length();
                while(!stack.isEmpty()){
                    nodesuccessors = graph.successorArray(curr);
                    charint = nodesuccessors[0];
                    if (charint<26)
                        current_string += String.valueOf((char)(charint+97));
                    else{
                        current_string += String.valueOf((char)(charint+71));
                        System.out.println("String "+current_string+" present at node number "+curr);
                    }
                    Stringlength++;
                    //System.out.println("The length at check 1 is "+Stringlength +" and the current string is "+current_string);
                    m = graph.outdegree(curr);
                    if (m>1){
                        numstack.push(m-2);
                        for (int i=m-1;i>1;i--){
                            stack.push((int)nodesuccessors[i]);
                        }
                        curr = nodesuccessors[1];
                    }
                    else{
                        current_num = numstack.pop();
                        Stringlength--;
                        curr = stack.pop();
                        while(current_num ==0){
                            current_num = numstack.pop();
                            Stringlength--;
                        }
                        numstack.push(current_num-1);
                        //System.out.println("The string length is "+Stringlength+ " and string is "+S);
                        current_string = current_string.substring(0,Stringlength);
                    }
                    
                }
                    
                    /*if (current_num==0)
                        continue;
                    current_num--;
                    numstack.push(current_num);
                    curr = stack.peek();*/
                    //nodesuccessors = graph.successorArray(curr);
                    
                       
                return 0;
	}

    /*public static void prefix_match(CharSequence basename, String S) throws Exception{
        ObjectArrayList<String> list_strings = new ObjectArrayList<String>();
        ObjectArrayList<Integer> list_numnodes= new ObjectArrayList<Integer>();
        int term_node = full_match(basename, S);
        
        if (term_node == 52){
            System.out.println("String not found");
            return;
        }
        m = 
        while()
        list_strings.add();
        list_numnodes.add();
        
        //return list_numnodes;
    }*/

}