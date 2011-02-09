
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.examples.search;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author SCARS Lapi
 */
public class ImmutableRandomAccessGraph{
    //ObjectArrayList<String> nodelist = new ObjectArrayList<String>();
    //ObjectArrayList<Integer> wordlist= new ObjectArrayList<Integer>();
    public static void main(String args[]) throws Exception{
        CharSequence graphname = "PageTitles";
        String in = "a";
        int ans = search.full_match(graphname,in, true);
        if (ans==52)
            System.out.println("String not present");
        /*System.out.println("The given string is present and terminates at = "+ ans);
        search.prefix_match(graphname, in);*/
    }
    
}
