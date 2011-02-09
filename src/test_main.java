/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
//import it.unimi.dsi.*;
/**
 *
 * @author SCARS Lapi
 */
public class test_main {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        // TODO code application logic here
         if (args.length == 0) {

            System.out.println("No Command Line arguments");
   
            } else {

   System.out.println("You provided " + args.length 
    + " arguments");
        test_net tn = new test_net();
        tn.show_message();
    }
}
}