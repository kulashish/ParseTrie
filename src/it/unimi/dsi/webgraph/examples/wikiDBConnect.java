package it.unimi.dsi.webgraph.examples;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;


public class wikiDBConnect {

	private static int count=0;
	private static int MAX_LIMIT=1000000;

	static String trim(String title)
	{
		String result=title.replace("(", "").replace(")", "").replace("\"", "").replace("\'", "").replace("_"," ");
		return result;
	}

	public static void main(String args[]) throws IOException
	{
		String dbUrl = "jdbc:mysql://localhost:3306/wikipedia";
		String dbClass = "com.mysql.jdbc.Driver";
		String path="/home/ambha/Arun_workspace/PrefixTree";

		try {
			Class.forName("com.mysql.jdbc.Driver");
			Connection con = DriverManager.getConnection (dbUrl,"root","aneedo");

			String query =null;

			PreparedStatement pst;
			ResultSet rs;

			BufferedWriter bw = new BufferedWriter(new FileWriter(new File("Database_redirects.txt"),true));
			BufferedWriter bwLog=new BufferedWriter(new FileWriter(new File("DBLog.txt"),true));

			//ParsedPage parsedPage;


			/*while(true)
			{
				query="SELECT pageId,name FROM Page where pageId>=? and pageId<=?";
				pst = con.prepareStatement(query);
				pst.setInt(1, count);
				pst.setInt(2, count+MAX_LIMIT);
				rs= pst.executeQuery();
				int size=0;
				while(rs.next())
					size++;
				if(size==0)break;
				bwLog.write("started writing from "+count+" to "+(count+MAX_LIMIT)+"\n");
				System.out.println("started writing from "+count+" to "+(count+MAX_LIMIT)+"\n");
				rs.first();
				while (rs.next()) {
					//String title=rs.getString(2);
					//parsedPage=JwplMediaWikiParserFactory.getInstance().getParser().parse(title);
					bw.write(trim(rs.getString(2))+"\t"+rs.getString(1)+"\n");
				}
				count=count+MAX_LIMIT;
			}*/

			//bwLog.write("....done with pageIDs.Started with page redirects....\n");
			while(true)
			{
				query="SELECT pageId,redirects from Page natural join page_redirects where pageId>=? and pageId<=?";
				pst = con.prepareStatement(query);
				pst.setInt(1, count);
				pst.setInt(2, count+MAX_LIMIT);
				rs= pst.executeQuery();
				int size=0;
				while(rs.next())
					size++;
				if(size==0)break;
				bwLog.write("started writing from "+count+" to "+(count+MAX_LIMIT)+"\n");
				System.out.println("started writing from "+count+" to "+(count+MAX_LIMIT));
				rs.first();
				while (rs.next()) {
					//String title=rs.getString(2);
					//parsedPage=JwplMediaWikiParserFactory.getInstance().getParser().parse(title);
					bw.write(trim(rs.getString(2))+"\t"+rs.getString(1)+"\n");
				}
				count=count+MAX_LIMIT;
			}
			con.close();
			bw.close();
		} //end try

		catch(ClassNotFoundException e) {
			e.printStackTrace();
		}

		catch(SQLException e) {
			e.printStackTrace();
		}

	}
}
