package it.unimi.dsi.webgraph;

/*		 
 * Copyright (C) 2008-2011 Sebastiano Vigna 
 *
 *  This program is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This program is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */


import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.objects.Object2IntOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

/** Reads a list of URLs from standard input and writes to standard output a host map
 * in {@link DataOutput} format.
 * 
 * <p><strong>Warning:</strong> this tool saves to standard output, but does some Log4J logging, too,
 * so be careful not to log to standard output.
 * 
 * @author Sebastiano Vigna
 */
public class BuildHostMap {
	private final static Logger LOGGER = Util.getLogger( BuildHostMap.class );

	public static void run( final FastBufferedReader fbr, final DataOutputStream dos, ProgressLogger pl ) throws IOException {
		final MutableString s = new MutableString();
		Object2IntOpenHashMap<MutableString> map = new Object2IntOpenHashMap<MutableString>();
		map.defaultReturnValue( -1 );
		int slash, hostIndex = -1;

		if ( pl != null ) pl.start( "Reading URLS..." );
		while( fbr.readLine( s ) != null ) {
			slash = s.indexOf( '/', 8 );
			// Fix for non-BURL URLs
			if ( slash != -1 ) s.length( slash );
			if ( ( hostIndex = map.getInt( s ) ) == -1 ) map.put( s.copy(), hostIndex = map.size() );
			dos.writeInt( hostIndex );
			if ( pl != null ) pl.lightUpdate();
		}
		
		if ( pl != null ) pl.done();
	}

	
	public static void main( String[] args ) throws IOException {
		final FastBufferedReader fbr = new FastBufferedReader( new InputStreamReader( System.in, "ISO-8859-1" ) );
		final DataOutputStream dos = new DataOutputStream( System.out );
		run( fbr, dos, new ProgressLogger( LOGGER ) );
		dos.close();
	}
}
