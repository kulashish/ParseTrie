package it.unimi.dsi.webgraph;

/*		 
 * Copyright (C) 2011 Sebastiano Vigna 
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

import static it.unimi.dsi.fastutil.HashCommon.bigArraySize;
import static it.unimi.dsi.fastutil.HashCommon.maxFill;
import it.unimi.dsi.Util;
import it.unimi.dsi.fastutil.BigArrays;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.booleans.BooleanBigArrays;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.longs.LongBigArrays;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.logging.ProgressLogger;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;


/** An {@link ImmutableGraph} that corresponds to a graph stored as a scattered list of arcs.
 * 
 * <p>A <em>scattered list of arcs</em> describes a graph in a fairly loose way. Each line
 * contains an arc specified as two node identifiers separated by whitespace 
 * (but we suggest exactly one TAB character). Sources and targets can be in any order. Node
 * identifiers can be in the range [-2<sup>63</sup>..2<sup>63</sup>): they will be remapped
 * in a compact identifier space by assigning to each newly seen identifier a new node number. The
 * list of identifiers in order of appearance is available in {@link #ids}.
 * Lines can be empty, or comments starting with <samp>#</samp>. Characters following the
 * target will be discarded with a warning.
 * 
 * <p><strong>Warning:</strong> Lines not conforming the above specification
 * will cause an error to be logged, but will be otherwise ignored.
 * 
 * <p>Additionally, the resulting graph can be symmetrized, and its loops be removed, using
 * {@linkplain #ScatteredArcsASCIIGraph(InputStream, boolean, boolean, int, File, ProgressLogger) suitable constructor options}.
 *  
 *  <P>This class has no load method, and its main method converts an scattered-arcs representation
 *  directly into a {@link BVGraph}.
 *  
 *  <h2>Using {@link ScatteredArcsASCIIGraph} to convert your data</h2>
 *  
 *  <p>A simple (albeit rather inefficient) way to import data into WebGraph is using ASCII graphs specified by scattered arcs. Suppose you
 *  create the following file, named <samp>example.arcs</samp>:
 *  <pre>
 *  # My graph
 *  -1 15
 *  15 2
 *  2 -1 This will cause a warning to be logged
 *  OOPS! (This will cause an error to be logged)
 *  -1 2
 *  </pre>
 *  Then, the command 
 *  <pre>
 *  java it.unimi.dsi.webgraph.ScatteredArcsASCIIGraph example &lt;example.arcs
 *  </pre>
 *  will produce a compressed graph in {@link it.unimi.dsi.webgraph.BVGraph} format
 *  with basename <samp>example</samp>. The file <samp>example.ids</samp> will contain
 *  the list of longs -1, 15, 2. The node with identifer -1 will be the node 0 in the
 *  output graph, the node with identifier 15 will be node 1, and the node with identifier 2 will be node 2.
 *  The graph <samp>example</samp> will thus have three nodes and four arcs (viz., &lt;0,1>, &lt;0,2>, &lt;1,2> and &lt;2,0>). 
 *  
 *  <h2>Memory requirements</h2>
 *  
 *  <p>To convert node identifiers to node numbers, instances of this class use a custom map that in the
 *  worst case will require 19.5&times;2<sup><big>&lceil;</big>log(4<var>n</var>/3)<big>&rceil;</big></sup>&nbsp;&le;&nbsp;52<var>n</var> bytes,
 *  where <var>n</var> is the number of distinct identifiers.
 */


public class ScatteredArcsASCIIGraph extends ImmutableSequentialGraph {
	private static final Logger LOGGER = Util.getLogger( ScatteredArcsASCIIGraph.class );
	private final static boolean DEBUG = false;

	/** The default batch size. */
	public static final int DEFAULT_BATCH_SIZE = 100000;
	/** The extension of the identifier file (a binary list of longs). */
	private static final String IDS_EXTENSION = ".ids";
	/** The batch graph used to return node iterators. */
	private Transform.BatchGraph batchGraph;
	/** The list of identifiers in order of appearance. */
	public long[] ids;

	private static final class Long2IntOpenHashBigMap implements java.io.Serializable, Cloneable, Hash {
		public static final long serialVersionUID = 0L;

		/** The big array of keys. */
		public transient long[][] key;

		/** The big array of values. */
		public transient int[][] value;

		/** The big array telling whether a position is used. */
		protected transient boolean[][] used;

		/** The acceptable load factor. */
		protected final float f;

		/** The current table size (always a power of 2). */
		protected transient long n;

		/** Threshold after which we rehash. It must be the table size times {@link #f}. */
		protected transient long maxFill;

		/** The mask for wrapping a position counter. */
		protected transient long mask;

		/** The mask for wrapping a segment counter. */
		protected transient int segmentMask;

		/** The mask for wrapping a base counter. */
		protected transient int baseMask;

		/** Number of entries in the set. */
		protected long size;

		/** Initialises the mask values. */
		private void initMasks() {
			mask = n - 1;
			/*
			 * Note that either we have more than one segment, and in this case all segments are
			 * BigArrays.SEGMENT_SIZE long, or we have exactly one segment whose length is a power of
			 * two.
			 */
			segmentMask = key[ 0 ].length - 1;
			baseMask = key.length - 1;
		}

		/**
		 * Creates a new hash big set.
		 * 
		 * <p>The actual table size will be the least power of two greater than
		 * <code>expected</code>/<code>f</code>.
		 * 
		 * @param expected the expected number of elements in the set.
		 * @param f the load factor.
		 */
		public Long2IntOpenHashBigMap( final long expected, final float f ) {
			if ( f <= 0 || f > 1 ) throw new IllegalArgumentException( "Load factor must be greater than 0 and smaller than or equal to 1" );
			if ( n < 0 ) throw new IllegalArgumentException( "The expected number of elements must be nonnegative" );
			this.f = f;
			n = bigArraySize( expected, f );
			maxFill = maxFill( n, f );
			key = LongBigArrays.newBigArray( n );
			value = IntBigArrays.newBigArray( n );
			used = BooleanBigArrays.newBigArray( n );
			initMasks();
		}

		/**
		 * Creates a new hash big set with initial expected {@link Hash#DEFAULT_INITIAL_SIZE} elements
		 * and {@link Hash#DEFAULT_LOAD_FACTOR} as load factor.
		 */

		public Long2IntOpenHashBigMap() {
			this( DEFAULT_INITIAL_SIZE, DEFAULT_LOAD_FACTOR );
		}

		public int put( final long k, final int v ) {
			final long h = it.unimi.dsi.fastutil.HashCommon.murmurHash3( k );

			// The starting point.
			int displ = (int)( h & segmentMask );
			int base = (int)( ( h & mask ) >>> BigArrays.SEGMENT_SHIFT );

			// There's always an unused entry.
			while ( used[ base ][ displ ] ) {
				if ( k == key[ base ][ displ ] ) {
					final int oldValue = value[ base ][ displ ];
					value[ base ][ displ ] = v;
					return oldValue;
				}
				base = ( base + ( ( displ = ( displ + 1 ) & segmentMask ) == 0 ? 1 : 0 ) ) & baseMask;
			}

			used[ base ][ displ ] = true;
			key[ base ][ displ ] = k;
			value[ base ][ displ ] = v;

			if ( ++size >= maxFill ) rehash( 2 * n );
			return -1;
		}

		public int get( final long k ) {
			final long h = it.unimi.dsi.fastutil.HashCommon.murmurHash3( k );

			// The starting point.
			int displ = (int)( h & segmentMask );
			int base = (int)( ( h & mask ) >>> BigArrays.SEGMENT_SHIFT );

			// There's always an unused entry.
			while ( used[ base ][ displ ] ) {
				if ( k == key[ base ][ displ ] ) return value[ base ][ displ ];
				base = ( base + ( ( displ = ( displ + 1 ) & segmentMask ) == 0 ? 1 : 0 ) ) & baseMask;
			}

			return -1;
		}

		protected void rehash( final long newN ) {
			final boolean used[][] = this.used;
			final long key[][] = this.key;
			final int[][] value = this.value;
			final boolean newUsed[][] = BooleanBigArrays.newBigArray( newN );
			final long newKey[][] = LongBigArrays.newBigArray( newN );
			final int newValue[][] = IntBigArrays.newBigArray( newN );
			final long newMask = newN - 1;
			final int newSegmentMask = newKey[ 0 ].length - 1;
			final int newBaseMask = newKey.length - 1;

			int base = 0, displ = 0;
			long h;
			long k;

			for ( long i = size; i-- != 0; ) {

				while ( !used[ base ][ displ ] )
					base = ( base + ( ( displ = ( displ + 1 ) & segmentMask ) == 0 ? 1 : 0 ) );

				k = key[ base ][ displ ];
				h = it.unimi.dsi.fastutil.HashCommon.murmurHash3( k );

				// The starting point.
				int d = (int)( h & newSegmentMask );
				int b = (int)( ( h & newMask ) >>> BigArrays.SEGMENT_SHIFT );

				while ( newUsed[ b ][ d ] )
					b = ( b + ( ( d = ( d + 1 ) & newSegmentMask ) == 0 ? 1 : 0 ) ) & newBaseMask;

				newUsed[ b ][ d ] = true;
				newKey[ b ][ d ] = k;
				newValue[ b ][ d ] = value[ base ][ displ ];

				base = ( base + ( ( displ = ( displ + 1 ) & segmentMask ) == 0 ? 1 : 0 ) );
			}

			this.n = newN;
			this.key = newKey;
			this.value = newValue;
			this.used = newUsed;
			initMasks();
			maxFill = maxFill( n, f );
		}

		public void compact() {
			int base = 0, displ = 0, b = 0, d = 0;
			for( long i = size; i-- != 0; ) {
				while ( ! used[ base ][ displ ] ) base = ( base + ( ( displ = ( displ + 1 ) & segmentMask ) == 0 ? 1 : 0 ) ) & baseMask;
				key[ b ][ d ] = key[ base ][ displ ];
				value[ b ][ d ] = value[ base ][ displ ];
				base = ( base + ( ( displ = ( displ + 1 ) & segmentMask ) == 0 ? 1 : 0 ) ) & baseMask;
				b = ( b + ( ( d = ( d + 1 ) & segmentMask ) == 0 ? 1 : 0 ) ) & baseMask;
			}
		}
		
		public long size() {
			return size;
		}
	}

	
	/** Creates a scattered-arcs ASCII graph.
	 * 
	 * @param is an input stream containing a scattered list of arcs.
	 */
	public ScatteredArcsASCIIGraph( final InputStream is ) throws IOException {
		this( is, false );
	}
	
	/** Creates a scattered-arcs ASCII graph.
	 * 
	 * @param is an input stream containing a scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 */
	public ScatteredArcsASCIIGraph( final InputStream is, final boolean symmetrize ) throws IOException {
		this( is, symmetrize, false );
	}
	
	/** Creates a scattered-arcs ASCII graph.
	 * 
	 * @param is an input stream containing a scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 */
	public ScatteredArcsASCIIGraph( final InputStream is, final boolean symmetrize, final boolean noLoops ) throws IOException {
		this( is, symmetrize, noLoops, DEFAULT_BATCH_SIZE );
	}
	
	/** Creates a scattered-arcs ASCII graph.
	 * 
	 * @param is an input stream containing a scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 */
	public ScatteredArcsASCIIGraph( final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize ) throws IOException {
		this( is, symmetrize, noLoops, batchSize, null );
	}
	
	/** Creates a scattered-arcs ASCII graph.
	 * 
	 * @param is an input stream containing a scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 */
	public ScatteredArcsASCIIGraph( final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir ) throws IOException {
		this( is, symmetrize, noLoops, batchSize, tempDir, null );
	}

	/** Creates a scattered-arcs ASCII graph.
	 * 
	 * @param is an input stream containing a scattered list of arcs.
	 * @param symmetrize the new graph will be forced to be symmetric.
	 * @param noLoops the new graph will have no loops.
	 * @param batchSize the number of integers in a batch; two arrays of integers of this size will be allocated by this method.
	 * @param tempDir a temporary directory for the batches, or <code>null</code> for {@link File#createTempFile(java.lang.String, java.lang.String)}'s choice.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public ScatteredArcsASCIIGraph( final InputStream is, final boolean symmetrize, final boolean noLoops, final int batchSize, final File tempDir, final ProgressLogger pl ) throws IOException {
		final FastBufferedInputStream fbis = new FastBufferedInputStream( is );
		Long2IntOpenHashBigMap map = new Long2IntOpenHashBigMap();

		int numNodes = -1;
		final Charset iso88591 = Charset.forName( "ISO-8859-1" );

		int j;
		int[] source = new int[ batchSize ] , target = new int[ batchSize ];
		ObjectArrayList<File> batches = new ObjectArrayList<File>();

		if ( pl != null ) {
			pl.itemsName = "arcs";
			pl.start( "Creating sorted batches..." );
		}

		j = 0;
		long pairs = 0; // Number of pairs
		byte[] array = new byte[ 1024 ];
		for( int line = 1; ; line++ ) {
			 int start = 0, len;
			 while( ( len = fbis.readLine( array, start, array.length - start, FastBufferedInputStream.ALL_TERMINATORS ) ) == array.length - start ) {
			     start += len;
			     array = ByteArrays.grow( array, array.length + 1 );
			 };
			 
			 if ( len == -1 ) break; // EOF

			 final int lineLength = start + len;
			
			if ( DEBUG ) System.err.println( "Reading line " + line + "... (" + new String( array, 0, lineLength, iso88591 ) + ")" );

			// Skip whitespace at the start of the line.
			int offset = 0;
			while( offset < lineLength && array[ offset ] >= 0 && array[ offset ] <= ' ' ) offset++;

			if ( offset == lineLength ) {
				if ( DEBUG ) System.err.println( "Skipping line " + line + "..." );
				continue; // Whitespace line
			}

			if ( array[ 0 ] == '#' ) continue;

			// Scan source id.
			start = offset;
			while( offset < lineLength && ( array[ offset ] < 0 || array[ offset ] > ' ' ) ) offset++;

			final long sl;
			try {
				sl = getLong( array, start, offset - start );
			}
			catch( RuntimeException e ) {
				// Discard up to the end of line
				LOGGER.error( "Error at line " + line + ": " + e.getMessage() );
				continue;
			}
			
			int s = map.get( sl );
			if ( s == -1 ) map.put( sl, s = (int)map.size() );

			if ( DEBUG ) System.err.println( "Parsed source at line " + line + ": " + sl + " => " + s );
			
			// Skip whitespace between identifiers.
			while( offset < lineLength && array[ offset ] >= 0 && array[ offset ] <= ' ' ) offset++;

			if ( offset == lineLength ) {
				LOGGER.error( "Error at line " + line + ": no target" );
				continue;
			}

			// Scan target id.
			start = offset;
			while( offset < lineLength && ( array[ offset ] < 0 || array[ offset ] > ' ' ) ) offset++;

			final long tl;
			try {
				tl = getLong( array, start, offset - start );
			}
			catch( RuntimeException e ) {
				// Discard up to the end of line
				LOGGER.error( "Error at line " + line + ": " + e.getMessage() );
				continue;
			}
			
			int t = map.get( tl );
			if ( t == -1 ) map.put( tl, t = (int)map.size() );

			// Skip whitespace after target.
			while( offset < lineLength && array[ offset ] >= 0 && array[ offset ] <= ' ' ) offset++;

			if ( offset < lineLength ) LOGGER.warn( "Trailing characters ignored at line " + line );
			
			if ( DEBUG ) {
				System.err.println( "Parsed target at line " + line + ": " + tl + " => " + t );
				System.err.println( "Parsed arc at line " + line + ": " + s + " -> " + t );
			}

			if ( s != t || ! noLoops ) { 
				source[ j ] = s;
				target[ j++ ] = t;

				if ( j == batchSize ) {
					pairs += Transform.processBatch( batchSize, source, target, tempDir, batches );
					j = 0;
				}

				if ( symmetrize && s != t ) {
					source[ j ] = t;
					target[ j++ ] = s;
					if ( j == batchSize ) {
						pairs += Transform.processBatch( batchSize, source, target, tempDir, batches );
						j = 0;
					}
				}
				
				if ( pl != null ) pl.lightUpdate();
			}
		}

		if ( j != 0 ) pairs += Transform.processBatch( j, source, target, tempDir, batches );

		if ( pl != null ) {
			pl.done();
			Transform.logBatches( batches, pairs, pl );
		}
		
		numNodes = (int)map.size();
		source = null;
		target = null;

		map.compact();
		
		final File keyFile = File.createTempFile( ScatteredArcsASCIIGraph.class.getSimpleName(), "keys", tempDir );
		keyFile.deleteOnExit();
		final File valueFile = File.createTempFile( ScatteredArcsASCIIGraph.class.getSimpleName(), "values", tempDir );
		valueFile.deleteOnExit();

		BinIO.storeLongs( map.key, 0, map.size(), keyFile );
		BinIO.storeInts( map.value, 0, map.size(), valueFile );
		
		map = null;
		
		long[][] key = BinIO.loadLongsBig( keyFile );
		keyFile.delete();
		int[][] value = BinIO.loadIntsBig( valueFile );
		valueFile.delete();

		ids = new long[ numNodes ];
		
		final long[] result = new long[ numNodes ];
		for( int i = numNodes; i--!= 0; ) result[ IntBigArrays.get( value, i ) ] = LongBigArrays.get( key, i );
		ids = result;
		
		key = null;
		value = null;

		batchGraph = new Transform.BatchGraph( numNodes, pairs, batches );
	}

	private final static long getLong( final byte[] array, int offset, int length ) {
		if ( length == 0 ) throw new NumberFormatException( "Empty number" );
		int sign = 1;
		if( array[ offset ] == '-' ) {
			sign = -1;
			offset++;
			length--;
		}
		
		long value = 0;
		for( int i = 0; i < length; i++ ) {
			final byte digit = array[ offset + i ];
			if ( digit < '0' || digit > '9' ) throw new NumberFormatException( "Not a digit: " + (char)digit );
			value *= 10;
			value += digit - '0';
		}
		
		return sign * value;
	}
	
	public int numNodes() {
		if ( batchGraph == null ) throw new UnsupportedOperationException( "The number of nodes is unknown (you need to exhaust the input)" );
		return batchGraph.numNodes();
	}
	
	public long numArcs() {
		if ( batchGraph == null ) throw new UnsupportedOperationException( "The number of arcs is unknown (you need to exhaust the input)" );
		return batchGraph.numArcs();
	}
	
	@Override
	public NodeIterator nodeIterator( final int from ) {
		return batchGraph.nodeIterator( from );
	}

	public static void main( String args[] ) throws IllegalArgumentException, SecurityException, IOException, JSAPException  {
		String basename;
		SimpleJSAP jsap = new SimpleJSAP( ScatteredArcsASCIIGraph.class.getName(), "Converts a scattered list of arcs into a BVGraph. The list of" +
				"identifiers in order of appearance will be saved with extension \"" + IDS_EXTENSION + "\".",
				new Parameter[] {
						new FlaggedOption( "logInterval", JSAP.LONG_PARSER, Long.toString( ProgressLogger.DEFAULT_LOG_INTERVAL ), JSAP.NOT_REQUIRED, 'l', "log-interval", "The minimum time interval between activity logs in milliseconds." ),
						new FlaggedOption( "batchSize", JSAP.INTSIZE_PARSER, Integer.toString( DEFAULT_BATCH_SIZE ), JSAP.NOT_REQUIRED, 's', "batch-size", "The maximum size of a batch, in arcs." ),
						new FlaggedOption( "tempDir", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'T', "temp-dir", "A directory for all temporary batch files." ),
						new Switch( "symmetrize", 'S', "symmetrize", "Force the output graph to be symmetric." ),
						new Switch( "noLoops", 'L', "no-loops", "Remove loops from the output graph." ),
						new FlaggedOption( "comp", JSAP.STRING_PARSER, null, JSAP.NOT_REQUIRED, 'c', "comp", "A compression flag (may be specified several times)." ).setAllowMultipleDeclarations( true ),
						new FlaggedOption( "windowSize", JSAP.INTEGER_PARSER, String.valueOf( BVGraph.DEFAULT_WINDOW_SIZE ), JSAP.NOT_REQUIRED, 'w', "window-size", "Reference window size (0 to disable)." ),
						new FlaggedOption( "maxRefCount", JSAP.INTEGER_PARSER, String.valueOf( BVGraph.DEFAULT_MAX_REF_COUNT ), JSAP.NOT_REQUIRED, 'm', "max-ref-count", "Maximum number of backward references (-1 for ∞)." ),
						new FlaggedOption( "minIntervalLength", JSAP.INTEGER_PARSER, String.valueOf( BVGraph.DEFAULT_MIN_INTERVAL_LENGTH ), JSAP.NOT_REQUIRED, 'i', "min-interval-length", "Minimum length of an interval (0 to disable)." ),
						new FlaggedOption( "zetaK", JSAP.INTEGER_PARSER, String.valueOf( BVGraph.DEFAULT_ZETA_K ), JSAP.NOT_REQUIRED, 'k', "zeta-k", "The k parameter for zeta-k codes." ),
						new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the output graph" ),
					}
				);
				
		JSAPResult jsapResult = jsap.parse( args );
		if ( jsap.messagePrinted() ) System.exit( 1 );
		
		basename = jsapResult.getString( "basename" );

		int flags = 0;
		for( String compressionFlag: jsapResult.getStringArray( "comp" ) ) {
			try {
				flags |= BVGraph.class.getField( compressionFlag ).getInt( BVGraph.class );
			}
			catch ( Exception notFound ) {
				throw new JSAPException( "Compression method " + compressionFlag + " unknown." );
			}
		}
		
		final int windowSize = jsapResult.getInt( "windowSize" );
		final int zetaK = jsapResult.getInt( "zetaK" );
		int maxRefCount = jsapResult.getInt( "maxRefCount" );
		if ( maxRefCount == -1 ) maxRefCount = Integer.MAX_VALUE;
		final int minIntervalLength = jsapResult.getInt( "minIntervalLength" );
		File tempDir = null;
		if ( jsapResult.userSpecified( "tempDir" ) ) tempDir = new File( jsapResult.getString( "tempDir" ) );

		final ProgressLogger pl = new ProgressLogger( LOGGER, jsapResult.getLong( "logInterval" ) );
		ScatteredArcsASCIIGraph graph = new ScatteredArcsASCIIGraph( System.in, jsapResult.userSpecified( "symmetrize" ), jsapResult.userSpecified( "noLoops" ), jsapResult.getInt( "batchSize" ), tempDir, pl );
		BVGraph.store( graph, basename, windowSize, maxRefCount, minIntervalLength, zetaK, flags, pl );
		BinIO.storeLongs( graph.ids, basename + IDS_EXTENSION );
	}
}
