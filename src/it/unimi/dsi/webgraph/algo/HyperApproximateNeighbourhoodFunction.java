package it.unimi.dsi.webgraph.algo;

/*		 
 * Copyright (C) 2010-2011 Sebastiano Vigna 
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
import it.unimi.dsi.bits.LongArrayBitVector;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.booleans.BooleanArrays;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrays;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntSets;
import it.unimi.dsi.fastutil.io.TextIO;
import it.unimi.dsi.fastutil.longs.LongBigList;
import it.unimi.dsi.fastutil.objects.ObjectIterators;
import it.unimi.dsi.io.SafelyCloseable;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.IntHyperLogLogCounterArray;
import it.unimi.dsi.webgraph.BVGraph;
import it.unimi.dsi.webgraph.GraphClassParser;
import it.unimi.dsi.webgraph.ImmutableGraph;
import it.unimi.dsi.webgraph.LazyIntIterator;
import it.unimi.dsi.webgraph.NodeIterator;
import it.unimi.dsi.webgraph.Transform;

import java.io.File;
import java.io.IOException;
import java.io.NotSerializableException;
import java.io.ObjectOutputStream;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.Parameter;
import com.martiansoftware.jsap.SimpleJSAP;
import com.martiansoftware.jsap.Switch;
import com.martiansoftware.jsap.UnflaggedOption;

/** <p>Computes the approximate neighbourhood function of a graph using HyperANF.
 * 
 * <p>HyperANF is an algorithm computing an approximation of the <em>neighbourhood function</em> of a graph, that is, the function returning
 * for each <var>h</var> the number of pairs of nodes at distance at most <var>h</var>. It has been
 * described in &ldquo;HyperANF: Approximating the Neighbourhood Function of Very Large Graphs
 * on a Budget&rdquo;, by Paolo Boldi, Marco Rosa and Sebastiano Vigna,
 * <i>Proceedings of the 20th international conference on World Wide Web</i>, pages 625&minus;634, ACM, (2011).  
 * It is a breakthrough improvement over the ANF technique described by Christopher R. Palmer, Phillip B. Gibbons, and Christos Faloutsos
 * in &ldquo;Fast Approximation of the &lsquo;Neighbourhood&rsquo; Function for Massive Graphs&rdquo;,
 * <i>Proceedings of the Eighth ACM SIGKDD International Conference on Knowledge Discovery and Data Mining</i>, pages 81&minus;90, ACM (2002).
 *  
 * <p>At step <var>h</var>, for each node we (approximately) keep track using {@linkplain IntHyperLogLogCounterArray HyperLogLog counters}
 * of the set of nodes at distance at most <var>h</var>. At each iteration, the sets associated with the successors of each node are joined,
 * thus obtaining the new sets. A crucial component in making this process efficient and scalable is the usage of
 * <em>broadword programming</em> to implement the join phase, which requires maximising in parallel the list of register associated with 
 * each successor (the implementation is geared towards 64-bits processors). 
 * 
 * <p>Using the approximate sets, for each <var>h</var> we estimates the number of pairs of nodes (<var>x</var>,<var>y</var>) such
 * that the distance from <var>x</var> to <var>y</var> is at most <var>h</var>.
 * 
 * <p>To use an instance of this class, you must first create an instance.
 * Then, you call {@link #init()} and {@link #iterate()} as much as needed (you can init/iterate several time, if you want so). 
 * Finally, you {@link #close()} the instance. The method {@link #modified()} will tell you whether the internal state of
 * the algorithm has changed.
 * 
 * <p>If you pass to the constructor (or on the command line) the <em>transpose</em> of your graph (you can compute it using {@link Transform#transpose(ImmutableGraph)}
 * or {@link Transform#transposeOffline(ImmutableGraph, int)}), when three quarters of the nodes stop changing their value
 * HyperANF will switch to a <em>systolic</em> computation: using the transpose, when a node changes it will signal back
 * to its predecessors that at the next iteration they could change. At the next scan, only the successors of 
 * signalled nodes will be scanned. This strategy makes the last phases of the computation significantly faster. In particular, 
 * when a very small number of nodes is modified by an iteration HyperANF will switch to a systolic <em>local</em> mode,
 * in which all information about modified nodes is kept in (traditional) dictionaries, rather than being represented as arrays of booleans. 
 * 
 * <p>Deciding when to stop iterating is a rather delicate issue. The only safe way is to iterate until {@link #modified()} is zero,
 * and systolic computation makes this goal easily attainable.
 * However, in some cases one can assume that the graph is not pathological, and stop when the relative increment of the number of pairs goes below
 * some threshold.
 * 
 * <p>A {@linkplain #approximateNeighbourhoodFunction(long, double) commodity method} will do everything for you.
 *
 * <h2>Performance issues</h2>
 * 
 * <p>This class can perform computations <em>offline</em>: instead of keeping in core memory 
 * an old and a new copy of the counters, it can dump on disk an <em>update list</em> containing pairs &lt;<var>node</var>,&nbsp;<var>counter</var>>.
 * At the end of an iteration, the update list is loaded and applied to the counters in memory.
 * The process is of course slower, but the core memory used is halved.
 * 
 * <p>If there are several available cores, the runs of {@link #iterate()} will be <em>decomposed</em> into relatively
 * small tasks (small blocks of nodes) and each task will be assigned to the first available core. Since all tasks are completely
 * independent, this ensures a very high degree of parallelism. Be careful, however, as this feature requires a graph with
 * a reasonably fast random access (e.g., in the case of a {@link BVGraph}, short reference chains), as many
 * calls to {@link ImmutableGraph#nodeIterator(int)} will be made. The <em>granularity</em> of the decomposition
 * is the number of nodes assigned to each task.
 * 
 * <p>In any case, when attacking very large graphs (in particular in offline mode) some system tuning (e.g.,
 * increasing the filesystem commit time) is a good idea. Also experimenting with granularity and buffer sizes
 * can be useful. Smaller buffers reduce the waits on I/O calls, but increase the time spent in disk seeks.
 * Large buffers improve I/O, but they use a lot of memory. The best possible setup is the one in which 
 * the cores are 100% busy during the graph scan, and the I/O time
 * logged at the end of a scan is roughly equal to the time that is necessary to reload the counters from disk:
 * essentially, you are computing as fast as possible.
 * 
 * <p>HyperANF keeps carefully track of which counters have changed their value, and uses this information to 
 * speed up the computation. A consequence is that if you iterate up to stabilisation (i.e., until {@link #modified()}
 * is zero) the last iterations will be significantly faster. 
 * 
 * @author Sebastiano Vigna
 */

public class HyperApproximateNeighbourhoodFunction extends IntHyperLogLogCounterArray implements SafelyCloseable {
	private static final Logger LOGGER = Util.getLogger( HyperApproximateNeighbourhoodFunction.class );
	private static final boolean ASSERTS = false;

	private static final long serialVersionUID = 1L;
	
	/** The default granularity of a task. */
	public static final int DEFAULT_GRANULARITY = 1024;
	/** The default size of a buffer in bytes. */
	public static final int DEFAULT_BUFFER_SIZE = 4 * 1024 * 1024;
	/** True if we have the tranpose graph. */
	private final boolean gotTranpose;
	/** True if we started a systolic computation. */
	private boolean systolic;
	/** True if we are preparing a local computation (we are {@link #systolic} and less than 1% nodes were modified). */
	private boolean preLocal;
	/** True if we started a local computation. */
	private boolean local;
	/** A cached copy of the bit vectors used for counting. */
	private final long bits[][];
	/** Whether counters are aligned to longwords. */
	private final boolean longwordAligned;
	/** A mask for the residual bits of a counter (the {@link #counterSize} <code>%</code> {@link Long#SIZE} lowest bits). */
	private final long counterResidualMask;
	/** The number of nodes of the graph, cached. */
	private final int numNodes;
	/** The number of arcs of the graph, cached. */
	private long numArcs;
	/** The square of {@link #numNodes}, cached. */
	private final double squareNumNodes;
	/** The number of cores used in the computation. */
	private final int numberOfThreads;
	/** The size of an I/O buffer, in counters. */
	private final int bufferSize;
	/** The number of actually scanned nodes per task in a multithreaded environment. <strong>Must</strong> be a multiple of {@link Long#SIZE}. */
	private final int granularity;
	/** The number of nodes per task (obtained by adapting {@link #granularity} to the current ratio of modified nodes). <strong>Must</strong> be a multiple of {@link Long#SIZE}. */
	private int adaptiveGranularity;
	/** The value computed by the last call to {@link #iterate()} . */
	private double last;
	/** The current iteration. */
	private int iteration;
	/** If {@link #offline} is true, the name of the temporary file that will be used to write the update list. */
	private final File updateFile;
	/** If {@link #offline} is true, a file channel used to write to the update list. */
	private final FileChannel fileChannel;
	/** If {@link #offline} is true, the random-access file underlying {@link #fileChannel}. */
	private RandomAccessFile randomAccessFile;
	/** A byte buffer used to bulk read/write the update list. */
	private ByteBuffer byteBuffer;
	/** A progress logger, or <code>null</code>. */
	private final ProgressLogger pl;
	/** The lock protecting all critical sections. */
	protected final ReentrantLock lock;
	/** A condition that is notified when all iteration threads are waiting to be started. */
	protected final Condition allWaiting;
	/** The condition on which all iteration threads wait before starting a new phase. */
	protected final Condition start;
	/** A mask containing a one in the most significant bit of each register (i.e., in positions of the form {@link #registerSize registerSize * (i + 1) - 1}). */
	private final long[] msbMask;
	/** A mask containing a one in the least significant bit of each register (i.e., in positions of the form {@link #registerSize registerSize * i}). */
	private final long[] lsbMask;
	/** The current computation phase. */
	public int phase;
	/** Whether this approximator has been already closed. */ 
	private boolean closed;
	/** The threads performing the computation. */
	private final IterationThread thread[];
	/** An atomic integer keeping track of the number of node processed so far. */
	private final AtomicInteger nodes;
	/** An atomic integer keeping track of the number of arcs processed so far. */
	private final AtomicLong arcs;
	/** An array used to gather the harmonic partial sums. */
	private final double[] result;
	/** A variable used to wait for all threads to complete their iteration. */
	private volatile int aliveThreads;
	/** True if the computation is over. */
	private volatile boolean completed;
	/** Total number of write operation performed on {@link #fileChannel}. */
	private volatile long numberOfWrites;
	/** Total wait time in milliseconds of I/O activity on {@link #fileChannel}. */
	private volatile long totalIoMillis;
	/** The starting node of the next chunk of nodes to be processed. */
	private final AtomicLong nextNode;
	/** The number of register modified by the last call to {@link #iterate()}. */
	private final AtomicInteger modified;
	/** Counts the number of unwritten entries when {@link #offline} is true, or
	 * the number of counters that did not change their value. */
	private final AtomicInteger unwritten;
	/** The size of a counter in longwords (ceiled if there are less then {@link Long#SIZE} registers per counter). */
	private final int counterLongwords;
	/** The last computed harmonic partial sum for blocks of 64 counters. It is used to speed up the computation when few nodes change. */
	private final double[] lastBlockSum;
	/** Whether we should used an update list on disk, instead of computing results in core memory. */
	private boolean offline;
	/** If {@link #offline} is false, the arrays where results are stored. */
	private final long[][] resultBits;
	/** If {@link #offline} is false, bit vectors wrapping {@link #resultBits}. */
	private final LongArrayBitVector[] resultBitVector;
	/** If {@link #offline} is false, a {@link #registerSize}-bit views of {@link #resultBits}. */
	private final LongBigList resultRegisters[];
	/** For each counter, whether it has changed its value. We use an array of boolean (instead of a {@link LongArrayBitVector}) just for access speed. */
	private boolean[] modifiedCounter;
	/** For each newly computed counter, whether it has changed its value. {@link #modifiedCounter}
	 * will be updated with the content of this bit vector by the end of the iteration. */
	private boolean[] modifiedResultCounter;
	/** For each counter, whether it has changed its value. We use an array of boolean (instead of a {@link LongArrayBitVector}) just for access speed. */
	private boolean[] nextMustBeChecked;
	/** For each newly computed counter, whether it has changed its value. {@link #modifiedCounter}
	 * will be updated with the content of this bit vector by the end of the iteration. */
	private boolean[] mustBeChecked;
	/** If {@link #local} is true, the list of nodes that should be scanned. */
	private int[] localCheckList;
	/** If {@link #local} is true, the list of nodes that should be scanned on the next iteration. Note that this set is synchronized. */
	private final IntSet localNextMustBeChecked;
	/** One of the throwables thrown by some of the threads, if at least one thread has thrown a throwable. */
	private volatile Throwable threadThrowable;
	
	private final static int ensureEnoughRegisters( final int log2m ) {
		if ( log2m < 4 ) throw new IllegalArgumentException( "There must be at least 16 registers per counter" );
		return log2m;
	}
	
	/** Creates a new approximator for the neighbourhood function.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 * @param cores the number of cores to be used (0 for {@link Runtime#availableProcessors()}).
	 * @param bufferSize the size of an I/O buffer in bytes (0 for {@link #DEFAULT_BUFFER_SIZE}).
	 * @param granularity the number of node per task in a multicore environment (it will be rounded to the next multiple of 64), or 0 for {@link #DEFAULT_GRANULARITY}.
	 * @param offline if true, results of an iteration will be stored on disk.
	 */
	public HyperApproximateNeighbourhoodFunction( final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl, final int cores, final int bufferSize, final int granularity, final boolean offline ) throws IOException {
		this( g, gt, log2m, pl, cores, bufferSize, granularity, offline, Util.randomSeed() );
	}

	/** Creates a new approximator for the neighbourhood function using default values.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 */
	public HyperApproximateNeighbourhoodFunction( final ImmutableGraph g, final ImmutableGraph gt, final int log2m ) throws IOException {
		this( g, gt, log2m, null, 0, 0, 0, false );
	}

	/** Creates a new approximator for the neighbourhood function using default values.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code> in case you want to perform systolic computations, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public HyperApproximateNeighbourhoodFunction( final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl ) throws IOException {
		this( g, null, log2m, pl, 0, 0, 0, false );
	}

	/** Creates a new approximator for the neighbourhood function using default values and disabling systolic computation.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 */
	public HyperApproximateNeighbourhoodFunction( final ImmutableGraph g, final int log2m ) throws IOException {
		this( g, null, log2m );
	}

	/** Creates a new approximator for the neighbourhood function using default values and disabling systolic computation.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 */
	public HyperApproximateNeighbourhoodFunction( final ImmutableGraph g, final int log2m, final ProgressLogger pl ) throws IOException {
		this( g, null, log2m, pl );
	}



	/** Computes the number of threads.
	 * 
	 * <p>If the specified number of threads is zero, an ideal number will be computed by
	 * calling {@link Runtime#availableProcessors()}.
	 * 
	 * @param suggestedNumberOfThreads
	 * @return the actual number of threads.
	 */
	private final static int numberOfThreads( final int suggestedNumberOfThreads ) {
		if ( suggestedNumberOfThreads != 0 ) return suggestedNumberOfThreads;
		return Runtime.getRuntime().availableProcessors();
	}
	
	/** Creates a new approximator for the neighbourhood function.
	 * 
	 * @param g the graph whose neighbourhood function you want to compute.
	 * @param gt the tranpose of <code>g</code>, or <code>null</code>.
	 * @param log2m the logarithm of the number of registers per counter.
	 * @param pl a progress logger, or <code>null</code>.
	 * @param numberOfThreads the number of threads to be used (0 for automatic sizing).
	 * @param bufferSize the size of an I/O buffer in bytes (0 for {@link #DEFAULT_BUFFER_SIZE}).
	 * @param granularity the number of node per task in a multicore environment (it will be rounded to the next multiple of 64), or 0 for {@link #DEFAULT_GRANULARITY}.
	 * @param offline if true, results of an iteration will be stored on disk.
	 * @param seed the random seed passed to {@link IntHyperLogLogCounterArray#IntHyperLogLogCounterArray(int, long, int, long)}.
	 */
	public HyperApproximateNeighbourhoodFunction( final ImmutableGraph g, final ImmutableGraph gt, final int log2m, final ProgressLogger pl, final int numberOfThreads, final int bufferSize, final int granularity, final boolean offline, final long seed ) throws IOException {
		super( g.numNodes(), g.numNodes(), ensureEnoughRegisters( log2m ), seed );

		info( "Seed : " + Long.toHexString( seed ) );

		gotTranpose = gt != null;
		bits = new long[ bitVector.length ][];
		for( int i = bits.length; i-- != 0; ) bits[ i ] = bitVector[ i ].bits();
		localNextMustBeChecked = gotTranpose ? IntSets.synchronize( new IntOpenHashSet( Hash.DEFAULT_INITIAL_SIZE, Hash.VERY_FAST_LOAD_FACTOR ) ) : null;
		
		numNodes = g.numNodes();
		numArcs = g.numArcs();
		squareNumNodes = (double)numNodes * numNodes;

		modifiedCounter = new boolean[ numNodes ];
		modifiedResultCounter = offline ? null : new boolean[ numNodes ];
		if ( gt != null ) {
			mustBeChecked = new boolean[ numNodes ];
			nextMustBeChecked = new boolean[ numNodes ];
		}

		counterLongwords = ( counterSize + Long.SIZE - 1 ) / Long.SIZE;
		counterResidualMask = ( 1L << counterSize % Long.SIZE ) - 1;
		longwordAligned = counterSize % Long.SIZE == 0;
		
		this.pl = pl;
		this.offline = offline;
		this.numberOfThreads = numberOfThreads( numberOfThreads );
		this.granularity = numberOfThreads == 1 ? numNodes : granularity == 0 ? DEFAULT_GRANULARITY : ( ( granularity + Long.SIZE - 1 ) & ~( Long.SIZE - 1 ) );
		this.bufferSize = Math.max( 1, ( bufferSize == 0 ? DEFAULT_BUFFER_SIZE : bufferSize ) / ( ( Long.SIZE / Byte.SIZE ) * ( counterLongwords + 1 ) ) ); 
		
		info( "Relative standard deviation: " + Util.format( 100 * IntHyperLogLogCounterArray.relativeStandardDeviation( log2m ) ) + "% (" + m  + " registers/counter, " + registerSize + " bits/counter)" );
		if ( offline ) info( "Running " + this.numberOfThreads + " threads with a buffer of " + Util.formatSize( this.bufferSize ) + " counters" );
		else info( "Running " + this.numberOfThreads + " threads" );

		thread = new IterationThread[ this.numberOfThreads ];
		result = new double[ this.numberOfThreads ];
		
		if ( offline ) {
			info( "Creating update list..." );
			updateFile = File.createTempFile( HyperApproximateNeighbourhoodFunction.class.getName(), "temp" );
			updateFile.deleteOnExit();	
			fileChannel = ( randomAccessFile = new RandomAccessFile( updateFile, "rw" ) ).getChannel();
			byteBuffer = ByteBuffer.allocate( 8 * ( counterLongwords + 1 ) * this.bufferSize );
		}
		else {
			updateFile = null;
			fileChannel = null;
		}

		// We initialise the masks for the broadword code in max().
		msbMask = new long[ counterLongwords ];
		lsbMask = new long[ counterLongwords ];
		for( int i = registerSize - 1; i < msbMask.length * Long.SIZE; i += registerSize ) msbMask[ i / Long.SIZE ] |= 1L << i % Long.SIZE; 
		for( int i = 0; i < lsbMask.length * Long.SIZE; i += registerSize ) lsbMask[ i / Long.SIZE ] |= 1L << i % Long.SIZE;
		
		nextNode = new AtomicLong();
		nodes = new AtomicInteger();
		arcs = new AtomicLong();
		modified = new AtomicInteger();
		unwritten = new AtomicInteger();

		if ( ! offline ) {
			info( "Allocating result bit vectors..." );
			// Allocate vectors that will store the result.
			resultBitVector = new LongArrayBitVector[ bitVector.length ];
			resultBits = new long[ bitVector.length ][];
			resultRegisters = new LongBigList[ bitVector.length ];
			for( int i = bitVector.length; i-- != 0; ) resultRegisters[ i ] = ( resultBitVector[ i ] = LongArrayBitVector.wrap( resultBits[ i ] = new long[ bitVector[ i ].bits().length ] ) ).asLongBigList( registerSize );
		}
		else {
			resultBitVector = null;
			resultBits = null;
			resultRegisters = null;
		}

		lastBlockSum = new double[ ( numNodes + Long.SIZE - 1 ) / Long.SIZE ];

		info( "HyperANF memory usage: " + Util.formatSize2( usedMemory() ) + " [not counting graph(s)]" );
		
		lock = new ReentrantLock();
		allWaiting = lock.newCondition();
		start = lock.newCondition();
		aliveThreads = this.numberOfThreads;
		
		if ( this.numberOfThreads == 1 ) ( thread[ 0 ] = new IterationThread( g, gt, 0, 0, numNodes ) ).start();
		else {
			// Make it a multiple of Long.SIZE for easier handling of the partial sum cache.
			final int increment = ( ( ( numNodes + this.numberOfThreads - 1 ) / this.numberOfThreads + Long.SIZE - 1 ) / Long.SIZE ) * Long.SIZE;
			for( int i = 0; i < this.numberOfThreads; i++ ) ( thread[ i ] = new IterationThread( g.copy(), gt != null ? gt.copy() : null, i, i * increment, (int)Math.min( numNodes, ( i + 1L ) * increment ) ) ).start();
		}
		
		// We wait for all threads being read to start.
		lock.lock();
		try {
			if ( aliveThreads != 0 ) allWaiting.await();
		}
		catch ( InterruptedException e ) {
			throw new RuntimeException( e );
		}
		finally {
			lock.unlock();
		}
	}

	private void info( String s ) {
		if ( pl != null ) pl.logger.info( s );
	}
	
	private long usedMemory() {
		long bytes = 0;
		for( long[] a: bits ) bytes += a.length * ( (long)Long.SIZE / Byte.SIZE );
		bytes += result.length * ( (long)Double.SIZE / Byte.SIZE );
		bytes += lastBlockSum.length * ( (long)Double.SIZE / Byte.SIZE );
		if ( resultBits != null ) for( long[] a: resultBits ) bytes += a.length * ( (long)Long.SIZE / Byte.SIZE );
		if ( modifiedCounter != null ) bytes += modifiedCounter.length;
		if ( modifiedResultCounter != null ) bytes += modifiedResultCounter.length;
		if ( nextMustBeChecked != null ) bytes += nextMustBeChecked.length;
		if ( mustBeChecked != null ) bytes += mustBeChecked.length;
		return bytes;
	}


	/** Initialises the approximator.
	 * 
	 * <p>This method must be call before a series of {@linkplain #iterate() iterations}.
	 */
	public void init() {
		info( "Clearing all registers..." );
		// Clear up all registers.
		for( LongArrayBitVector bv: bitVector ) bv.fill( false );
		// Invalidate the cache of partial harmonic sums.
		DoubleArrays.fill( lastBlockSum, -1 );
		// We load the counter i with node i.
		for( int i = numNodes; i-- != 0; ) add( i, i );

		iteration = -1;
		completed = systolic = local = preLocal = false;
		
		if ( offline ) byteBuffer.clear();

		last = numNodes; // The initial value (the iteration for this value does not actually happen).
		BooleanArrays.fill( modifiedCounter, true ); // Initially, all counters are modified.
		
		if ( pl != null ) { 
			pl.itemsName = "iterates";
			pl.start( "Iterating..." );
		}
	}

	public void close() throws IOException {
		if ( closed ) return;
		closed = true;
		
		lock.lock();
		try {
			completed = true;
			start.signalAll();
		}
		finally {
			lock.unlock();
		}

		for( Thread t: thread )
			try {
				t.join();
			}
			catch ( InterruptedException e ) {
				throw new RuntimeException( e );
			}
		
			if ( offline ) {
				randomAccessFile.close();
				fileChannel.close();
				updateFile.delete();
			}
	}

	protected void finalize() throws Throwable {
		try {
			if ( ! closed ) {
				LOGGER.warn( "This " + this.getClass().getName() + " [" + toString() + "] should have been closed." );
				close();
			}
		}
		finally {
			super.finalize();
		}
	}


	/** Performs a multiple precision subtraction, leaving the result in the first operand.
	 * 
	 * @param x a vector of longs.
	 * @param y a vector of longs that will be subtracted from <code>x</code>.
	 * @param l the length of <code>x</code> and <code>y</code>.
	 */
	private final static void subtract( final long[] x, final long[] y, final int l ) {
		boolean borrow = false;
		
		for( int i = 0; i < l; i++ ) {			
			if ( ! borrow || x[ i ]-- != 0 ) borrow = x[ i ] < y[ i ] ^ x[ i ] < 0 ^ y[ i ] < 0; // This expression returns the result of an unsigned strict comparison.
			x[ i ] -= y[ i ];
		}	
	}
	
	/** Computes the register-by-register maximum of two bit vectors.
	 * 
	 * @param x first vector of longs, representing a bit vector in {@link LongArrayBitVector} format, where the result will be stored.
	 * @param y a second vector of longs, representing a bit vector in {@link LongArrayBitVector} format, that will be maximised with <code>x</code>.
	 * @param r the register size.
	 */
	
	protected final void max( final long[] x, final long[] y, final int r, final long[] accumulator, final long[] mask ) {
		final int l = x.length;
		final long[] msbMask = this.msbMask;

		/* We work in two phases. Let H_r (msbMask) by the mask with the
		 * highest bit of each register (of size r) set, and L_r (lsbMask) 
		 * be the mask with the lowest bit of each register set. 
		 * We describe the algorithm on a single word.
		 * 
		 * If the first phase we perform an unsigned strict register-by-register 
		 * comparison of x and y, using the formula
		 * 
		 * z = (  ( ((y | H_r) - (x & ~H_r)) | (y ^ x) )^ (y | ~x)  ) & H_r
		 *  
		 * Then, we generate a register-by-register mask of all ones or
		 * all zeroes, depending on the result of the comparison, using the 
		 * formula
		 * 
		 * ( ( (z >> r-1 | H_r) - L_r ) | H_r ) ^ z
		 * 
		 * At that point, it is trivial to select from x and y the right values.
		 */
		
		// We load y | H_r into the accumulator.
		for( int i = l; i-- != 0; ) accumulator[ i ] = y[ i ] | msbMask[ i ]; 
		// We subtract x & ~H_r, using mask as temporary storage
		for( int i = l; i-- != 0; ) mask[ i ] = x[ i ] & ~msbMask[ i ]; 
		subtract( accumulator, mask, l );
		
		// We OR with x ^ y, XOR with ( x | ~y), and finally AND with H_r. 
		for( int i = l; i-- != 0; ) accumulator[ i ] = ( ( accumulator[ i ] | ( y[ i ] ^ x[ i ] ) ) ^ ( y[ i ] | ~x[ i ] ) ) & msbMask[ i ]; 
	
		if ( ASSERTS ) {
			final LongBigList a = LongArrayBitVector.wrap( x ).asLongBigList( r );
			final LongBigList b = LongArrayBitVector.wrap( y ).asLongBigList( r );
			for( int i = 0; i < m; i++ ) {
				long pos = ( i + 1 ) * (long)r - 1; 
				assert ( b.getLong( i ) < a.getLong( i ) ) == ( ( accumulator[ (int)( pos / Long.SIZE ) ] & 1L << pos % Long.SIZE ) != 0 ); 
			}
		}
		
		// We shift by r - 1 places and put the result into mask.
		final int rMinus1 = r - 1, longSizeMinusRMinus1 = Long.SIZE - rMinus1;
		for( int i = l - 1; i-- != 0; ) mask[ i ] = accumulator[ i ] >>> rMinus1 | accumulator[ i + 1 ] << longSizeMinusRMinus1 | msbMask[ i ]; 
		mask[ l - 1 ] = accumulator[ l - 1 ] >>> rMinus1 | msbMask[ l - 1 ];

		// We subtract L_r from mask.
		subtract( mask, lsbMask, l );

		// We OR with H_r and XOR with the accumulator.
		for( int i = l; i-- != 0; ) mask[ i ] = ( mask[ i ] | msbMask[ i ] ) ^ accumulator[ i ];
		
		if ( ASSERTS ) {
			final long[] t = x.clone();
			LongBigList a = LongArrayBitVector.wrap( t ).asLongBigList( r );
			LongBigList b = LongArrayBitVector.wrap( y ).asLongBigList( r );
			for( int i = 0; i < Long.SIZE * l / r; i++ ) a.set( i, Math.max( a.getLong( i ), b.getLong( i ) ) );
			// Note: this must be kept in sync with the line computing the result.
			for( int i = l; i-- != 0; ) assert t[ i ] == ( ~mask[ i ] & x[ i ] | mask[ i ] & y[ i ] );
		}

		// Finally, we use mask to select the right bits from x and y and store the result.
		for( int i = l; i-- != 0; ) x[ i ] ^= ( x[ i ] ^ y[ i ] ) & mask[ i ]; 

	}

	/** Copies a counter to a local array.
	 * 
	 * @param chunkBits the array storing the counter.
	 * @param t a local destination array.
	 * @param node the node number.
	 */
	protected final void copyToLocal( final long[] chunkBits, final long[] t, final int node ) {
		if ( longwordAligned ) System.arraycopy( chunkBits, (int)( offset( node ) / Long.SIZE ), t, 0, counterLongwords );
		else {
			// Offset in bits
			final long offset = offset( node );
			// Offsets in elements in the array
			final int longwordOffset = (int)( offset / Long.SIZE );
			// Offset in bits in the word of index longwordOffset
			final int bitOffset = (int)( offset % Long.SIZE );
			final int last = counterLongwords - 1; 

			if ( bitOffset == 0 ) {
				for( int i = last; i-- != 0; ) t[ i ] = chunkBits[ longwordOffset + i ];
				t[ last ] = chunkBits[ longwordOffset + last ] & counterResidualMask;
			}
			else {
				for( int i = 0; i < last; i++ ) t[ i ] = chunkBits[ longwordOffset + i ] >>> bitOffset | chunkBits[ longwordOffset + i + 1 ] << Long.SIZE - bitOffset;  
				t[ last ] = chunkBits[ longwordOffset + last ] >>> bitOffset & counterResidualMask;
			}
		}
	}
	
	/** Copies a counter from a local array.
	 * @param t a local array.
	 * @param chunkBits the array where the counter will be stored.
	 * @param node the node number.
	 */
	protected final void copyFromLocal( final long[] t, final long[] chunkBits, final int node ) {
		if ( longwordAligned ) System.arraycopy( t, 0, chunkBits, (int)( offset( node ) / Long.SIZE ), counterLongwords );
		else {
			// Offset in bits
			final long offset = offset( node );
			// Offsets in elements in the array
			final int longwordOffset = (int)( offset / Long.SIZE );
			// Offset in bits in the word of index longwordOffset
			final int bitOffset = (int)( offset % Long.SIZE );
			final int last = counterLongwords - 1; 

			if ( bitOffset == 0 ) {
				for( int i = last; i-- != 0; ) chunkBits[ longwordOffset + i ] = t[ i ];
				chunkBits[ longwordOffset + last ] &= ~counterResidualMask;
				chunkBits[ longwordOffset + last ] |= t[ last ] & counterResidualMask;
			}
			else {
				chunkBits[ longwordOffset ] &= ( 1L << bitOffset ) - 1;
				chunkBits[ longwordOffset ] |= t[ 0 ] << bitOffset;
				
				for( int i = 1; i < last; i++ ) chunkBits[ longwordOffset + i ] = t[ i - 1 ] >>> Long.SIZE - bitOffset | t[ i ] << bitOffset; 

				final int remaining = counterSize % Long.SIZE + bitOffset;

				final long mask = -1L >>> ( Long.SIZE - Math.min( Long.SIZE, remaining ) );
				chunkBits[ longwordOffset + last ] &= ~mask;
				chunkBits[ longwordOffset + last ] |= mask & ( t[ last - 1 ] >>> Long.SIZE - bitOffset | t[ last ] << bitOffset );

				// Note that it is impossible to enter in this conditional unless you use 7 or more bits per register, which is unlikely.
				if ( remaining > Long.SIZE ) {
					final long mask2 = ( 1L << remaining - Long.SIZE ) - 1;
					chunkBits[ longwordOffset + last + 1 ] &= ~mask2;
					chunkBits[ longwordOffset + last + 1 ] |= mask2 & ( t[ last ] >>> Long.SIZE - bitOffset );
				}
			}
			
			if ( ASSERTS ) {
				final LongArrayBitVector l = LongArrayBitVector.wrap( chunkBits );
				for( int i = 0; i < counterSize; i++ ) assert l.getBoolean( offset + i ) == ( ( t[ i / Long.SIZE ] & ( 1L << i % Long.SIZE ) ) != 0 ); 
			}
		}
	}
	
	/** Transfers the content of a counter between two parallel array of longwords.
	 * 
	 * @param source the source array.
	 * @param dest the destination array.
	 * @param node the node number.
	 */
	protected final void transfer( final long[] source, final long[] dest, final int node ) {
		if ( longwordAligned ) {
			final int longwordOffset = (int)( offset( node ) / Long.SIZE );
			System.arraycopy( source, longwordOffset, dest, longwordOffset, counterLongwords );
		}
		else { 
			// Offset in bits in the array
			final long offset = offset( node );
			// Offsets in elements in the array
			final int longwordOffset = (int)( offset / Long.SIZE );
			// Offset in bits in the word of index longwordOffset
			final int bitOffset = (int)( offset % Long.SIZE );
			final int last = counterLongwords - 1; 

			if ( bitOffset == 0 ) {
				for( int i = last; i-- != 0; ) dest[ longwordOffset + i ] = source[ longwordOffset + i ];
				dest[ longwordOffset + last ] &= ~counterResidualMask;
				dest[ longwordOffset + last ] |= source[ longwordOffset + last ] & counterResidualMask;
			}
			else {
				final long mask = -1L << bitOffset;
				dest[ longwordOffset ] &= ~mask;
				dest[ longwordOffset ] |= source[ longwordOffset ] & mask;
				
				for( int i = 1; i < last; i++ ) dest[ longwordOffset + i ] = source[ longwordOffset + i ]; 

				final int remaining = ( counterSize + bitOffset ) % Long.SIZE;
				if ( remaining == 0 ) dest[ longwordOffset + last ] = source[ longwordOffset + last ];
				else {
					final long mask2 = ( 1L << remaining ) - 1;
					dest[ longwordOffset + last ] &= ~mask2;
					dest[ longwordOffset + last ] |= mask2 & source[ longwordOffset + last ];
				}
			}

			if ( ASSERTS ) {
				LongArrayBitVector aa = LongArrayBitVector.wrap( source );
				LongArrayBitVector bb = LongArrayBitVector.wrap( dest );
				for( int i = 0; i < counterSize; i++ ) assert aa.getBoolean( offset + i ) == bb.getBoolean( offset + i ); 
			}
		}
	}
	
	private final class IterationThread extends Thread {
		/** A copy of the graph for this thread only. */
		private final ImmutableGraph g;
		/** A copy of the tranpose graph for this thread only. */
		private final ImmutableGraph gt;
		/** The index of this thread (just used to identify the thread). */
		private final int index;
		/**  The first node (inclusive) that this thread will have to scan when computing the harmonic partial sums. */
		private final int from;
		/** The last node (exclusive) that this thread will have to scan when computing the harmonic partial sums. */
		private final int to;
		/** The time spent in I/O calls by this thread. */
		private long ioMillis;
		
		/** Create a new iteration thread.
		 * @param index the index of this thread (just used to identify the thread).
		 * @param from the first node (inclusive) that this thread will have to scan when computing the harmonic partial sums.
		 * @param to the last node (exclusive) that this thread will have to scan when computing the harmonic partial sums.
		 */
		private IterationThread( final ImmutableGraph g, ImmutableGraph gt, final int index, final int from, final int to ) {
			this.g = g;
			this.gt = gt;
			this.index = index;
			this.from = from;
			this.to = to;
		}

		private final boolean synchronize( final int phase ) throws InterruptedException {
			//System.err.println( "Thread " + index + " is going to wait...(" + phase + ")" );
			lock.lock();
			try {
				if ( --aliveThreads == 0 ) allWaiting.signal();
				if ( aliveThreads < 0 ) throw new IllegalStateException();
				start.await();
				if ( completed ) return true;
				if ( phase != HyperApproximateNeighbourhoodFunction.this.phase ) throw new IllegalStateException( "Main thread is in phase " + HyperApproximateNeighbourhoodFunction.this.phase + ", but thread " + index + " is heading to phase " + phase );
				return false;
			}
			finally {
				lock.unlock();
			}
		}

		@Override
		public void run() {
			try {
				// Lots of local caching.
				final int registerSize = HyperApproximateNeighbourhoodFunction.this.registerSize;
				final int counterLongwords = HyperApproximateNeighbourhoodFunction.this.counterLongwords;
				final boolean offline = HyperApproximateNeighbourhoodFunction.this.offline;
				final ImmutableGraph g = this.g;

				final long[] accumulator = new long[ counterLongwords ];
				final long[] mask = new long[ counterLongwords ];

				final long t[] = new long[ counterLongwords ];
				final long prevT[] = new long[ counterLongwords ];
				final long u[] = new long[ counterLongwords ];

				final ByteBuffer byteBuffer = offline ? ByteBuffer.allocate( ( Long.SIZE / Byte.SIZE ) * bufferSize * ( counterLongwords + 1 ) ) : null;
				if ( offline ) byteBuffer.clear();

				for(;;) {
					
					if ( synchronize( 0 ) ) return;

					// These variables might change across executions of the loop body.
					final int granularity = HyperApproximateNeighbourhoodFunction.this.adaptiveGranularity;
					final long bits[][] = HyperApproximateNeighbourhoodFunction.this.bits;
					final long resultBits[][] = HyperApproximateNeighbourhoodFunction.this.resultBits;
					final boolean[] modifiedCounter = HyperApproximateNeighbourhoodFunction.this.modifiedCounter;
					final boolean[] modifiedResultCounter = HyperApproximateNeighbourhoodFunction.this.modifiedResultCounter;
					final boolean[] mustBeChecked = HyperApproximateNeighbourhoodFunction.this.mustBeChecked;
					final boolean[] nextMustBeChecked = HyperApproximateNeighbourhoodFunction.this.nextMustBeChecked;
					final boolean systolic = HyperApproximateNeighbourhoodFunction.this.systolic;
					final boolean local = HyperApproximateNeighbourhoodFunction.this.local;
					final boolean preLocal = HyperApproximateNeighbourhoodFunction.this.preLocal;
					final int[] localCheckList = HyperApproximateNeighbourhoodFunction.this.localCheckList;
					final IntSet localNextMustBeChecked = HyperApproximateNeighbourhoodFunction.this.localNextMustBeChecked;
					
					long start = -1;
					int end = -1;
					int modified = 0; // The number of registers that have been modified during the computation of the present task.
					int unwritten = 0; // The number of counters not written to disk.

					// In a local computation tasks are based on the content of localCheckList.
					int upperLimit = local ? localCheckList.length : numNodes;
					
					for(;;) {

						// Try to get another piece of work.
						start = nextNode.getAndAdd( granularity ); 
						if ( start >= upperLimit ) {
							nextNode.getAndAdd( -granularity );
							break;
						}
						
						end = (int)(Math.min( upperLimit, start + granularity ) );

						final NodeIterator nodeIterator = local || systolic ? null : g.nodeIterator( (int)start ); 
						long arcs = 0;
						
						for( int i = (int)start; i < end; i++ ) {
							final int node = local ? localCheckList[ i ] : i;
							/* The three cases in which we enumerate successors:
							 * 1) A non-systolic computation (we don't know anything, so we enumerate).
							 * 2) A systolic, local computation (the node is by definition to be checked, as it comes from the local check list).
							 * 3) A systolic, non-local computation in which the node should be checked. 
							 */
							if ( ! systolic || local || mustBeChecked[ node ] ) {
								int d;
								int[] successor = null;
								LazyIntIterator successors = null;
								
								if ( local || systolic ) {
									d = g.outdegree( node );
									successors = g.successors( node );
								}
								else {
									nodeIterator.nextInt();
									d = nodeIterator.outdegree();
									successor = nodeIterator.successorArray();
								}
								 
								final int chunk = chunk( node );
								copyToLocal( bits[ chunk ], t, node );
								// Caches t's values into prevT
								System.arraycopy( t, 0, prevT, 0, counterLongwords );
								
								boolean counterModified = false;

								for( int j = d; j-- != 0; ) {
									final int s = local || systolic ? successors.nextInt() : successor[ j ];
									/* Neither self-loops nor unmodified counter do influence the computation. Note 
									 * that in local mode we no longer keep track of modified counters. */
									if ( s != node && ( local || modifiedCounter[ s ] ) ) { 
										counterModified = true; // This is just to mark that we entered the loop at least once.
										copyToLocal( bits[ chunk( s ) ], u, s );
										max( t, u, registerSize, accumulator, mask );
									}
								}

								arcs += d;
								
								if ( ASSERTS )  {
									LongBigList test = LongArrayBitVector.wrap( t ).asLongBigList( registerSize );
									for( int rr = 0; rr < m; rr++ ) {
										int max = (int)registers[ chunk( node ) ].getLong( ( (long)node << log2m ) + rr );
										if ( local || systolic ) successors = g.successors( node );
										for( int j = d; j-- != 0; ) {
											final int s = local || systolic ? successors.nextInt() : successor[ j ];
											max = Math.max( max, (int)registers[ chunk( s ) ].getLong( ( (long)s << log2m ) + rr ) );
										}
										assert max == test.getLong( rr ) : max + "!=" + test.getLong( rr ) + " [" + rr + "]";
									}
								}

								if ( counterModified ) {
									/* If we enter this branch, we have maximised with at least one successor.
									 * We must thus check explicitly whether we have modified the counter. */
									counterModified = false;
									for( int p = counterLongwords; p-- != 0; ) 
										if ( prevT[ p ] != t[ p ] ) {
											counterModified = true;
											break;
										}
								}

								// Here counterModified is true only if the counter was *actually* modified.
								if ( counterModified ) {
									/* We keep track of modified counters in the result either if we are preparing
									 * a local computation, or if we are not in offline mode (in offline mode
									 * modified counters are computed when the update list is reloaded). 
									 * Note that we must add the current node to the must-be-checked set 
									 * for the next iteration if it is modified, as it might need a copy 
									 * to the result array at the next iteration. */
									if ( preLocal ) localNextMustBeChecked.add( node );
									else if ( ! offline ) modifiedResultCounter[ node ] = true;

									// We invalidate the cache of partial harmonic sum the current node belongs to.
									lastBlockSum[ node / Long.SIZE ] = -1;

									if ( systolic ) {
										final LazyIntIterator predecessors = gt.successors( node );
										int p;
										/* In systolic computations we must keep track of which counters must
										 * be checked on the next iteration. If we are preparing a local computation,
										 * we do this explicitly, by adding the predecessors of the current
										 * node to a set. Otherwise, we do this implicitly, by setting the
										 * corresponding entry in an array. */
										if ( preLocal ) while( ( p = predecessors.nextInt() ) != -1 ) localNextMustBeChecked.add( p );
										else while( ( p = predecessors.nextInt() ) != -1 ) nextMustBeChecked[ p ] = true;
									}

									modified++;
								}

								if ( offline ) {
									if ( counterModified ) {
										byteBuffer.putLong( node );
										for( int p = counterLongwords; p-- != 0; ) byteBuffer.putLong( t[ p ] );

										if ( ! byteBuffer.hasRemaining() ) {
											byteBuffer.flip();
											long time = -System.currentTimeMillis();
											fileChannel.write( byteBuffer );
											time += System.currentTimeMillis();
											totalIoMillis += time;
											ioMillis += time;
											numberOfWrites++;
											byteBuffer.clear();
										}
									}
									else unwritten++;
								}
								else {
									/* This is slightly subtle: if a counter is not modified, and
									 * the present value was not a modified value in the first place,
									 * then we can avoid updating the result altogether. In local computations
									 * we must always update, as we do not keep track of modified counters
									 * (but we know that all counters modified in the previous iteration
									 * are in the local check list). */
									if ( counterModified || local || modifiedCounter[ node ] ) copyFromLocal( t, resultBits[ chunk ], node );
									else unwritten++;
								}
							}
							else if ( ! offline ) {
								/* Even if we cannot possible have changed our value, still our copy
								 * in the result vector might need to be updated because it does not
								 * reflect our current value. */
								if ( modifiedCounter[ node ] ) {
									final int chunk = chunk( node );
									transfer( bits[ chunk ], resultBits[ chunk ], node );
								}
								else unwritten++;
							}
						}

						// Update the global progress counter.
						HyperApproximateNeighbourhoodFunction.this.arcs.addAndGet( arcs );
						nodes.addAndGet( end - (int)start );
					}

					//System.err.println( "Thread " + this + " completed at time " + System.currentTimeMillis() + "; I/O time: " + ioMillis  );					

					if ( offline ) {
						// If we can avoid at all calling FileChannel.write(), we do so.
						if( byteBuffer.position() != 0 ) {
							byteBuffer.flip();
							long time = -System.currentTimeMillis();
							fileChannel.write( byteBuffer );
							time += System.currentTimeMillis();
							totalIoMillis += time;
							ioMillis += time;
							numberOfWrites++;
							byteBuffer.clear();
						}
					}

					HyperApproximateNeighbourhoodFunction.this.modified.addAndGet( modified );
					HyperApproximateNeighbourhoodFunction.this.unwritten.addAndGet( unwritten );

					if ( offline ) {
						synchronize( 1 );
						//final boolean logUpdated = HyperApproximateNeighbourhoodFunction.this.modified.intValue() < 16;
						//System.err.println( "Modified nodes: " +HyperApproximateNeighbourhoodFunction.this.modified.intValue() + " logUpdated: " + logUpdated );
						
						// Read into memory newly computed counters, updating modifiedCounter.
						for(;;) {
							byteBuffer.clear();
							if ( fileChannel.read( byteBuffer ) <= 0 ) break;
							byteBuffer.flip();
							while( byteBuffer.hasRemaining() ) {
								final int node = (int)byteBuffer.getLong();
								for( int p = counterLongwords; p-- != 0; ) t[ p ] = byteBuffer.getLong();
								copyFromLocal( t, bits[ chunk( node ) ], node );
								//if ( logUpdated ) LOGGER.info( "Updating node " + node );
								if ( ! preLocal ) modifiedCounter[ node ] = true;
							}
						}
					}
					
					synchronize( 2 );
					
					// Compute harmonic partial sum. We know that from is a multiple of Long.SIZE. Invalidated entries contain -1.					
					final double[] lastBlockSum = HyperApproximateNeighbourhoodFunction.this.lastBlockSum;
					double result = 0, c = 0, w, x;
					
					// Kahan summation
					for( int i = from; i < to; i += Long.SIZE ) {
						double blockSum = lastBlockSum[ i / Long.SIZE ];
						if ( blockSum == -1 ) {
							blockSum = 0;
							for( int j = Math.min( to, i + Long.SIZE ); j-- != i; ) blockSum += count( j );
							lastBlockSum[ i / Long.SIZE ] = blockSum;
						}
						
						w = blockSum - c;
						x = result + w;
						c = ( x - result ) - w;
						result = x;
					}
					
					HyperApproximateNeighbourhoodFunction.this.result[ index ] = result;
				}
			}
			catch( Throwable t ) {
				t.printStackTrace();
				threadThrowable = t;
				lock.lock();
				try {
					if ( --aliveThreads == 0 ) allWaiting.signal();
				}
				finally {
					lock.unlock();
				}
			}
		}
		
		public String toString() {
			return "Thread " + index;
		}
	}
	
	/** Performs a new iteration of HyperANF.
	 *
	 * @return an approximation of 	the following value of the neighbourhood function (the first returned value is for distance one).
	 */
	public double iterate() throws IOException {
		try {
			iteration++;
						
			// Let us record whether the previous computation was systolic.
			final boolean preSystolic = systolic; 
			
			/* If less than one fourth of the nodes have been modified, and we have the transpose, 
			 * it is time to pass to a systolic computation. */
			systolic = gotTranpose && iteration > 0 && modified.get() < numNodes / 4;

			// If we completed the last iteration in pre-local mode, we MUST run in local mode.
			local = preLocal;
			
			// We run in pre-local mode if we are systolic and few nodes where modified.
			preLocal = systolic && modified.get() < numNodes / 100;

			info( "Starting " + ( systolic ? "systolic iteration (local: " + local + "; pre-local: " + preLocal + ")"  : "standard " + ( offline ? "offline " : "" ) + "iteration" ) );
			
			if ( local ) {
				/* In case of a local computation, we convert the set of must-be-checked for the 
				 * next iteration into a check list. */
				localCheckList = localNextMustBeChecked.toIntArray();
			}
			else if ( systolic ) {
				// Systolic, non-local computations store the could-be-modified set implicitly into this array.
				BooleanArrays.fill( nextMustBeChecked, false );
				// If the previous computation wasn't systolic, we must assume that all registers could have changed.
				if ( ! preSystolic ) BooleanArrays.fill( mustBeChecked, true );
			}

			if ( preLocal ) localNextMustBeChecked.clear();
			
			if ( ! offline && ! preLocal ) BooleanArrays.fill( modifiedResultCounter, false );
			
			adaptiveGranularity = granularity;
			if ( numberOfThreads > 1 ) {
				if ( ! local && iteration > 0 ) {
					adaptiveGranularity = (int)Math.min( Math.max( 1, numNodes / numberOfThreads ), granularity * ( numNodes / Math.max( 1., modified() ) ) );
					adaptiveGranularity = ( adaptiveGranularity + Long.SIZE - 1 ) & ~( Long.SIZE - 1 );
				}
				info( "Adaptive granularity for this iteration: " + adaptiveGranularity );
			}
			
			modified.set( 0 );
			totalIoMillis = 0;
			numberOfWrites = 0;
			final ProgressLogger npl = pl == null ? null : new ProgressLogger( LOGGER, ProgressLogger.ONE_MINUTE, "arcs" );
			
			if ( npl != null ) {
				arcs.set( 0 );
				npl.expectedUpdates = systolic || local ? -1 : numArcs;
				npl.start( "Scanning graph..." );
			}

			nodes.set( 0 );
			nextNode.set( 0 );
			unwritten.set( 0 );
			if ( offline ) fileChannel.position( 0 );

			// Start all threads.
			lock.lock();
			try {
				phase = 0;
				//System.err.println( "Starting phase 0..." );
				aliveThreads = numberOfThreads;
				start.signalAll();

				// Wait for all threads to complete their tasks, logging some stuff in the mean time.
				while( aliveThreads != 0 ) {
					allWaiting.await( 1, TimeUnit.MINUTES );
					if ( threadThrowable != null ) throw new RuntimeException( threadThrowable );
					final int aliveThreads = this.aliveThreads;
					if ( npl != null && aliveThreads != 0 ) {
						if ( arcs.longValue() != 0 ) npl.set( arcs.longValue() );
						if ( offline && numberOfWrites > 0 ) {
							final long time = npl.millis();
							info( "Writes: " + numberOfWrites + "; per second: " + Util.format( 1000.0 * numberOfWrites / time ) );
							info( "I/O time: " + Util.format( ( totalIoMillis / 1000.0 ) ) + "s; per write: " + ( totalIoMillis / 1000.0 ) / numberOfWrites + "s" );
						}
						if ( aliveThreads != 0 ) info( "Alive threads: " + aliveThreads + " (" + Util.format( 100.0 * aliveThreads / numberOfThreads ) + "%)" );
					}
				}
			}
			finally {
				lock.unlock();
			}

			if ( npl != null ) {
				npl.done( arcs.longValue() );
				if ( ! offline ) info( "Unwritten counters: " + Util.format( unwritten.intValue() ) + " (" + Util.format( 100.0 * unwritten.intValue() / numNodes ) + "%)" );
				info( "Unmodified counters: " + Util.format( numNodes - modified.intValue() ) + " (" + Util.format( 100.0 * ( numNodes - modified.intValue() ) / numNodes ) + "%)" );
			}
			

			if ( offline ) {
				if ( npl != null ) {
					npl.itemsName = "counters";
					npl.start( "Updating counters..." );
				}

				// Read into memory the newly computed counters.
			
				fileChannel.truncate( fileChannel.position() );
				fileChannel.position( 0 );
				
				// In pre-local mode, we do not clear modified counters.
				if ( ! preLocal ) BooleanArrays.fill( modifiedCounter, false );

				lock.lock();
				try {
					phase = 1;
					//System.err.println( "Starting phase 1..." );
					aliveThreads = numberOfThreads;
					start.signalAll();
					// Wait for all threads to complete the counter update.
					if ( aliveThreads != 0 ) allWaiting.await();
					if ( threadThrowable != null ) throw new RuntimeException( threadThrowable );
				}
				finally {
					lock.unlock();
				}

				if ( npl != null ) {
					npl.count = modified();
					npl.done();
				}
			}
			else {
				// Switch the bit vectors.
				for( int i = 0; i < bitVector.length; i++ ) {
					if ( npl != null ) npl.update( bitVector[ i ].bits().length );
					final LongBigList r = registers[ i ];
					registers[ i ] = resultRegisters[ i ];
					resultRegisters[ i ] = r;
					final LongArrayBitVector v = bitVector[ i ];
					bitVector[ i ] = resultBitVector[ i ];
					resultBitVector[ i ] = v;
					resultBits[ i ] = resultBitVector[ i ].bits();
					bits[ i ] = bitVector[ i ].bits();
				}

				// Switch modifiedCounters and modifiedResultCounters, and fill with zeroes the latter.
				final boolean[] t = modifiedCounter;
				modifiedCounter = modifiedResultCounter;
				modifiedResultCounter = t;
			}
			
			if ( systolic ) {
				// Switch mustBeChecked and nextMustBeChecked, and fill with zeroes the latter.
				final boolean[] t = mustBeChecked;
				mustBeChecked = nextMustBeChecked;
				nextMustBeChecked = t;
			}
			
			lock.lock();
			try {
				phase = 2;
				//System.err.println( "Starting phase 2..." );
				aliveThreads = numberOfThreads;

				if ( npl != null ) {
					npl.expectedUpdates = numNodes;
					npl.itemsName = "nodes";
					npl.start( "Computing harmonic mean..." );
				}

				start.signalAll();
				// Wait for all threads to complete the harmonic partial sum computation.
				if ( aliveThreads != 0 ) allWaiting.await();
				if ( threadThrowable != null ) throw new RuntimeException( threadThrowable );
			}
			finally {
				lock.unlock();
			}

			if ( npl != null ) {
				npl.count = numNodes;
				npl.done();
			}
			
			double result = 0;
			
			for( int i = this.result.length; i-- != 0; ) result += this.result[ i ];
		
			/* We enforce monotonicity. Non-monotonicity can only be caused
			 * by approximation errors. */
			if ( result < last ) result = last; 
			
			if ( pl != null ) {
				pl.updateAndDisplay();
				pl.logger.info( "Pairs: " + result + " (" + 100.0 * result / squareNumNodes + "%)"  );
				pl.logger.info( "Absolute increment: " + ( result - last ) );
				pl.logger.info( "Relative increment: " + ( result / last ) );
			}

			return last = result;
		}
		catch ( InterruptedException e ) {
			throw new RuntimeException( e );
		}
	}

	/** Returns the number of HyperLogLog counters that were modified by the last call to {@link #iterate()}.
	 * 
	 * @return the number of HyperLogLog counters that were modified by the last call to {@link #iterate()}.
	 */
	public int modified() {
		return modified.get();
	}
	
	/** Returns an approximation of the neighbourhood function.
	 * 
	 * @param upperBound an upper bound to the number of iterations.
	 * @param threshold a value that will be used to stop the computation either by relative increment; if you specify -1,
	 * the computation will stop when {@link #modified()} returns false.
	 * @return an approximation of the neighbourhood function.
	 */
	public double[] approximateNeighbourhoodFunction( long upperBound, double threshold ) throws IOException {
		DoubleArrayList approximateNeighbourhoodFunction = new DoubleArrayList();
		upperBound = Math.min( upperBound, numNodes );
		double last;
		approximateNeighbourhoodFunction.add( last = numNodes );

		init();
		
		for( long i = 0; i < upperBound; i++ ) {
			final double current = iterate();

			if ( modified() == 0 ) {
				info( "Terminating approximation after " + i + " iteration(s) by stabilisation" );
				break;
			}
		
			// At this point we want the current value (even if we will stop).
			approximateNeighbourhoodFunction.add( current );

			if ( i > 3 && current / last < ( 1 + threshold ) ) {
				info( "Terminating approximation after " + i + " iteration(s) by relative bound" );
				break;
			}
			
			last = current;
		}

		if ( pl != null ) pl.done();
		return approximateNeighbourhoodFunction.toDoubleArray();
	}
	
	/** Throws a {@link NotSerializableException}, as this class implements {@link Serializable}
	 * because it extends {@link IntHyperLogLogCounterArray}, but it's not really. */
	private void writeObject( @SuppressWarnings("unused") final ObjectOutputStream oos ) throws IOException {
        throw new NotSerializableException();
    }

	/** Combines several approximate neighbourhood functions for the same
	 * graph by averaging their values.
	 * 
	 * @param anf an iterable object returning arrays of doubles representing approximate neighbourhood functions.
	 * @return a combined approximate neighbourhood functions.
	 */
	public static double[] combine( final Iterable<double[]> anf ) {

		final Object[] t = ObjectIterators.unwrap( anf.iterator() );
		final double a[][] = Arrays.copyOf( t, t.length, double[][].class );
		
		final int n = a.length;
		
		int length = 0;
		for( double[] b : a ) length = Math.max( length, b.length );
		final double[] result = new double[ length ];
		
		BigDecimal last = BigDecimal.valueOf( 0 ), curr;
		
		for( int i = 0; i < length; i++ ) {
			curr = BigDecimal.valueOf( 0 );
			for( int j = 0; j < n; j++ ) curr  = curr.add( BigDecimal.valueOf(  a[ j ][ i < a[ j ].length ? i : a[ j ].length - 1 ] ) );
			if ( curr.compareTo( last ) < 0 ) curr = last;
			result[ i ] = curr.doubleValue() / n;
			last = curr;
		}
		
		return result;
	}
	
	
	public static void main( String arg[] ) throws IOException, JSAPException, IllegalArgumentException, ClassNotFoundException, IllegalAccessException, InvocationTargetException, InstantiationException, NoSuchMethodException {
		SimpleJSAP jsap = new SimpleJSAP( HyperApproximateNeighbourhoodFunction.class.getName(), "Prints an approximation of the neighbourhood function.",
			new Parameter[] {
			new FlaggedOption( "log2m", JSAP.INTEGER_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, 'l', "log2m", "The logarithm of the number of registers." ),
			new FlaggedOption( "upperBound", JSAP.LONGSIZE_PARSER, Long.toString( Long.MAX_VALUE ), JSAP.NOT_REQUIRED, 'u', "upper-bound", "An upper bound to the number of iteration." ),
			new FlaggedOption( "threshold", JSAP.DOUBLE_PARSER, "-1", JSAP.NOT_REQUIRED, 't', "threshold", "A threshold that will be used to stop the computation by relative increment. If it is -1, the iteration will stop only when all registers do not change their value (recommended)." ),
			new FlaggedOption( "threads", JSAP.INTSIZE_PARSER, "0", JSAP.NOT_REQUIRED, 'T', "threads", "The number of threads to be used. If 0, the number will be estimated automatically." ),
			new FlaggedOption( "granularity", JSAP.INTSIZE_PARSER, Integer.toString( DEFAULT_GRANULARITY ), JSAP.NOT_REQUIRED, 'g',  "granularity", "The number of node per task in a multicore environment." ),
			new FlaggedOption( "bufferSize", JSAP.INTSIZE_PARSER, Util.formatBinarySize( DEFAULT_BUFFER_SIZE ), JSAP.NOT_REQUIRED, 'b',  "buffer-size", "The size of an I/O buffer in bytes." ),
			new FlaggedOption( "seed", JSAP.LONG_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, 'S', "seed", "The random seed." ),
			new Switch( "spec", 's', "spec", "The source is not a basename but rather a specification of the form <ImmutableGraphImplementation>(arg,arg,...)." ),
			new Switch( "offline", 'o', "offline", "Do not load the graph in main memory. If this option is used, the graph will be loaded in offline (for one thread) or mapped (for several threads) mode." ),
			new Switch( "memory", 'm', "memory", "Use core memory instead of an external file to store new counter values." ),
			new UnflaggedOption( "basename", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.REQUIRED, JSAP.NOT_GREEDY, "The basename of the graph." ),
			new UnflaggedOption( "basenamet", JSAP.STRING_PARSER, JSAP.NO_DEFAULT, JSAP.NOT_REQUIRED, JSAP.NOT_GREEDY, "The basename of the transpose graph for systolic computations. If it is equal to <basename>, the graph will be assumed to be symmetric and will be loaded just once." ),
			}		
		);

		JSAPResult jsapResult = jsap.parse( arg );
		if ( jsap.messagePrinted() ) System.exit( 1 );
		
		final boolean spec = jsapResult.getBoolean( "spec" );
		final boolean memory = jsapResult.getBoolean( "memory" );
		final boolean offline = jsapResult.getBoolean( "offline" );
		final String basename = jsapResult.getString( "basename" );
		final String basenamet = jsapResult.getString( "basenamet" );
		final ProgressLogger pl = new ProgressLogger( LOGGER );
		final int log2m = jsapResult.getInt( "log2m" );
		final int threads = jsapResult.getInt( "threads" );
		final int bufferSize = jsapResult.getInt( "bufferSize" );
		final int granularity = jsapResult.getInt( "granularity" );
		final long seed = jsapResult.userSpecified( "seed" ) ? jsapResult.getLong( "seed" ) : Util.randomSeed();
		
		final ImmutableGraph graph = spec 
				? ObjectParser.fromSpec( basename, ImmutableGraph.class, GraphClassParser.PACKAGE ) 
				: offline	
					? ( ( numberOfThreads( threads ) == 1 && basenamet == null ? ImmutableGraph.loadOffline( basename ) : ImmutableGraph.loadMapped( basename, new ProgressLogger() ) ) ) 
					: ImmutableGraph.load( basename, new ProgressLogger() ); 

		final ImmutableGraph grapht = basenamet == null ? null : basenamet.equals( basename ) ? graph : spec ? ObjectParser.fromSpec( basenamet, ImmutableGraph.class, GraphClassParser.PACKAGE ) : 
			offline ? ImmutableGraph.loadMapped( basenamet, new ProgressLogger() ) : ImmutableGraph.load( basenamet, new ProgressLogger() ); 

		HyperApproximateNeighbourhoodFunction hanf = new HyperApproximateNeighbourhoodFunction( graph, grapht, log2m, pl, threads, bufferSize, granularity, ! memory, seed );
		TextIO.storeDoubles( hanf.approximateNeighbourhoodFunction( jsapResult.getLong( "upperBound" ), jsapResult.getDouble( "threshold" ) ), System.out );
		hanf.close();
	}
}
