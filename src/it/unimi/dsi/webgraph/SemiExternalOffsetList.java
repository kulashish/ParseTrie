package it.unimi.dsi.webgraph;


/*		 
 * MG4J: Managing Gigabytes for Java
 *
 * Copyright (C) 2006-2011 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */
import it.unimi.dsi.fastutil.longs.AbstractLongBigList;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;

import java.io.IOException;

/** Provides semi-external random access to offsets of a {@link BVGraph}. 
 * 
 * <p>This class is a semi-external {@link it.unimi.dsi.fastutil.longs.LongList} that
 * is used to access graph streams when an {@link EliasFanoMonotoneLongBigList}
 * is not available.
 *  
 * <p>This class accesses offsets in their
 * compressed forms, and provides entry points for random access to each offset. At construction
 * time, entry points are computed with a certain <em>step</em>, which is the number of offsets
 * accessible from each entry point, or, equivalently, the maximum number of offsets that will
 * be necessary to read to access a given offset.
 *
 * <p><strong>Warning:</strong> This class is not thread safe, and needs to be synchronised to be used in a
 * multithreaded environment. 
 *
 * @author Fabien Campagne
 * @author Sebastiano Vigna
 */
public class SemiExternalOffsetList extends AbstractLongBigList {
	/** Position in the offset stream for each random access entry point (one each {@link #offsetStep} elements). */
	private final long[] position;
	/** An array parallel to {@link #position} recording the value of the offset for each random access entry point. */
	private final long[] startValue;
	/** Stream over the compressed offset information. */
	private final InputBitStream ibs;
	/** Maximum number of times {@link InputBitStream#readLongGamma()} will be called to access an offset. */
	private final int offsetStep;
	/** The number of offsets. */
	private final long numOffsets;

	/** Creates a new semi-external list.
	 * 
	 * @param offsetRawData a bit stream containing the offsets in compressed form (&gamma;-encoded deltas).
	 * @param offsetStep the step used to build random-access entry points.
	 * @param numOffsets the overall number of offsets (i.e., the number of terms).
	 */

	public SemiExternalOffsetList( final InputBitStream offsetRawData, final int offsetStep, final long numOffsets ) throws IOException {
		int slots = (int)( ( numOffsets + offsetStep - 1 ) / offsetStep );
		this.position = new long[ slots ];
		this.startValue = new long[ slots ];
		this.offsetStep = offsetStep;
		this.numOffsets = numOffsets;
		this.ibs = offsetRawData;
		prepareRandomAccess( numOffsets );
	}

	/** Scans {@link #ibs} and fills the necessary data in {@link #position} and {@link #startValue}.
	 * 
	 * @param numOffsets the number of offsets.
	 */
	
	private void prepareRandomAccess( final long numOffsets ) throws IOException {
		long offset = 0;
		ibs.position( 0 );
		
		int k = 0;
		int slotIndex = 0;
		
		for ( long i = numOffsets; i-- != 0; ) {
			offset += ibs.readLongGamma();

			if ( k-- == 0 ) {
				k = offsetStep - 1;

				startValue[ slotIndex ] = offset;
				position[ slotIndex ] = ibs.readBits();
				slotIndex++;
			}
		}
	}

	public synchronized final long getLong( final long index ) {
		if ( index < 0 ) throw new IndexOutOfBoundsException();
		final int slotNumber = (int)( index / offsetStep );
		final int k = (int)( index % offsetStep );
		if ( k == 0 ) {
			// exact match to an index in startValue:
			return startValue[ slotNumber ];
		}
		else {
			try {
				long value = startValue[ slotNumber ];
				ibs.position( position[ slotNumber ] );
				for ( int i = k; i-- != 0; ) {
					final long diff = ibs.readLongGamma();
					// System.out.println("diff: " + diff);
					value += diff;
				}
				return value;
			}
			catch( IOException e ) {
				throw new RuntimeException( e );
			}
		}
	}
	
	@Deprecated
	public long length() {
		return size64();
	}

	public long size64() {
		return numOffsets;
	}
}
