package exercise3;

import com.google.common.hash.HashCode;
import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.exercise3.InnerJoinOperation;
import de.hpi.dbs2.exercise3.JoinAttributePair;
import de.hpi.dbs2.exercise3.NestedLoopEquiInnerJoin;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;


@ChosenImplementation(true)
public class HashEquiInnerJoinJava extends InnerJoinOperation {

	public HashEquiInnerJoinJava(
		@NotNull BlockManager blockManager, int leftColumnIndex, int rightColumnIndex
	) {
		super(blockManager, new JoinAttributePair.EquiJoinAttributePair(leftColumnIndex, rightColumnIndex));
	}

	@Override
	public int estimatedIOCost(
		@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation
	) {
		/*
		In Phase 1 (Hashing-Phase) we entirely read the leftInputRelation and the rightInputRelation and additionally
		write the hashed Buckets back to disc.
		In Phase 2 both of the Relations will be read a second time in order to construct joined tuples imparted to the
		next operator.
		 */
		return 3 * (leftInputRelation.estimatedBlockCount() + rightInputRelation.estimatedBlockCount());
	}

	@Override
	public void join(
		@NotNull Relation leftInputRelation, @NotNull Relation rightInputRelation,
		@NotNull Relation outputRelation
	) {
		// decide whether Hash-Join is possible or not
		if (Math.min(leftInputRelation.estimatedBlockCount(), rightInputRelation.estimatedBlockCount()) >= Math.pow(getBlockManager().getFreeBlocks() - 1, 2)) {
			throw new RelationSizeExceedsCapacityException();
		}

		// prepare supporting data structure (HashMap) for Disc-Buckets
		int bucketCount = getBlockManager().getFreeBlocks() - 1;
		HashMap<Integer, List<Block>> bucketsLeftRelation = new HashMap<>();
		HashMap<Integer, List<Block>> bucketsRightRelation = new HashMap<>();
		for (int i = 0; i < bucketCount; i++) {
			bucketsLeftRelation.put(i, new ArrayList<>());
			bucketsRightRelation.put(i, new ArrayList<>());
		}

		// ------------------------------
		// partitioning by use of hashing
		// ------------------------------

		Block RAM[] = new Block[getBlockManager().getFreeBlocks()];
		for (int i = 0; i < RAM.length - 1; i++) {
			RAM[i] = getBlockManager().allocate(true);
		}

		// partitioning phase for leftInputRelation
		for (Block leftBlockRef : leftInputRelation) {
			Block loadedLeftBlock = getBlockManager().load(leftBlockRef);
			for (Tuple tuple : loadedLeftBlock) {
				int hashValue = Math.floorMod(tuple.get(getJoinAttributePair().getLeftColumnIndex()).hashCode(), bucketCount);
				if (RAM[hashValue].isFull()) {
					bucketsLeftRelation.get(hashValue).add(getBlockManager().release(RAM[hashValue], true));
					RAM[hashValue] = getBlockManager().allocate(true);
				}
				RAM[hashValue].append(tuple);
			}
			getBlockManager().release(loadedLeftBlock, false);
		}
		for (int i = 0; i < RAM.length - 1; i++) {
			if (!RAM[i].isEmpty()) {
				bucketsLeftRelation.get(i).add(getBlockManager().release(RAM[i], true));
				RAM[i] = getBlockManager().allocate(true);
			}
		}

		// partitioning phase for rightInputRelation
		for (Block rightBlockRef : rightInputRelation) {
			Block loadedRightBlock = getBlockManager().load(rightBlockRef);
			for (Tuple tuple : loadedRightBlock) {
				int hashValue = Math.floorMod(tuple.get(getJoinAttributePair().getRightColumnIndex()).hashCode(), bucketCount);
				if (RAM[hashValue].isFull()) {
					bucketsRightRelation.get(hashValue).add(getBlockManager().release(RAM[hashValue], true));
					RAM[hashValue] = getBlockManager().allocate(true);
				}
				RAM[hashValue].append(tuple);
			}
			getBlockManager().release(loadedRightBlock, false);
		}
		for (int i = 0; i < RAM.length - 1; i++) {
			if (!RAM[i].isEmpty()) {
				bucketsRightRelation.get(i).add(getBlockManager().release(RAM[i], true));
			} else {
				getBlockManager().release(RAM[i], false);
			}
		}

		// -------------------------------
		// Join Phase
		// -------------------------------

		TupleAppender tupleAppender = new HashEquiInnerJoinJava.TupleAppender(outputRelation.getBlockOutput());
		for (int i = 0; i < bucketCount; i++) {
			List<Block> smallerBucket = ((bucketsLeftRelation.get(i).size() <= bucketsRightRelation.get(i).size()) ? bucketsLeftRelation.get(i) : bucketsRightRelation.get(i));
			List<Block> biggerBucket = ((bucketsLeftRelation.get(i).size() > bucketsRightRelation.get(i).size()) ? bucketsLeftRelation.get(i) : bucketsRightRelation.get(i));

			Boolean swapped = (biggerBucket == bucketsLeftRelation.get(i)) ? true : false;

			// load smaller Bucket for One-pass algorithm
			for (int j = 0; j < smallerBucket.size(); j++) {
				RAM[j] = getBlockManager().load(smallerBucket.get(j));
			}

			for (Block bigBucketBlock : biggerBucket) {
				Block loadedBigBucketBlock = getBlockManager().load(bigBucketBlock);
				for (int k = 0; k < smallerBucket.size(); k++) {
					joinBlocks(
							swapped ? loadedBigBucketBlock : RAM[k],
							swapped ? RAM[k] : loadedBigBucketBlock,
							outputRelation.getColumns(),
							tupleAppender
					);
				}
				getBlockManager().release(loadedBigBucketBlock, false);
			}

			for (int j = 0; j < smallerBucket.size(); j++) {
				getBlockManager().release(RAM[j], false);
			}
		}
		tupleAppender.close();

	}

	class TupleAppender implements AutoCloseable, Consumer<Tuple> {

		BlockOutput blockOutput;

		TupleAppender(BlockOutput blockOutput) {
			this.blockOutput = blockOutput;
		}

		Block outputBlock = getBlockManager().allocate(true);

		@Override
		public void accept(Tuple tuple) {
			if(outputBlock.isFull()) {
				blockOutput.move(outputBlock);
				outputBlock = getBlockManager().allocate(true);
			}
			outputBlock.append(tuple);
		}

		@Override
		public void close() {
			if(!outputBlock.isEmpty()) {
				blockOutput.move(outputBlock);
			} else {
				getBlockManager().release(outputBlock, false);
			}
		}
	}
}
