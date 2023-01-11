package exercise1;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.dbms.*;
import de.hpi.dbs2.dbms.utils.BlockSorter;
import de.hpi.dbs2.exercise1.SortOperation;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.lang.Math;

/*@ChosenImplementation(true)
public class TPMMSJava extends SortOperation {
    public TPMMSJava(@NotNull BlockManager manager, int sortColumnIndex) {
        super(manager, sortColumnIndex);
    }

    @Override
    public int estimatedIOCost(@NotNull Relation relation) {
        throw new UnsupportedOperationException("TODO");
    }

    private void loadNextBlockInMemory(LinkedList<Iterator<Block>> blockIterList, Block[] memory, Iterator<Tuple>[] memoryTupleIter, Tuple[] topTuples, int index) {
        Block currentTopBlock = blockIterList.get(index).next();
        this.getBlockManager().load(currentTopBlock);
        memory[index] = currentTopBlock;

        Iterator<Tuple> tupleIter = currentTopBlock.iterator();
        memoryTupleIter[index] = tupleIter;
        topTuples[index] = tupleIter.next();
    }

    @Override
    public void sort(@NotNull Relation relation, @NotNull BlockOutput output) {
        // handle Exceeding-Capacity Errors
        BlockManager manager = this.getBlockManager();
        int ramCapacity = manager.getFreeBlocks();
        if (relation.getEstimatedSize() > Math.pow(ramCapacity, 2) - ramCapacity) {
            throw new RelationSizeExceedsCapacityException();
        }

        // Phase 1
        Iterator<Block> relationIter = relation.iterator();
        LinkedList<Iterator<Block>> blockIterList = new LinkedList<>();
        while (relationIter.hasNext()) {
            LinkedList<Block> subList = new LinkedList<>();
            while (relationIter.hasNext() && manager.getFreeBlocks() > 0) {
                Block currentRelationBlock = relationIter.next();
                subList.add(currentRelationBlock);
                manager.load(currentRelationBlock);
            }
            BlockSorter.INSTANCE.sort(
                    relation,
                    subList,
                    relation.getColumns().getColumnComparator(getSortColumnIndex())
            );
            Iterator<Block> subListIter = subList.iterator();
            blockIterList.add(subListIter);
            for (Block block : subList) {
                manager.release(block, true);
            }
        }

        // Phase 2
        Block outputBlock = manager.allocate(true);
        Iterator<Tuple>[] memoryTupleIter = new Iterator[blockIterList.size()];
        Tuple[] topTuples = new Tuple[blockIterList.size()];
        Block[] memory = new Block[blockIterList.size()];
        for (int i = 0; i < blockIterList.size(); i++) {
            loadNextBlockInMemory(blockIterList, memory, memoryTupleIter, topTuples, i);
        }

        boolean tuplesEmpty = false;
        while (!tuplesEmpty) {

            // linear Min-Search
            int minTupleIndex = 0;
            Tuple minTuple = null;
            for (int i = 0; i < blockIterList.size(); i++) {
                if (topTuples[i] == null) {
                    continue;
                } else if (minTuple == null) {
                    minTuple = topTuples[i];
                    minTupleIndex = i;
                } else if (relation.getColumns().getColumnComparator(getSortColumnIndex()).compare(topTuples[i], minTuple) < 0) {
                    minTuple = topTuples[i];
                    minTupleIndex = i;
                }
            }

            // write to Output-Block (if necessary transfer to output whenever Output-Block is full)
            if (!outputBlock.isFull()) {
                outputBlock.append(minTuple);
            } else {
                output.output(outputBlock);
                outputBlock.append(minTuple);
            }

            // load next Tuple/Block for next linear Min-Search
            if (!memoryTupleIter[minTupleIndex].hasNext()) {
                manager.release(memory[minTupleIndex], false);
                if (!blockIterList.get(minTupleIndex).hasNext()) {
                    topTuples[minTupleIndex] = null;
                } else {
                    loadNextBlockInMemory(blockIterList, memory, memoryTupleIter, topTuples, minTupleIndex);
                }
            } else {
                topTuples[minTupleIndex] = memoryTupleIter[minTupleIndex].next();
            }

            // verify if there's a remnant Tuple that needs to be written into output
            tuplesEmpty = true;
            for (Tuple tuple : topTuples) {
                if (tuple != null) {
                    tuplesEmpty = false;
                    break;
                }
            }
        }

        output.output(outputBlock);
        manager.release(outputBlock, false);
    }
}*/

