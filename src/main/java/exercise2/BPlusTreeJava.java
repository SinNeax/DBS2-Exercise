package exercise2;

import de.hpi.dbs2.ChosenImplementation;
import de.hpi.dbs2.exercise2.*;
import kotlin.Pair;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;
import java.util.Stack;

/**
 * This is the B+-Tree implementation you will work on.
 * Your task is to implement the insert-operation.
 *
 */
@ChosenImplementation(true)
public class BPlusTreeJava extends AbstractBPlusTree {
    public BPlusTreeJava(int order) {
        super(order);
    }

    public BPlusTreeJava(BPlusTreeNode<?> rootNode) {
        super(rootNode);
    }

    public void insertIntoInnerNode(Stack<InnerNode> nodeStack, InnerNode node, Integer key, BPlusTreeNode reference) {

        // innerNode has enough space to insert key-reference pair
        if (!node.isFull()) {
            // search index where to insert new key/value
            Integer index = 0;
            while (node.keys[index] < key) {
                index++;
                // dodge ValueCantBeComparedWithNull error
                if (node.keys[index] == null) {
                    node.keys[index] = key;
                    node.references[index + 1] = reference;
                    return;
                }
            }
            // shift all keys/references on the right of the key's/reference's index to the right
            for (int i = node.n - 1; i > index; i--) {
                node.keys[i] = node.keys[i - 1];
                node.references[i + 1] = node.references[i];
            }
            node.keys[index] = key;
            node.references[index + 1] = reference;
        }
        // innerNode is full -> split
        else {
            // create Pair Array consisting of keys with their right reference
            Pair<Integer, BPlusTreeNode>[] keyNodePairs = new Pair[node.n + 1];
            Integer index = 0;
            while (node.keys[index] < key) {
                keyNodePairs[index] = new Pair(node.keys[index], node.references[index + 1]);
                index++;
                // dodge OutOfBoundsError by breaking the while loop when reaching the end of the keys array
                if (index == node.keys.length) {
                    break;
                }
            }
            keyNodePairs[index] = new Pair(key, reference);
            for (; index < node.n; index++) {
                keyNodePairs[index + 1] = new Pair(node.keys[index], node.references[index + 1]);
            }

            Integer splitIndex = (int) Math.ceil(keyNodePairs.length / 2);
            Pair<Integer, BPlusTreeNode>[] leftNodeEntries = Arrays.copyOfRange(keyNodePairs, 0, splitIndex);
            Pair<Integer, BPlusTreeNode>[] rightNodeEntries = Arrays.copyOfRange(keyNodePairs, splitIndex, keyNodePairs.length);

            // reset left-split InnerNode (= N1) except for references[0] in order to keep InnerNode as the N1 node
            for (int i = 0; i < node.n; i++) {
                node.keys[i] = null;
                node.references[i + 1] = null;
            }
            // copy elements into N1
            BPlusTreeNode N2firstReference = null;
            for (int i = 0; i < leftNodeEntries.length; i++) {
                node.keys[i] = leftNodeEntries[i].getFirst();
                if (i == leftNodeEntries.length - 1) {
                    N2firstReference = leftNodeEntries[i].getSecond();
                }
                else {
                    node.references[i + 1] = leftNodeEntries[i].getSecond();
                }
            }

            // create N2 while filling in the same breath
            InnerNode n2 = new InnerNode(node.order);
            n2.references[0] = N2firstReference;
            for (int i = 0; i < rightNodeEntries.length; i++) {
                n2.keys[i] = rightNodeEntries[i].getFirst();
                n2.references[i + 1] = rightNodeEntries[i].getSecond();
            }

            // create new Root with attaching respective reference-nodes
            if (node == this.getRootNode()) {
                InnerNode newRoot = new InnerNode(node.order);
                newRoot.keys[0] = node.keys[splitIndex - 1];
                node.keys[splitIndex - 1] = null;
                newRoot.references[0] = node;
                newRoot.references[1] = n2;
                this.rootNode = newRoot;
            } else {
                Integer keyToBeLifted = node.keys[splitIndex - 1];
                node.keys[splitIndex - 1] = null;
                if (!nodeStack.isEmpty()) {
                    InnerNode parentNode = nodeStack.pop();
                    insertIntoInnerNode(nodeStack, parentNode, keyToBeLifted, n2);
                }
            }
        }
    }

    public ValueReference insertIntoLeafNode(Stack<InnerNode> nodeStack, LeafNode leaf, Integer key, ValueReference value) {

        // overwrite key
        for (int i = 0; i < leaf.n; i++) {
            if (leaf.keys[i] == key) {
                ValueReference oldValue = leaf.references[i];
                leaf.references[i] = value;
                return oldValue;
            }
        }

        // leaf has enough space for inserting new key/value
        if (!leaf.isFull()) {
            if (leaf.isEmpty()) {
                leaf.keys[0] = key;
                leaf.references[0] = value;
                return null;
            } else {
                // search index where to insert new key/value
                Integer index = 0;
                while (leaf.keys[index] < key) {
                    index++;
                    // dodge ValueCantBeComparedWithNull error
                    if (leaf.keys[index] == null) {
                        leaf.keys[index] = key;
                        leaf.references[index] = value;
                        return null;
                    }
                }
                // shift all keys/values on the right of the key's/value's index to the right
                for (int i = leaf.n - 1; i > index; i--) {
                    leaf.keys[i] = leaf.keys[i - 1];
                    leaf.references[i] = leaf.references[i - 1];
                }
                leaf.keys[index] = key;
                leaf.references[index] = value;
                return null;
            }
        }
        // leaf node is full -> split leaf
        else {
            // create Entry Array for easier creation process of LeafNodes
            Entry[] toSeparatedEntries = new Entry[leaf.n + 1];
            Integer index = 0;
            while (leaf.keys[index] < key) {
                toSeparatedEntries[index] = new Entry(leaf.keys[index], leaf.references[index]);
                index++;
                // dodge OutOfBoundsError by breaking the while loop when reaching the end of the keys array
                if (index == leaf.keys.length) {
                    break;
                }
            }
            toSeparatedEntries[index] = new Entry(key, value);
            for (; index < leaf.n; index++) {
                toSeparatedEntries[index + 1] = new Entry(leaf.keys[index], leaf.references[index]);
            }

            Integer splitIndex = (int) Math.ceil(toSeparatedEntries.length / 2);
            Entry[] leftLeafEntries = Arrays.copyOfRange(toSeparatedEntries, 0, splitIndex);
            Entry[] rightLeafEntries = Arrays.copyOfRange(toSeparatedEntries, splitIndex, toSeparatedEntries.length);

            // reset left-split leafNode (= N1) in order to keep leafNode as the N1 node and thus, easily set nextSiblings
            for (int i = 0; i < leaf.n; i++) {
                leaf.keys[i] = null;
                leaf.references[i] = null;
            }
            // copy elements into N1
            for (int i = 0; i < leftLeafEntries.length; i++) {
                leaf.keys[i] = leftLeafEntries[i].getKey();
                leaf.references[i] = leftLeafEntries[i].getValue();
            }

            // create N2 with rightLeafEntries
            LeafNode n2 = new LeafNode(leaf.order, rightLeafEntries);
            n2.nextSibling = leaf.nextSibling;
            leaf.nextSibling = n2;

            // create new root because of InitialRootNode-instance of leaf
            if (leaf instanceof InitialRootNode) {
                LeafNode n1 = new LeafNode(leaf.order, leftLeafEntries);
                n1.nextSibling = n2;
                InnerNode newRootNode = new InnerNode(this.order, n1, n2);
                this.rootNode = newRootNode;
            } else {
                Integer smallestKey = n2.getSmallestKey();
                InnerNode parentNode = nodeStack.pop();
                insertIntoInnerNode(nodeStack, parentNode, smallestKey, n2);
            }
        }
        return null;
    }

    public Stack<InnerNode> createNodeStack(InnerNode node, Integer insertKey) {
        Stack<InnerNode> pathStack = new Stack<>();
        while (node.getHeight() > 0) {
            // cast current node for selecting child
            InnerNode parentNode = node;
            pathStack.push(parentNode);
            if (parentNode.selectChild(insertKey) instanceof LeafNode) {
                break;
            }
            parentNode = (InnerNode) parentNode.selectChild(insertKey);
            node = parentNode;
        }
        return pathStack;
    }

    @Nullable
    @Override
    public ValueReference insert(@NotNull Integer key, @NotNull ValueReference value) {

        BPlusTreeNode currentNode = this.getRootNode();
        Stack<InnerNode> nodeStack = new Stack<>();
        if (!(currentNode instanceof InitialRootNode)) {
            nodeStack = createNodeStack((InnerNode) currentNode, key);
            currentNode = nodeStack.peek().selectChild(key);
        }
        ValueReference returnValue = insertIntoLeafNode(nodeStack, (LeafNode) currentNode, key, value);
        return returnValue;

        // Find LeafNode in which the key has to be inserted.
        //   It is a good idea to track the "path" to the LeafNode in a Stack or something alike.
        // Does the key already exist? Overwrite!
        //   leafNode.references[pos] = value;
        //   But remember return the old value!
        // New key - Is there still space?
        //   leafNode.keys[pos] = key;
        //   leafNode.references[pos] = value;
        //   Don't forget to update the parent keys and so on...
        // Otherwise
        //   Split the LeafNode in two!
        //   Is parent node root?
        //     update rootNode = ... // will have only one key
        //   Was node instanceof LeafNode?
        //     update parentNode.keys[?] = ...
        //   Don't forget to update the parent keys and so on...

        // Check out the exercise slides for a flow chart of this logic.
        // If you feel stuck, try to draw what you want to do and
        // check out Ex2Main for playing around with the tree by e.g. printing or debugging it.
        // Also check out all the methods on BPlusTreeNode and how they are implemented or
        // the tests in BPlusTreeNodeTests and BPlusTreeTests!
    }
}
