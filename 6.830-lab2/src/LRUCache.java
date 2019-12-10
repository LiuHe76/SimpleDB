import java.util.*;

class LRUCache {
    private int capacity;
    private int num;
    private Map<Integer, Node> info;
    private Node sentinel;

    private class Node {
        private Integer val;
        private Integer key;
        private Node pre, next;

        public Node() {}

        public Node(Integer val, Integer key) {
            this.val = val;
            this.key = key;
        }
    }

    public LRUCache(int capacity) {
        this.capacity = capacity;
        info = new HashMap<>();
        sentinel = new Node();
        sentinel.next = sentinel;
        sentinel.pre = sentinel;
    }

    public int get(int key) {
        if (!info.containsKey(key)) return -1;
        Node targetNode = info.get(key);
        addToLast(targetNode, false);
        return targetNode.val;
    }

    public void put(int key, int value) {
        if (info.containsKey(key)) {
            Node n = info.get(key);
            n.val = value;
            info.put(key, n);
            addToLast(n, false);
        } else {
            if (num == capacity) {
                removeFirst();
            }
            Node n = new Node(value, key);
            addToLast(n, true);
            info.put(key, n);
            num += 1;
        }
    }

    private void addToLast(Node node, boolean isNew) {
        if (!isNew) {
            node.next.pre = node.pre;
            node.pre.next = node.next;
        }
        node.pre = sentinel.pre;
        sentinel.pre.next = node;
        sentinel.pre = node;
        node.next = sentinel;
    }

    private void removeFirst() {
        Node first = sentinel.next;
        sentinel.next.next.pre = sentinel;
        sentinel.next = sentinel.next.next;
        num -= 1;
        info.remove(first.key);
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        Node n = sentinel.next;
        while (n != sentinel) {
            sb.append(n.key).append(" ");
            n = n.next;
        }
        return sb.toString();
    }

    public static void main(String[] args) {
//        ["LRUCache","put","put","put","put","get","get"]
//[[2],[2,1],[1,1],[2,3],[4,1],[1],[2]]

        LRUCache lruCache = new LRUCache(2);
        lruCache.put(2, 1);
        System.out.println(lruCache);
        lruCache.put(1, 1);
        System.out.println(lruCache);
        lruCache.put(2, 3);
        System.out.println(lruCache);
        lruCache.put(4, 1);
        System.out.println(lruCache);
        int i = lruCache.get(1);
        i = lruCache.get(2);
    }
}


