import java.util.ArrayList;

public class Test {
    public static void main(String[] args) {
        DiGraph diGraph = new DiGraph(4);
        diGraph.addEdge(0, 1);
        diGraph.addEdge(3, 2);
        diGraph.addEdge(3, 1);
        diGraph.addEdge(1, 2);

        CycleDetection cycleDetection = new CycleDetection(diGraph);
        System.out.println(cycleDetection.hasCycle());
    }
}


class CycleDetection {
    private DiGraph dig;
    private boolean hascycle;
    private boolean[] onStack;
    private boolean[] marked;

    public CycleDetection(DiGraph dig) {
        this.dig = dig;
        onStack = new boolean[dig.V()];
        marked = new boolean[dig.V()];

        for (int i = 0; i < dig.V(); i += 1) {
            if (!marked[i]) {
                hascycle = dfs(i);
                if (hascycle) break;
            }
        }

    }

    public boolean hasCycle() {
        return hascycle;
    }

    private boolean dfs(int i) {
        onStack[i] = true;
        marked[i] = true;

        for (int j : dig.adj(i)) {
            if (marked[j]) {
                if (onStack[j]) {
                    return true;
                }
            } else {
                if (dfs(j)) {
                    return true;
                }
            }
        }
        onStack[i] = false;
        return false;
    }
}

class DiGraph {
    private int V;
    private ArrayList<Integer>[] edges;

    public DiGraph(int V) {
        this.V = V;
        edges = (ArrayList<Integer>[]) new ArrayList[V];
        for (int i = 0; i < V; i += 1) {
            edges[i] = new ArrayList<>();
        }
    }

    public void addEdge(int i, int j) {
        edges[i].add(j);
    }

    public Iterable<Integer> adj(int i) {
        return edges[i];
    }

    public int V() {
        return V;
    }
}