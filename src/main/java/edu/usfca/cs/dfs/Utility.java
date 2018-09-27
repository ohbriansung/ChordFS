package edu.usfca.cs.dfs;

class Utility {
    private final int capacity;

    Utility(int capacity) {
        this.capacity = capacity;
    }

    /**
     * The result of XOR two same integer will be 0.
     *
     * @param target
     * @param left
     * @param right
     * @return boolean - in the range
     */
    boolean in(int target, int left, int right) {
        if (left < right) {
            return target > left && target < right;
        }
        else if (left > right) {
            // passing zero
            return (target > left && target <= this.capacity) || (target < right && target >= 0);
        }
        else {
            // left == right
            return !((target ^ left) == 0b0);
        }
    }

    boolean includesLeft(int target, int left, int right) {
        return (target ^ left) == 0b0 || in(target, left, right);
    }

    boolean includesRight(int target, int left, int right) {
        return (target ^ right) == 0b0 || in(target, left, right);
    }

    int genId() {
        return Math.abs(Long.hashCode(System.currentTimeMillis()) % this.capacity);
    }

    int start(int id, int i) {
        return (id + (0b1 << i)) % this.capacity;
    }
}
