package edu.usfca.cs.dfs;

import java.io.File;
import java.math.BigInteger;

public class Utility {
    private final int capacity;

    public Utility(int capacity) {
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
    public boolean in(int target, int left, int right) {
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

    public boolean includesLeft(int target, int left, int right) {
        return (target ^ left) == 0b0 || in(target, left, right);
    }

    public boolean includesRight(int target, int left, int right) {
        return (target ^ right) == 0b0 || in(target, left, right);
    }

    public int genId() {
        return Math.abs(Long.hashCode(System.currentTimeMillis()) % this.capacity);
    }

    public int start(int id, int i) {
        return (id + (0b1 << i)) % this.capacity;
    }

    public int getKey(BigInteger hash) {
        BigInteger rem = hash.remainder(new BigInteger(String.valueOf(this.capacity)));
        return rem.intValue();
    }

    public String getFreeSpace(String volume) {
        String unit = " bytes";
        double space = new File(volume).getFreeSpace();

        while (space >= 1024) {
            switch (unit) {
                case " bytes":
                    unit = " kb";
                    break;
                case " kb":
                    unit = " mb";
                    break;
                case " mb":
                    unit = " gb";
                    break;
                case " gb":
                    unit = " tb";
            }

            space /= 1024;
        }

        return String.valueOf(space) + unit;
    }
}
