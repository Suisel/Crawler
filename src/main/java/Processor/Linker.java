package Processor;

public class Linker {
    private String seed;
    private String linkPath;
    private int amount;

    public Linker() {
    }

    public Linker(String seed, String linkPath, int amount) {
        this.seed = seed;
        this.linkPath = linkPath;
        this.amount = amount;
    }

    public String getSeed() {
        return seed;
    }

    public void setSeed(String seed) {
        this.seed = seed;
    }

    public String getLinkPath() {
        return linkPath;
    }

    public void setLinkPath(String linkPath) {
        this.linkPath = linkPath;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }
}

