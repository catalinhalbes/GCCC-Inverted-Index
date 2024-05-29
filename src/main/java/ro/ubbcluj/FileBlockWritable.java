package ro.ubbcluj;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// defining own index over file/block
public class FileBlockWritable implements WritableComparable<FileBlockWritable> {
    private final Text file = new Text();
    private long block;

    public void setFile(String file) {
        this.file.set(file);
    }

    public void setBlock(long block) {
        this.block = block;
    }

    public void write(DataOutput out) throws IOException {
        out.writeLong(block);
        out.writeInt(file.getLength());
        out.write(file.getBytes());
    }

    public void readFields(DataInput in) throws IOException {
        block = in.readLong();
        int length = in.readInt();
        byte[] data = new byte[length];
        in.readFully(data, 0, length);
        String filename = new String(data);
        file.set(filename);
    }

    public int compareTo(FileBlockWritable o) {
        int firstResult = this.file.compareTo(o.file);
        if (firstResult != 0)
            return firstResult;
        return Long.compare(this.block, o.block);
    }

    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + file.hashCode();
        result = prime * result + (int) (block ^ (block >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return file.toString() + "," + block;
    }
}
