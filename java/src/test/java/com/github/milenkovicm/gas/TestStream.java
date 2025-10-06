package com.github.milenkovicm.gas;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.Test;

public class TestStream {
    static final BufferAllocator allocator = new RootAllocator();
    @Test
    public void testStream() throws Exception{

        var stream = ArrowArrayStream.allocateNew(allocator);
        var array = ArrowArray.allocateNew(allocator);

        LibJvmStream.readStream("", stream.memoryAddress());
        stream.getNext(array);
        System.out.println(array);

        array.release();
        stream.close();
    }
}
