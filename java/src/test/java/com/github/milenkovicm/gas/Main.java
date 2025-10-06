package com.github.milenkovicm.gas;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import java.io.IOException;
// --add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED -agentlib:native-image-agent=config-output-dir=target/native-image-config
public class Main {
    static final BufferAllocator allocator = new RootAllocator();

    static void main (String[] args) throws IOException {
        var stream = ArrowArrayStream.allocateNew(allocator);
        var array = ArrowArray.allocateNew(allocator);

        LibJvmStream.readStream("", stream.memoryAddress());

        stream.getNext(array);
        System.out.println(array);

        array.release();
        stream.close();

    }
}
