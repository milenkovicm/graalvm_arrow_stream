package com.github.milenkovicm.gas;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.ipc.message.ArrowRecordBatch;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.IOException;

import static java.util.Arrays.asList;
/*
* This reader should be replaced with a real implementation.
* Once the real implementation is in place, Graal packaging should be adjusted.
*/
class MockReader extends ArrowReader {
    protected MockReader(BufferAllocator allocator) {
        super(allocator);
    }

    @Override
    public boolean loadNextBatch() throws IOException {
        super.ensureInitialized();
        super.loadRecordBatch(mockRecordBatch());
        return true;
    }

    @Override
    public long bytesRead() {
        return 0;
    }

    @Override
    protected void closeReadSource() throws IOException {
        super.allocator.close();
    }

    @Override
    protected Schema readSchema() throws IOException {
        return mockRecordSchema();
    }

    protected ArrowRecordBatch mockRecordBatch() {

        try(
                VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(mockRecordSchema(), this.allocator)
        ) {
            VarCharVector nameVector = (VarCharVector) vectorSchemaRoot.getVector("name");
            nameVector.allocateNew(3);
            nameVector.set(0, "David".getBytes());
            nameVector.set(1, "Peter".getBytes());
            nameVector.set(2, "Mike".getBytes());
            IntVector ageVector = (IntVector) vectorSchemaRoot.getVector("age");
            ageVector.allocateNew(3);
            ageVector.set(0, 41);
            ageVector.set(1, 52);
            ageVector.set(2, 63);
            vectorSchemaRoot.setRowCount(3);

            VectorUnloader unloader = new VectorUnloader(vectorSchemaRoot);
            var batch = unloader.getRecordBatch();
            //System.out.println("----> "+ batch);
            return batch;
        }


    }

    static Schema mockRecordSchema() {
        Field name = new Field("name", FieldType.nullable(new ArrowType.Utf8()), null);
        Field age = new Field("age", FieldType.nullable(new ArrowType.Int(32, true)), null);
        return new Schema(asList(name, age));
    }
}
