package com.test.clientfileprocessor;

import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.util.Optional;

public class ClientFileReaderTest {
    private final String inputFilePath = "/test-dir/input.txt";
    private final int maxNumRowsInMemoryPerInstant = 0;
    private final String outputFilePath = "/test-dir/temp.txt";
    private final ClientFileReader clientFileReader = new ClientFileReader(inputFilePath, outputFilePath,
            maxNumRowsInMemoryPerInstant);

    @Test
    public void shouldGetFileNameElements(){
        Pair<String, Optional<String>> expectedResults = Pair.of("/test-dir/temp", Optional.of("txt"));
        Assertions.assertThat(clientFileReader.getFileNameElements(outputFilePath)).isNotNull().isEqualTo(expectedResults);
    }
}
