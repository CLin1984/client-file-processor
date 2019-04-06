package com.test.clientfileprocessor;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.assertj.core.api.Assertions;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.LinkedList;
import java.util.stream.Collectors;

@Slf4j
public class ClientFileReaderIntegrationTest {
    private final String inputFilePath = "src/integration-test/resources/Sample-Inputs.txt";
    private final int maxNumRowsInMemoryPerInstant = 10;
    private static final LinkedList<String> EXPECTED_OUTPUT_FILE_ENTIRES = getExpectedOutput();

    @Test
    public void shouldCreateOutputFileFromInputFile() throws IOException {
        String outputFilePath = "test-output.txt";
        Pair<File, ClientFileReader> testInputs = getInputsForOutputFilePath(outputFilePath);
        File outputFile = testInputs.getLeft();
        testInputs.getRight().sortInputFileAndOutput();
        Assertions.assertThat(outputFile).isNotNull().exists().isFile();
        Assertions.assertThat(getOutputFileLines(outputFile)).hasSize(4).isEqualTo(EXPECTED_OUTPUT_FILE_ENTIRES);
    }

    @Test
    public void shouldCreateAppendixedOutputFiles() throws IOException {
        String outputFilePath = "appendix-creation-test-output.txt";
        Pair<File, ClientFileReader> testInputs = getInputsForOutputFilePath(outputFilePath);
        File outputFile = testInputs.getLeft();
        ClientFileReader clientFileReader = testInputs.getRight();
        outputFile.createNewFile();
        File generatedFile0 = clientFileReader.getNewOutputFile(outputFilePath, 1);
        log.info("Generated File Original: {}", outputFile);
        log.info("Generated File 0: {}", generatedFile0);
        Assertions.assertThat(outputFile).isNotNull().isFile();
        Assertions.assertThat(generatedFile0).isNotNull().hasName("appendix-creation-test-output-0.txt");
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowMaxRetriesException() throws IOException {
        String outputFilePath = "appendix-exception-test-output.txt";
        Pair<File, ClientFileReader> testInputs = getInputsForOutputFilePath(outputFilePath);
        ClientFileReader clientFileReader = testInputs.getRight();
        testInputs.getLeft().createNewFile();
        clientFileReader.getNewOutputFile(outputFilePath, 0);
    }

    private LinkedList<String> getOutputFileLines(File outputFile ) throws IOException {
        return Lists.newLinkedList(Files.lines(outputFile.toPath()).collect(Collectors.toList()));
    }

    private static LinkedList<String> getExpectedOutput(){
        LinkedList<String> expectedValues =  Lists.newLinkedList();
        expectedValues.add("Alpha, Beta\t1\tNot-A-Real-Number");
        expectedValues.add("Doe, Jane\t31\t112-112-1122");
        expectedValues.add("Doe, John\t22\t112-323-2221");
        expectedValues.add("Freeman, Morgan\t60\t222-111-3212");
        return expectedValues;
    }

    private Pair<File, ClientFileReader> getInputsForOutputFilePath(String outputFilePath) {
        File outputFile = new File(outputFilePath);
        outputFile.deleteOnExit();
        ClientFileReader clientFileReader = new ClientFileReader(inputFilePath, outputFilePath, maxNumRowsInMemoryPerInstant);
        return Pair.of(outputFile, clientFileReader);
    }
}