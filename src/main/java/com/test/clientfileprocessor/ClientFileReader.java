package com.test.clientfileprocessor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
public class ClientFileReader {
    private final String inputFilePath;
    private final File inputFile;
    private final String outputFilePath;
    private final int maxRowsProcessedPerMoment;
    private static final String TEMPORARY_FILE_REPO = "tmpFileRepo";
    private static final String TEMPORARY_FILE_EXTENSION = ".txt";

    public ClientFileReader(String inputFilePath, String outputFilePath, int maxRowsProcessedPerMoment) {
        this.inputFilePath = inputFilePath;
        this.outputFilePath = outputFilePath;
        this.maxRowsProcessedPerMoment = maxRowsProcessedPerMoment;
        this.inputFile = new File(inputFilePath);
    }

    public void sortInputFileAndOutput() throws IOException {
        writeInputToTemporaryFiles();
        writeTopEntriesToOutputFile();
    }

    private void writeInputToTemporaryFiles() throws IOException {
        writeInputToOutputFile(inputFile, Optional.empty());
    }

    private void writeInputToOutputFile(File inputFile, Optional<File> outputFileOpt) throws IOException {
        try (Stream<String> stream = Files.lines(inputFile.toPath())) {
            stream.sorted().forEach(line -> {
                try {
                    if ( line != null && !line.isEmpty() ) {
                        writeLineToFile(outputFileOpt.orElse(getTemporaryFile(line)), line);
                    }
                }
                catch (IOException e) {
                    log.error("", e);
                }
            });
        }
    }

    private void writeTopEntriesToOutputFile() throws IOException {
        List<File> fileList = Lists.newArrayList(createTemporaryFileRepoIfNotExists().listFiles());
        File outputFile = getNewOutputFile(outputFilePath, 10);
        fileList.stream().sorted().forEachOrdered( lastNameFile -> {
            try {
                log.info("Processing file: {}", lastNameFile);
                writeInputToOutputFile(lastNameFile, Optional.of(outputFile));
            } catch (IOException e) {
                log.error("", e);
            }
        });
    }

    @VisibleForTesting
    File getNewOutputFile(String outputFileName, int maxRetries) throws IOException {
        int outputFileCollisionCounter = 0;
        File outputFile = new File(outputFileName);
        Pair<String, Optional<String>> fileNameAndExtension = getFileNameElements(outputFileName);
        String outputFileNameForFormatting =
                fileNameAndExtension.getLeft() + "-%s" + ((fileNameAndExtension.getRight().isPresent())
                ? ("." + fileNameAndExtension.getRight().get()) : "");
        while ( outputFile.exists() ) {
            String newFileName = String.format(outputFileNameForFormatting, outputFileCollisionCounter++);
            outputFile = new File(newFileName);
            log.info("Assigning new file name: {}", newFileName);
            if ( outputFileCollisionCounter > maxRetries ) {
                throw new IllegalStateException(String.format("File: %s exists. Attempted to create file appended " +
                        "with -# %s times but those files also existed. Please declare a new file name.", outputFileName,
                        maxRetries));
            }
        }
        return outputFile;
    }

    @VisibleForTesting
    Pair<String, Optional<String>> getFileNameElements(String fileName) {
        String filePathSeperator = "\\.";
        List<String> fileNameElements = Arrays.asList(fileName.split(filePathSeperator)).stream()
                .collect(Collectors.toList());
        if ( fileNameElements.size() > 1) {
            int lastElementIndex = fileNameElements.size() - 1;
            String filePath = StringUtils.join(fileNameElements.subList(0, lastElementIndex), filePathSeperator);
            String extension = fileNameElements.get(lastElementIndex);
            return Pair.of(filePath, Optional.of(extension));
        }
        else {
            log.warn("No extension found for file: {}. This is not required but not advised.", fileName);
            return Pair.of(fileName, Optional.empty());
        }
    }

    private File getTemporaryFile(String line) throws IOException {
        String lastName = getLastNameFromFile(line);
        String fileRepoPath = createTemporaryFileRepoIfNotExists().getAbsolutePath();
        File lastNameTemporaryFile = new File(getLastNameFile(lastName, fileRepoPath));
        if ( !lastNameTemporaryFile.exists() ) {
            lastNameTemporaryFile.createNewFile();
            lastNameTemporaryFile.deleteOnExit();
        }
        return lastNameTemporaryFile;
    }

    private void writeLineToFile(File temporaryLastNameFile, String line) throws IOException {
        log.info("Writing to file: {} line: {}", temporaryLastNameFile.getName(), line);
        FileOutputStream fos = new FileOutputStream(temporaryLastNameFile, true);

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        bw.write(line);
        bw.newLine();

        bw.flush();
        bw.close();
    }

    private String getLastNameFile(String lastName, String repoPath) {
        return repoPath + File.separator + lastName + TEMPORARY_FILE_EXTENSION;
    }

    private File createTemporaryFileRepoIfNotExists() throws IOException {
        File tmpFileRepo = new File(TEMPORARY_FILE_REPO);
        tmpFileRepo.mkdirs();
        tmpFileRepo.deleteOnExit();
        return tmpFileRepo;
    }

    private String getLastNameFromFile(String line) {
        String[] nameAgePhoneNumElements = line.split("\t");
        if ( nameAgePhoneNumElements.length == 3) {
            String name = nameAgePhoneNumElements[0];
            String[] lastFirstNameElements = name.split(", ");
            // Handle possible middle Name
            if ( lastFirstNameElements.length == 2 || lastFirstNameElements.length == 3) {
                return lastFirstNameElements[0];
            }
            else {
                throw new IllegalArgumentException(String.format("Name Entry: %s is not in the expected format: Last " +
                        "Name, First Name, <Optional> Middle Name", name));
            }
        }
        else {
            throw new IllegalArgumentException(String.format("Entry: %s is not in the expected format: " +
                    "Name<tab>Age<tab>Phone Number", line));
        }
    }


}
