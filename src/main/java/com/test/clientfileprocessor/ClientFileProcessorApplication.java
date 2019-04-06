package com.test.clientfileprocessor;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.util.Arrays;

@Slf4j
public class ClientFileProcessorApplication {
    public static void main(String[] args){
        System.out.println("Args: " + Arrays.asList(args));
        if ( args.length == 3 ){
            int maxRowsProcessedPerMoment = Integer.valueOf(args[0]);
            String inputFilePath = args[1];
            String outputFilePath = args[2];
            ClientFileReader clientFileReader = new ClientFileReader(inputFilePath, outputFilePath,
                    maxRowsProcessedPerMoment);
            try {
                clientFileReader.sortInputFileAndOutput();
            } catch (IOException e) {
                log.error("", e);
            }

        }
        else {
            throw new IllegalArgumentException(
                    String.format(
                            "Unable to process input arguments: %s" +
                    "\nExpected <Maxnames> <Input File Name> <Output File Name>", StringUtils.join(args, "\n\t")));
        }
    }

}
