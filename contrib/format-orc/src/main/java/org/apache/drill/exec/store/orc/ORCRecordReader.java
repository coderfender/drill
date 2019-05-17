package org.apache.drill.exec.store.orc;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.exception.OutOfMemoryException;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.dfs.DrillFileSystem;
import org.apache.drill.exec.store.dfs.easy.FileWork;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

public class ORCRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(ORCRecordReader.class);
  private static final int MAX_RECORDS_PER_BATCH = 10000;
  private static final String FAILED_TO_OPEN = "failed to open input file ";

  private final DrillFileSystem drillFileSystem;
  private final FileWork fileWork;
  private final String userName;
  private BufferedReader bufferedReader;
  private DrillBuf drillBuf;
  private VectorContainerWriter vectorContainerWriter;
  private ORCFormatConfig orcFormatConfig;
  private int maxErrors;
  private boolean flattenStructuredData;
  private int errorCount;
  private int lineCount;
  private List<SchemaPath> projectedColumns;
  private String line;
  private Path path;


  public ORCRecordReader(FragmentContext context,
                         DrillFileSystem drillFileSystem,
                         FileWork fileWork,
                         List<SchemaPath> columns,
                         String userName,
                         ORCFormatConfig orcFormatConfig,
                         Path path) throws OutOfMemoryException {

    this.fileWork = fileWork;
    this.drillFileSystem = drillFileSystem;
    this.userName = userName;
    this.orcFormatConfig = orcFormatConfig;
    this.maxErrors = orcFormatConfig.getMaxErrors();
    this.drillBuf = context.getManagedBuffer().reallocIfNeeded(MAX_RECORDS_PER_BATCH);
    this.projectedColumns = columns;
    this.flattenStructuredData = orcFormatConfig.getFlattenedStructureData();

    setColumns(columns);

  }


  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    openFile();
    this.vectorContainerWriter = new VectorContainerWriter(output);
  }

  private void openFile(){

    InputStream inputStream;
    try{

      inputStream = drillFileSystem.open(fileWork.getPath());
    }

    catch(Exception e){

      throw UserException
        .dataWriteError(e)
        .message(FAILED_TO_OPEN + fileWork.getPath().toString())
        .build(logger);
    }

    this.lineCount = 0;

    bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
  }

  @Override
  public int next() {

    this.vectorContainerWriter.allocate();
    this.vectorContainerWriter.reset();

    int recordCount = 0;

    BaseWriter.MapWriter map = this.vectorContainerWriter.rootAsMap();
    String line = null;

    try{

      while ( recordCount < MAX_RECORDS_PER_BATCH && (this.bufferedReader.readLine() != null)){

        if (line.trim().length() == 0){
          continue;
        }

      }
    }
    catch (Exception e){

      throw UserException
            .dataReadError(e)
            .message("unable to read record ")
            .build(logger);
    }


    return 0;
  }

  @Override
  public void close() throws Exception {

    this.bufferedReader.close();

  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projected){

    Set<SchemaPath> transformedCols = new LinkedHashSet<>();

    if (!(isStarQuery())){
      for (SchemaPath col: projected){
        transformedCols.add(col);
      }
    }
    else {
      transformedCols.add(SchemaPath.STAR_COLUMN);
    }

    return transformedCols;
  }


  private void readORCFile(String path){

    try {

      Reader orcReader = OrcFile.createReader(new Path(path),OrcFile.readerOptions(this.drillFileSystem.getConf()));
      String codec = orcReader.getCompressionKind().toString();
      String schemaString = orcReader.getSchema().toString();
      System.out.print("schema fetched as " + schemaString);
      org.apache.orc.RecordReader rows = orcReader.rows();
      VectorizedRowBatch batch = orcReader.getSchema().createRowBatch();


      while (rows.nextBatch(batch)){

        for (int r=0; r< batch.size; r++){

          System.out.println(r);
        }

      }

      rows.close();

    } catch (IOException e) {

      UserException
        .dataReadError(e)
        .message("unable to read ORC file at path %s",fileWork.getPath())
        .addContext("User name ",this.userName)
        .build(logger);
    }

  }
}
