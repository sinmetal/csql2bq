package com.souzoh.csql2bq;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.souzoh.csql2bq.model.SampleModel;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Logger;

public class CloudSQLToBigQuery {

    static Logger logger = Logger.getLogger(CloudSQLToBigQuery.class.getSimpleName());

    public interface CloudSQLToBigQueryOptions extends PipelineOptions {

        @Description("MySQL User ID")
        @Default.String("dataflow")
        String getMySQLUser();
        void setMySQLUser(String value);

        @Description("MySQL User Password")
        @Default.String("password")
        String getMySQLPassword();
        void setMySQLPassword(String value);
    }

    public static class KVToRow extends PTransform<PCollection<SampleModel>, PCollection<TableRow>> {
        @Override
        public PCollection<TableRow> expand(PCollection<SampleModel> input) {
            return input.apply(ParDo.of(new MySQLRowToBigQueryRowFn()));
        }
    }

    public static void main(String[] args) {
        CloudSQLToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(CloudSQLToBigQueryOptions.class);

        for (int i = 0; i < 1; i++) {
            run(options, i);
        }
    }

    public static void run(CloudSQLToBigQueryOptions options, Integer number) {
        SimpleDateFormat dt = new SimpleDateFormat("yyyyymmdd-hhmmss");
        options.setJobName("csql2bq-" + dt.format(new Date()) + "-" + number);
        Pipeline p = Pipeline.create(options);

        final String databaseName = "db1";
        final String instanceConnectionName = "souzoh-p-sinmetal-tokyo:asia-northeast1:sql1-replica-0222";
        String jdbcUrl = String.format(
                "jdbc:mysql://google/%s?cloudSqlInstance=%s&"
                        + "socketFactory=com.google.cloud.sql.mysql.SocketFactory",
                databaseName,
                instanceConnectionName);

        List<TableFieldSchema> fields = new ArrayList<>();
        fields.add(new TableFieldSchema().setName("Id").setType("INTEGER"));
        fields.add(new TableFieldSchema().setName("UUID").setType("STRING"));
        TableSchema schema = new TableSchema().setFields(fields);

        TableReference tableRef = new TableReference();
        tableRef.setProjectId("souzoh-p-sinmetal-tokyo");
        tableRef.setDatasetId("work");
        tableRef.setTableId("sample" + number);

        // PrimaryKeyを100個に分割して、BigQueryのTableにLoadする
        // 対象のテーブルサイズが大きいと、JdbcIOがOutOfMemoryになるため
        p.apply(JdbcIO.<SampleModel>read()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration.create(
                        "com.mysql.cj.jdbc.Driver", jdbcUrl)
                        .withUsername(options.getMySQLUser())
                        .withPassword(options.getMySQLPassword()))
                .withQuery("select id,uuid from sample where mod(id, 100) =  " + number)
                .withCoder(SerializableCoder.of(SampleModel.class))
                .withRowMapper(new JdbcIO.RowMapper<SampleModel>() {
                    public SampleModel mapRow(ResultSet resultSet) throws Exception {
                        SampleModel model = new SampleModel();
                        model.id = resultSet.getLong("id");
                        model.uuid = resultSet.getString("uuid");

                        return model;
                    }
                }))
                .apply(new KVToRow())
                .apply(BigQueryIO.writeTableRows()
                        .withSchema(schema)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .to(tableRef));

        p.run();
    }
}
