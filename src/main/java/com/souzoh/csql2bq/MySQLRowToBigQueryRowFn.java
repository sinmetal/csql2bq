package com.souzoh.csql2bq;

import com.google.api.services.bigquery.model.TableRow;
import com.souzoh.csql2bq.model.SampleModel;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

import java.util.logging.Logger;

public class MySQLRowToBigQueryRowFn extends DoFn<SampleModel, TableRow> {

    static Logger logger = Logger.getLogger(MySQLRowToBigQueryRowFn.class.getSimpleName());

    @ProcessElement
    public void processElement(ProcessContext c) {
        TableRow row = new TableRow();
        row.put("Id", c.element().id);
        row.put("UUID", c.element().uuid);
        c.output(row);
    }
}
