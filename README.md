# csql2bq
Copy Cloud SQL data to BigQuery with Dataflow

## Run

```
mvn compile exec:java -Dexec.mainClass=com.souzoh.csql2bq.CloudSQLToBigQuery -Dexec.args="--runner=DataflowRunner --project={Dataflow GCP Project} \
      --tempLocation={Cloud Storage Path} --workerMachineType=n1-highmem-8 --mySQLUser={your MySQL User} --mySQLPassword={your MySQL password}" -Pdataflow-runner 
```

### Example

```
mvn compile exec:java -Dexec.mainClass=com.souzoh.csql2bq.CloudSQLToBigQuery -Dexec.args="--runner=DataflowRunner --project=souzoh-p-sinmetal-tokyo \
      --tempLocation=gs://dataflow-souzoh-p-sinmetal-tokyo/temp --workerMachineType=n1-highmem-8 --mySQLUser=root --mySQLPassword=7xnNjwDCDtm8ALav" -Pdataflow-runner
```