# FlightScope — Flight Delay Analytics

A Big Data Analytics project that processes U.S. flight records with **Apache Hadoop HDFS, YARN and Java MapReduce**, then presents the results in an interactive **Streamlit + Plotly** dashboard.

The dashboard is called **FlightScope** and includes carrier, airport, delay-cause, monthly, route, cancellation and diversion analysis, plus an interactive geographic route network.

---

## 1. Project features

### Hadoop MapReduce analytics

The project contains eight Java MapReduce jobs:

| MapReduce job | Analysis |
|---|---|
| `CarrierAvgArrivalDelay` | Average arrival delay by carrier |
| `DelayCauseTotals` | Total reported delay minutes by cause |
| `AirportAvgArrivalDelay` | Average arrival delay by airport |
| `AirportDelayRate` | Percentage of flights delayed by more than 15 minutes at each airport |
| `AirportDelayCauses` | Reported delay-cause minutes by airport |
| `MonthlyDelayTrend` | Monthly average delay, valid-flight count and delayed-flight rate |
| `RouteDelayAnalysis` | Average delay and delayed-flight rate by origin–destination route |
| `CancellationDiversionAnalysis` | Cancellation and diversion counts and rates by carrier |

### Streamlit dashboard

The dashboard provides these sections:

1. Overview
2. Carrier Analysis
3. Airport Analysis
4. Delay Causes
5. Smart Insights
6. Airport Comparison
7. Monthly Trend Analysis
8. Route Analysis
9. Cancellation & Diversion

The Route Analysis section also includes an interactive geographic route network. Airport coordinates are read from `data/airports_reference.csv`.

---

## 2. Technology stack

- **Operating system used:** Ubuntu on WSL2
- **Programming language:** Java, Python
- **Big Data processing:** Apache Hadoop 3.4.1, HDFS, YARN, MapReduce
- **Dashboard:** Streamlit
- **Data analysis:** Pandas
- **Visualizations:** Plotly
- **Java runtime:** OpenJDK 17

---

## 3. Prerequisites

Install and configure the following before running the project:

- Ubuntu / Linux environment (the development environment used WSL2)
- Java JDK 17
- Apache Hadoop 3.4.1, configured for single-node operation
- Python 3 and `venv`
- Git (optional, for cloning the repository)

The commands below assume:

- Hadoop is installed at `/usr/local/hadoop`.
- Hadoop commands such as `hadoop`, `hdfs`, `start-dfs.sh` and `start-yarn.sh` are available in your `PATH`.
- The project root contains `src/`, `data/`, `dashboard/` and `results/`.

If Hadoop is installed elsewhere, adjust the paths and environment variables to match your machine.

---

## 4. Project structure

```text
flight-delay-BDA/
│
├── src/
│   ├── CarrierAvgArrivalDelay.java
│   ├── DelayCauseTotals.java
│   ├── AirportAvgArrivalDelay.java
│   ├── AirportDelayRate.java
│   ├── AirportDelayCauses.java
│   ├── MonthlyDelayTrend.java
│   ├── RouteDelayAnalysis.java
│   └── CancellationDiversionAnalysis.java
│
├── data/
│   ├── flights.csv
│   └── airports_reference.csv
│
├── dashboard/
│   └── app.py
│
├── results/
│   ├── carrier-average.txt
│   ├── delay-cause-totals.txt
│   ├── airport-average.txt
│   ├── airport-delay-rate.txt
│   ├── airport-delay-causes.txt
│   ├── monthly-trends.txt
│   ├── route-analysis.txt
│   └── cancellation-diversion.txt
│
├── build/
├── flight-delay.jar
├── requirements.txt       # optional; create from the Python environment if not present
└── README.md
```

Some generated files (such as `build/`, the JAR and result exports) may not be committed to version control. They can be regenerated using the steps below.

---

## 5. Set up the Python environment

From the project root:

```bash
python3 -m venv venv
source venv/bin/activate
```

Install the dashboard dependencies:

```bash
pip install streamlit pandas plotly
```

If a `requirements.txt` file is included in the repository, use this instead:

```bash
pip install -r requirements.txt
```

---

## 6. Start Hadoop

Start HDFS and YARN:

```bash
start-dfs.sh
start-yarn.sh
```

Check that the Hadoop daemons are running:

```bash
jps
```

For a single-node setup, expect to see processes such as:

- `NameNode`
- `DataNode`
- `SecondaryNameNode`
- `ResourceManager`
- `NodeManager`

Check HDFS:

```bash
hdfs dfs -ls /
```

If the daemons are not running, check the Hadoop configuration and logs before submitting jobs.

---

## 7. Put the flight dataset into HDFS

Make sure the input CSV is available locally at:

```text
data/flights.csv
```

Create the HDFS input directory:

```bash
hdfs dfs -mkdir -p /flight-delay/input
```

Upload the dataset:

```bash
hdfs dfs -put data/flights.csv /flight-delay/input/
```

Verify the upload:

```bash
hdfs dfs -ls /flight-delay/input/
```

If HDFS reports that the file already exists, do not upload it again. Verify the existing file and continue.

The input CSV is expected to contain the columns used by the Java jobs, including:

- `FL_DATE`
- `OP_UNIQUE_CARRIER`
- `ORIGIN`
- `DEST`
- `ARR_DELAY`
- `CANCELLED`
- `DIVERTED`
- `CARRIER_DELAY`
- `WEATHER_DELAY`
- `NAS_DELAY`
- `SECURITY_DELAY`
- `LATE_AIRCRAFT_DELAY`

Keep the input file's column order consistent with the Java mapper implementations.

---

## 8. Compile the Java MapReduce programs

From the project root, compile all Java source files:

```bash
mkdir -p build
javac -classpath "$(hadoop classpath)" -d build src/*.java
```

Package the compiled classes into a JAR:

```bash
jar -cf flight-delay.jar -C build .
```

Confirm that the JAR exists:

```bash
ls -lh flight-delay.jar
```

If the source files use Java packages or external libraries, adapt the compile and JAR commands accordingly.

---

## 9. Run the MapReduce jobs

Each job writes to its own HDFS output directory. **Hadoop does not allow a MapReduce job to write into an output directory that already exists.**

Run these commands from the project root:

```bash
hadoop jar flight-delay.jar CarrierAvgArrivalDelay \
  /flight-delay/input/flights.csv \
  /flight-delay/output/carrier-average
```

```bash
hadoop jar flight-delay.jar DelayCauseTotals \
  /flight-delay/input/flights.csv \
  /flight-delay/output/delay-cause-totals
```

```bash
hadoop jar flight-delay.jar AirportAvgArrivalDelay \
  /flight-delay/input/flights.csv \
  /flight-delay/output/airport-average
```

```bash
hadoop jar flight-delay.jar AirportDelayRate \
  /flight-delay/input/flights.csv \
  /flight-delay/output/airport-delay-rate
```

```bash
hadoop jar flight-delay.jar AirportDelayCauses \
  /flight-delay/input/flights.csv \
  /flight-delay/output/airport-delay-causes
```

```bash
hadoop jar flight-delay.jar MonthlyDelayTrend \
  /flight-delay/input/flights.csv \
  /flight-delay/output/monthly-trends
```

```bash
hadoop jar flight-delay.jar RouteDelayAnalysis \
  /flight-delay/input/flights.csv \
  /flight-delay/output/route-analysis
```

```bash
hadoop jar flight-delay.jar CancellationDiversionAnalysis \
  /flight-delay/input/flights.csv \
  /flight-delay/output/cancellation-diversion
```

Wait for each job to report successful completion before starting the next one.

### Re-running a job

The output directory must not exist before a job starts. First inspect the existing output:

```bash
hdfs dfs -ls /flight-delay/output/
```

If you intentionally want to replace one result, remove only that specific output directory after confirming it is the correct one. For example:

```bash
hdfs dfs -rm -r /flight-delay/output/route-analysis
```

Then run the route job again. **Do not delete the HDFS input directory, and do not format HDFS.**

---

## 10. Export MapReduce results for the dashboard

Create the local results directory:

```bash
mkdir -p results
```

Download each reducer output from HDFS:

```bash
hdfs dfs -get /flight-delay/output/carrier-average/part-r-00000 results/carrier-average.txt
hdfs dfs -get /flight-delay/output/delay-cause-totals/part-r-00000 results/delay-cause-totals.txt
hdfs dfs -get /flight-delay/output/airport-average/part-r-00000 results/airport-average.txt
hdfs dfs -get /flight-delay/output/airport-delay-rate/part-r-00000 results/airport-delay-rate.txt
hdfs dfs -get /flight-delay/output/airport-delay-causes/part-r-00000 results/airport-delay-causes.txt
hdfs dfs -get /flight-delay/output/monthly-trends/part-r-00000 results/monthly-trends.txt
hdfs dfs -get /flight-delay/output/route-analysis/part-r-00000 results/route-analysis.txt
hdfs dfs -get /flight-delay/output/cancellation-diversion/part-r-00000 results/cancellation-diversion.txt
```

Verify the exported files:

```bash
ls -lh results/
```

If a local result file already exists, `hdfs dfs -get` may refuse to overwrite it. Move or rename the old local file before exporting the updated result.

---

## 11. Run the Streamlit dashboard

Activate the virtual environment if it is not already active:

```bash
source venv/bin/activate
```

Run:

```bash
streamlit run dashboard/app.py
```

Streamlit normally prints a local URL, commonly:

```text
http://localhost:8501
```

Open the URL in your browser.

The dashboard reads the exported text files from `results/` and the airport reference data from `data/airports_reference.csv`. Make sure these files exist before starting the dashboard.

---

## 12. Metric definitions and interpretation

- **Average arrival delay:** Mean of valid `ARR_DELAY` values. Negative values indicate flights arriving earlier than scheduled.
- **Delayed flight:** A flight with `ARR_DELAY > 15` minutes, as defined by this project.
- **Delayed-flight rate:** Delayed flights divided by valid arrival-delay records, multiplied by 100.
- **Cancellation rate:** Cancelled flights divided by the carrier's total flight records, multiplied by 100.
- **Diversion rate:** Diverted flights divided by the carrier's total flight records, multiplied by 100.
- **Delay-cause totals:** Accumulated minutes attributed to the delay categories in the input dataset.

Route and airport delay metrics that depend on `ARR_DELAY` exclude records where arrival delay is missing or invalid. Cancellation and diversion are calculated separately using the corresponding status fields.

Delay-cause totals are reported attributions in the dataset; they should not be interpreted as proof of causation. Rankings based on accumulated minutes can also reflect differences in flight volume. The route dashboard includes a minimum-flight threshold to reduce the influence of routes with very few observations.

---

## 13. Troubleshooting

### `JAVA_HOME is not set`

Check the Java installation:

```bash
java -version
readlink -f "$(which java)"
```

Set `JAVA_HOME` to the installed JDK directory in Hadoop's `etc/hadoop/hadoop-env.sh`.

### HDFS connection refused

Check the daemons:

```bash
jps
```

If HDFS daemons are stopped, start them:

```bash
start-dfs.sh
```

Then start YARN:

```bash
start-yarn.sh
```

Verify again with `jps` and:

```bash
hdfs dfs -ls /
```

### MapReduce output directory already exists

Choose a new output directory, or remove only the specific old output directory if you intentionally want to rerun that job. See Section 9.

### Dashboard says a result file is missing

Check the local exports:

```bash
ls -lh results/
```

Export the missing reducer output from HDFS using the commands in Section 10.

### Python module not found

Activate the virtual environment and install the dependencies:

```bash
source venv/bin/activate
pip install streamlit pandas plotly
```

### `IndentationError` or syntax error in the dashboard

Validate the file:

```bash
python -m py_compile dashboard/app.py
```

Correct the reported line before launching Streamlit.

---

## 14. Notes for contributors

- Keep the input CSV column order consistent with the Java mapper field indexes.
- Keep the HDFS output directory names consistent with the dashboard export commands.
- If a MapReduce output format changes, update the corresponding dashboard parser as well.
- Keep `airports_reference.csv` in `data/`; the route map needs airport latitude and longitude.
- Do not commit large raw datasets or generated Hadoop output folders unless repository policy requires them. Document how to obtain the dataset if it is not included.

---

## 15. Project summary

**FlightScope — Flight Delay Analytics** demonstrates a distributed data-processing workflow:

```text
Flight CSV Dataset
        |
        v
   HDFS Storage
        |
        v
 Java MapReduce Jobs
        |
        v
  HDFS Result Files
        |
        v
 Local Result Exports
        |
        v
 Python + Pandas
        |
        v
 Streamlit + Plotly Dashboard
```

The project combines distributed storage and batch processing with interactive visual analytics to explore flight delays, airport and carrier performance, routes, cancellations and diversions.
