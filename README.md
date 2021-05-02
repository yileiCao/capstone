


<h1>Udacity Data Enginnering Capstone</h1>

<h2>Introduction</h2>
<p>Millions of international tourists travel to United States every year, by routes connecting their home country and big citis in United States. The board of one big airline want to know whether the existing routes satisfied present demand. To decide whether there need to add new routes. A dataset needs to be built and to be analyzed. This project built one demo with Spark.</p>

<h2>The dataset includes data from three sourses:</h2>
<ul>
  <li><h3>i94 immigration data</h3></li>
    <p>This data comes from the US National Tourism and Trade Office. https://travel.trade.gov/research/reports/i94/historical/2016.html</p>
    <p>International tourist information come from this data.</p>
  <li><h3>World tempereture data</h3></li>
    <p>This data comes from Kaggle. https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data</p>
    <p>Tempereture of main cities in United States are extracted from this data.</p>
  <li><h3>airline data</h3></li>
    <p>This data comes from OpenFlight. https://openflights.org/data.html</p>
    <p>Airline information like country comes from this data.</p>
</ul>

  
<h2>Table preprocess steps</h2>
  <h3>Immigration table</h3>
    <ol>
      <li>Replace recerence number in raw immigration table with information from I94_SAS_Labels_Descriptions.SAS file.</li>
      <li>Split port column with city_port and state_port columns.</li> 
      <li>Reformat arrival date and departure date.</li>
      <li>Calculate day stayed of immigrator.</li>
      <li>Some immigrators' address information is missed.(49 in 1000) Replace it with state_port column.</li> 
      <li>Assume address is the state where the immigrators most want to visit. It is important in my analysis.</li>
      <p>model: the approach the immigrator take to come to america, including Air, Sea, Land and Not reported.</p>
      <li>Extract useful columns and write the table ordered by address and partitioned by month.</li>
      <li>Include 15 columns: id(primary key), year, month, resident, city_port, state_port, model, address, age, visa, gender, airline, day_stayed, arrival_date, depardure_date</li>
    </ol>
    <ul>
      <li>id:             Immigration record id.</li>
      <li>arrival_date:   Date of entry.</li> 
      <li>depardure_date: Date of leave.</li>
      <li>year, month:    year and month of entry.</li>
      <li>resident:       country where immigrator lived.</li> 
      <li>port:           port of immigration entry in the USA.</li>
      <li>model:          the approach the immigrator take to come to america, including Air, Sea, Land and Not reported.</li>
      <li>address:        the address in the USA where the immigrator can be contacted.</li>
      <li>age:            the age of immigrator.</li>
      <li>visa:           the visa type of immigrator, including Business, Pleasure, Student.</li>
      <li>gender:         the gender of immigrator.</li>
      <li>airline:        the airline code of the flight the immigrator took if he came to america by air.</li>
      <li>day_stayed:     the number of days the immigrator stayed in the USA.</li>
    </ul>
    
  
  
    
  <h3>Temperature table</h3>
    <ol>
      <li>Filter country with "United States"</li>
      <li>Filter year after 2000-01-01. Because the climate changes a lot, the statistics too long ago have no meanings.</li> 
      <li>Order the data by month and city.</li>
      <li>Extract useful columns.</li>
      <li>Include 4 columns: year, month, city, average_temperature. year, month, city are primary keys</li>
    </ol>
    
    
  <h3>Airline table</h3>
    <p>Include 5 columns: id(primary key), full_name, code, country, finantial</p>
    <p>The code column is used to join with immigration table</p>


  
  
<h2>Getting Started</h2>


<h3>Prerequisites</h3>
<ol>
    <li>Python 2.7 or above.</li>
    <li>AWS Account.</li>
    <li>AWS Command Line.</li>
</ol>


  
<h3>Procedures</h3>
<ol>
  <li>Upload data into S3</li>
  <li>Set up EMR cluster</li>
    <p>aws emr create-cluster --name spark-cluster --use-default-roles --release-label emr-5.28.0 --instance-count 3 --applications Name=Spark  --ec2-attributes KeyName=spark-cluster --instance-type m5.xlarge --instance-count 3<p>
  <li>Move data from S3 to HDFS.</li>
    <p>aws emr add-steps --cluster-id j-XXXXXXXX --steps file://./Desktop/myStep.json</p>
   <P>JSON file
     [
    {
        "Name":"S3DistCp step",
        "Args":["s3-dist-cp","--s3Endpoint=s3.amazonaws.com","--src=s3://XXXX/airline_data","--dest=hdfs:///airline_result","--srcPattern=.*[a-zA-Z,]+"],
        "ActionOnFailure":"CONTINUE",
        "Type":"CUSTOM_JAR",
        "Jar":"command-runner.jar"
    }
     ]</p>    
  <li>Upload scripts(immigration_table and tempereture table) to EMR master node and run script</li>
    <p>scp -i  XXX.pem XXX/immigration_table.py hadoop@XXXXXXXXXX.us-west-2.compute.amazonaws.com:/home/hadoop</p>
  <li>Move result from HDFS to s3.</li>
    <p>s3-dist-cp --src hdfs:///immigration_data/immigration.csv --dest s3://XXX/immigration_result/immigration.csv</p>
  <li>Use Apache Zeppelin to visualize dataset.</li>
</ol>

<h2>Example usage</h2>

<p>There are many casinos in Las Vegas in the USA which are making plans to appeal international tourists to play and live in their hotels. They want to analyze statistics of their customers first by digging information from USA I94 immigration data.</p>
<h3>Their plan includes</h3>
  <ol>
    <li>Making some discounts on international flight tickets landing Las Vegas</li>
      <p>They need to know by which airline tourists fly to Las Vegas.</p>
    <li>Opening some potential hot internagional flight route.</li>
      <p>They need to know information of tourists whose landing port is not Las Vegas but address is Las Vegas.</p>
    <li>Analyzing the temperature influence to the tourists number</li>
      <p>They need to know tourists and temperature each month</p>
  </ol>
  
<h3>Dataset Mining</h3>
<ol>
  <li>Count tourists group by the airlines they took.</li>
  <p>record.filter(record.port_state=="NEVADA").filter(record.visa=="Pleasure").groupBy(record.airline).agg(count("id").\
    alias("count")).sort(desc("count")).limit(20).join(airline, record.airline==airline.code, how = 'inner').select("airline", "count", "full name", "country").sort(desc("count"))</p>
  <img width="900" alt="example1" src="https://github.com/yileiCao/capstone/blob/main/example1.png">
  <p>With the figure above, it is easy to find business parner.</p>

  
  <li>Count and compare tourists whose port state are Nevada and tourists whose address are Nevada</li>
  <p> record.filter(record.address=="NEVADA").filter(record.visa=="Pleasure").groupBy(record.resident).agg(count("id").alias("count")).sort(desc("count")).limit(10)</p>
  <p> record.filter(record.port_state=="NEVADA").filter(record.visa=="Pleasure").groupBy(record.resident).agg(count("id").alias("count")).sort(desc("count")).limit(10)</p>
  <img width="600" alt="example2" src="https://github.com/yileiCao/capstone/blob/main/example2.png">
  <p>By comparing two tables above, there are many Japanese tourists coming to Nevada in 2016, while only very small number of them entering United States through port in Nevada. There may not be enough air routes connecting Japan and Nevada.</p>

  <li>Plot tourists number againest tempereture.</li>
    <p>tempereture1 = tempereture.filter(tempereture.City=="Las Vegas").groupBy(tempereture.month).agg(avg("AverageTemperature").alias("AverageTempereture"))</p>
    <p>record.filter(record.address=="NEVADA").filter(record.visa=="Pleasure").groupBy(record.month).agg(count("id").\
    alias("count")).join(tempereture1, "month", how = 'inner').select("AverageTempereture","count").orderBy("AverageTempereture")</p>
    
   <img width="900" alt="example3" src="https://github.com/yileiCao/capstone/blob/main/example3.png">
   <p>By comparing two tables above, it seems that tourists prefer to travel when Nevada has mild tempereture. </p>
  </ol>
  <h3>This dataset can also be used by other American city to analyze their international tourists.</h3>
  
<h2>Other Scenarios</h2>
<ol>
  <li>The data was increased by 100x.</li>
    <p>By splitting the data into several parts before running in EMR, the EMR cluster can handle this amount of data. My script ran 36 mins in EMR cluster with three nodes. A 100 larger dataset can be easily handled by adding more nodes.</p>
  <li>The pipelines would be run on a daily basis by 7 am every day.</li>
    <p>An airflow pipeline can be built to deal with this scenario.</p>
  <li>The database needed to be accessed by 100+ people.</li>
    <p>A preserved redshift cluster can be built to contain the database. The authorized user can easily access data.</p>
