<h1>Udacity Data Enginnering Capstone</h1>

<h2>Introduction</h2>
<p>There are many casinos in Las Vegas in the USA which are making plans to appeal international tourists to play and live in their hotels. They want to analyze statistics of their customers first by digging information from USA I94 immigration data.</p>

<p>To create a dataset they can analyze with, I collect data from USA I94 immigration data of 2016 and world temperature data. After preprocessing data and filtering useful information with spark. I store the dataset in S3 Bucket.</p>

<p>This dataset can also be used by other American city to analyze their international tourists.</p>

  
<h2>Procedures</h2>
  <p>To preprocess and filter data using spark.</p>
  <p>To build pipelines using Airflow to moniter and control each step</p>
  <p>To store refined dataset to S3 Bucket</p>
  
<h2>Preprocessing steps</h2>
  <h3>Immigration table</h3>
    <ol>
      <li>Replace recerence number in raw immigration table with information from I94_SAS_Labels_Descriptions.SAS file.</li>
      <li>Split port column with city_port and state_port columns.</li> 
      <li>Reformat arrival date and departure date<./li>
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
    <p>Include 5 columns: year, month, city, average_temperature,	uncertainty</p>	
    <p>year, month, city are primary keys</p>
    <p>uncentainty: the 95% confidence interval around the average of city temperature.</p>
    
  <h3>Airline table</h3>
    <p>Include 5 columns: id(primary key), full_name, code, country, finantial</p>


  
  
<h2>Getting Started</h2>


<h3>Prerequisites</h3>
<ol>
    <li>Python 2.7 or above.</li>
    <li>configparser and pyspark package</li>
    <li>AWS Account.</li>
    <li>Set your AWS access and secret key in the config file.</li>
    <p>[AWS]<br/>
    AWS_ACCESS_KEY_ID = [your aws key]<br/>
    AWS_SECRET_ACCESS_KEY = [your aws secret]</p>
</ol>

<h2>Purpose</h2>

<p>Their plan includes</p>
  <ul>
    <li>Making some discounts on international flight tickets landing Las Vegas</li>
      <p>They need to know by which airline tourists fly to Las Vegas.</p>
    <li>Opening some potential hot internagional flight route.</li>
      <p>They need to know information of tourists whose landing port is not Las Vegas but address is Las Vegas.</p>
    <li>Analyzing the temperature influence to the tourists number</li>
      <p>They need to know tourists and temperature each month</p>
  </ul>
