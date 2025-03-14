# data-breakable-toy

This is a project to gain knowledge and improve skills in the area of data engineering dealing with ELT pipelines in batch and streaming data with the use cloud technologies. In this case this project was developed using Databricks Data Intelligence Platform and AWS Kinsesis.

## App Architecture

<p align="center">
  <img width="709" alt="Screenshot 2025-03-14 at 4 47 10 p m" src="https://github.com/user-attachments/assets/c841231d-5d01-4508-9b97-343edc35a5c4" />
<p />
 
## Prerequisites

* Create a IAM role in AWS with:
  * AmazonKinesisFullAccess permission.
  * Create an Access Key and Secret Key to the user.
* Have a Databrics DI Workspace, If case you dont have one follow the next steps:
  * Go to https://login.databricks.com/?dbx_source=www&itm_data=databricks-web-nav&intent=SIGN_UP&l=en-EN&rl_aid=97338e1a-3b2b-4022-a7bb-96f44b1c3ca1&tuuid=b528321b-d161-4d86-820a-834532d28fa3
  * Click on Express Setup and register with an email to receive a a free trial.

> [!NOTE]
> The Free Trial has some limitations, not allowing you to create your own compute cluster and work only with a serverless cluster. To see the limitations of a serverless cluster you can visit: https://docs.databricks.com/aws/en/compute/serverless/limitations

Follow the next steps to set up the project:

## Clonning the Repo into Databricks Workspace

* On the left panel click on **Workspace** > Right Click everywhere inside the workspace > Select **Create** > Select **Git Folder** > in the **Git Repository URL** paste https://github.com/alonsofabila-dev/data-breakable-toy.git > Click on **Create Git Folder**


## Batch processing

1. Set up a volume:

  * Creating a volume to Store data in Unity Catalog:
    * On the left panel click on **Catalog** > Click on **Workspace (catalog)** > Click on **Defaul (schema)**

    * Once in the default schema in the right upper corner Click on the **Create** dropdown and select volume.

    * Give the volume a name and select **Managed volume**

    * Once the volume is created, Click the button **Upload to this volume** located in the right upper corner.

    * Explore and Select or Drag and Drop the social_media_engagement.csv file and click the **Upload** button.
 
2. Creating the Batch ETL pipeline:

  * Creating a Job Workflow to process batch data:
    * On the left panel click on **Workflows** 

    * In the right upper corner Click on the **Create Job**.

      1. Give the task a name according to the notebook to append.

      2. In the path Select the one where you cloned the repository and select the **Ingest Data** notebook.

        * For the **Ingest Data and Data Exploratory Notebook Tasks** in the task form under parametes create one with the key **'source'** and the value being the path to the social_media_engagement.csv file located in the workspace catalog.

      3. Click on the **Create Task** button.

      4. Click on the **Add Task** button and select **Notebook**.

        * You'll notice that now in the form **Dependes on** input now the previous taks is selected, this means that the subsucuent task will not begin until the previous task has finished.

        * For the **Data Exploratory Notebook Task** remove the dependens on input and for the **Transform Data Notebook Task** add the data exploratory notebook task as a dependency along with the ingest data notebook task.

      5. After finishing seting up the workflow click on the **Run Now** button on the right upper corner.

> [!NOTE]
> Repeat the step 'a' to 'd' for every notebook under the Batch Data Pipeline folder in the clonned repository until you reach a workflow graph like this one:
   
<img width="666" alt="Screenshot 2025-03-14 at 2 49 54 p m" src="https://github.com/user-attachments/assets/1898f3a0-4b87-4972-aeb6-c6a46aa38d9e" />


## Stream processing

1. Set up a Pipeline:

  * Creating a Pipeline to process stream:

    * On the left panel click on **Pipelines** located under the Data Engineering menu > Click on **Create Pipeline**  dropdown > Select **ETL pipeline**.

    * Give the pipeline a name.

    * Select **Continous** as pipeline mode.

    * under the Source Code block select the path to the **Streaming ETL Pipeline** notebook located in the clonned repository.

    * For the destination Select the **Workspace** Catalog and the **default** schema.

    * In the Advance configuration you'll need to create four key/value pair one for each of the following with the same name, just make sure you use your own values:

      * awsAccessKeyId
      * awsSecretKey
      * kinesisStreamName
      * kinesisRegion
      
    * Click on the **Create** button and It will start ingesting data from kinesis.

## Data Dashboard

1. Seting up a Dashboard in Databricks:

    * Once the batch and streaming data has been processed. On the left panel click on **Dashboards** located under the SQL menu.

    * In the right upper corner Click the **arrow dropdown** next to the **Create dashboard** button and Select **Import Dashboard from File**.

    * Select the file **Social Media Engagement.lvdash.json** from the clonned repository.

    * Click on **Import Dashboard**
  
   ### Sample of the generated report in the Dashboard

   <img width="945" alt="Screenshot 2025-03-14 at 3 58 22 p m" src="https://github.com/user-attachments/assets/5d02ac7a-7869-4b5e-b113-171407e65b3c" />












