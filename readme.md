Welcome to this project.

I developed a simple example of my routine tasks in a previous job position.

This is an ETL-like process that downloads data from a free dataset,
transform it using pandas and upload it into Postgres database
for future analysis.

To try it on your own PC you don't need anything except the docker and
docker-compose tools. Perform the next steps:

1) Clone the repo to your local PC.

2) Go to the folder of the project and run "docker-compose up --build" to run 
Postgres database in a container.

3) Run "docker build -t upload_data ." 
and then "docker run -it --network=etl_sample_task_default upload_data" 
to run the main script.

4) Check the output data in console. Also you can go to "localhost:8080" 
to use pgadmin tool and check data in database.

5) Use "root@admin.com" for login and "root" for password to sign up into 
pgadmin.

6) Use host name "pgdatabase" to register the server.

7) Explore uploaded data in tables.

I don't map my containers on any volumes deliberately. So when you stop
docker-compose, all data will be lost.
