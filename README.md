# S3AppDatabase
A free, no-frills NoSQL datastore. Store your data in S3, compute from anywhere with a local REST API.

<img src="https://cmdimkpa.github.io/S3AppDatabase.png"/>

## Getting Started

### 1. Clone this project

Clone this project to a suitable directory on your local server.

### 2. Configure your S3 Bucket

Edit the file `s3config.json` with the required information for your target S3 bucket (this is where your database objects will be stored).
 - You can ignore the `iostreamer` setting, but ensure you provide the rest.

### 3. Install Redis on your local server

Download and install Redis on your local Server. Assuming Ubuntu (please adjust for other Linux distros as required):
```
sudo apt update
sudo apt-get install redis-server
sudo service redis-server start
```

### 4. Run the Database Services

You will need to use a `runner`, and I recommend `Forever`.

From the project root, run the following commands:

```
sudo npm install -g forever
sudo forever start -c node redis-worker.js
sudo forever start -c node s3appdb.js
```
### 5. Review the API Documentation
