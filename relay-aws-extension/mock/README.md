## Mocking the AWS Lambda environment

### Prerequisites

- Make sure you set the AWS environment variable to localhost port 5000:

  ```bash
  export AWS_LAMBDA_RUNTIME_API=localhost:5000
  ```

  (You can also use https://direnv.net/ for setting environment variables.)

- Make sure you have a Python 3 version installed and running on your machine.

### Running the Mock AWS extensions API

Then open a terminal and start our little server, that mocks the AWS Lambda API being run on localhost:5000:

```bash
cd mock-aws-lambda-extensions-api
./run.sh
```

The `run.sh` will

- create a Python virtual environment
- install necessary Python libraries
- start the mock API on [http://localhost:5000](http://localhost:5000)
