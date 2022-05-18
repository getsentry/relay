from flask import Flask, request
from time import sleep


app = Flask(__name__)

base_url = "/2020-01-01/extension"

LAMBDA_EXTENSION_ID = "mockedExtensionID"
HEADERS = {
    "Lambda-Extension-Identifier": LAMBDA_EXTENSION_ID,
}

next_counter = 0
SHUTDOWN_MODE = True
SHUTDOWN_MAX = 5


@app.route("/")
def main():
    data = {
        "register": f"{base_url}/register",
        "next-event": f"{base_url}/event/next",
        "init-error": None,
        "exit-error": None,
    }
    return data


@app.route(f"{base_url}/register", methods=["POST"])
def register():
    request_body = request.get_json()
    print(f"Register request body: {request_body}")

    data = {
        "functionName": "helloWorld",
        "functionVersion": "X.X.X",
        "handler": "lambda_function.lambda_handler",
    }
    return (data, HEADERS)


@app.route(f"{base_url}/event/next")
def next_event():
    global next_counter
    next_counter += 1
    if SHUTDOWN_MODE and next_counter == SHUTDOWN_MAX:
        data = {
            "eventType": "SHUTDOWN",
            "deadlineMs": 1581512138111,
            "shutdownReason": "OOPSIES",
        }
    else:
        sleep(1)
        data = {
            "eventType": "INVOKE",
            "deadlineMs": 1581512138111,
            "requestId": "aws-request-ID",
            "invokedFunctionArn": "invoked-function-arn",
            "tracing": {
                "type": "X-Amzn-Trace-Id",
                "value": "Root=1-5759e988-bd862e3fe1be46a994272793;Parent=53995c3f42cd8ad8;Sampled=1",
            },
        }
    return (data, HEADERS)


@app.route(f"{base_url}/init/error", methods=["POST"])
def init_error():
    raise NotImplementedError(f"Endpoint {base_url}/init/error not yet implemented")


@app.route(f"{base_url}/exit/error", methods=["POST"])
def exit_error():
    raise NotImplementedError(f"Endpoint {base_url}/exit/error not yet implemented")
