#!/bin/bash

# Create virtual Python environment
python -m venv .venv
source .venv/bin/activate

# Install Python requirements
pip install -r requirements.txt

# Start mock API server
FLASK_APP="mock-aws-lambda-extensions-api" flask run
