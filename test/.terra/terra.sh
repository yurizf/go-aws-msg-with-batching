#!/bin/bash

terraform init -backend-config="bucket=yuri-terraform"

terraform plan -out terra.plan