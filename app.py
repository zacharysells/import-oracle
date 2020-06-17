#!/usr/bin/env python3

from aws_cdk import core

from import_oracle.import_oracle_stack import ImportOracleStack


app = core.App()
ImportOracleStack(app, "import-oracle")

app.synth()
