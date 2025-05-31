# Databricks notebook source
import os
import subprocess
import sys

from dbacademy import LessonConfig

subprocess.check_call(
    [
        sys.executable,
        "-m",
        "pip",
        "install",
        "git+https://github.com/databricks-academy/user-setup",
    ]
)


LessonConfig.configure(
    course_name="Databases Tables and Views on Databricks", use_db=False
)
LessonConfig.install_datasets(silent=True)
