#!/bin/bash

bucket=
config=
user='hadoop'

aws s3 cp "s3://$bucket/$config" "/home/$user"
