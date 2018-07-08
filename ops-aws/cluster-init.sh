#!/bin/bash

bucket=
jar=
bootstrap=
instance_type=
instance_count=
keyname=
loguri=
region=

echo aws emr create-cluster                                                   \
    --applications Name=Ganglia Name=Spark Name=Zeppelin                      \
    --ec2-attributes \'{                                                      \
        \"KeyName\": \""$keyname"\",                                          \
        \"InstanceProfile\": \"EMR_EC2_DefaultRole\",                         \
        \"SubnetId\": \"subnet-d5e88cda\",                                    \
        \"EmrManagedSlaveSecurityGroup\": \"sg-010cef2262e4e2eb3\",           \
        \"EmrManagedMasterSecurityGroup\": \"sg-06b3348aacbf2f71e\"           \
    }\'                                                                       \
    --service-role EMR_DefaultRole                                            \
    --release-label emr-5.15.0                                                \
    --name 'InfoFlow'                                                         \
    --instance-groups \'[                                                     \
        {                                                                     \
            \"InstanceCount\": "$instance_count",                             \
            \"EbsConfiguration\": {                                           \
                \"EbsBlockDeviceConfigs\": [                                  \
                    {                                                         \
                       \"VolumeSpecification\": {                             \
                            \"SizeInGB\": 32,                                 \
                            \"VolumeType\": \"gp2\"                           \
                        },                                                    \
                        \"VolumesPerInstance\": 1                             \
                    }                                                         \
                ]                                                             \
            },                                                                \
            \"InstanceGroupType\": \"CORE\",                                  \
            \"InstanceType\": \"$instance_type\",                             \
            \"Name\": \"Core Instance Group\"                                 \
        },                                                                    \
        {                                                                     \
            \"InstanceCount\": 1,                                             \
            \"EbsConfiguration\": {                                           \
                \"EbsBlockDeviceConfigs\": [                                  \
                    {                                                         \
                        \"VolumeSpecification\": {                            \
                            \"SizeInGB\": 32,                                 \
                            \"VolumeType\": \"gp2\"                           \
                        },                                                    \
                        \"VolumesPerInstance\": 1                             \
                    }                                                         \
                ]                                                             \
            },                                                                \
            \"InstanceGroupType\": \"MASTER\",                                \
            \"InstanceType\": \"$instance_type\",                             \
            \"Name\": \"Master Instance Group\"                               \
        }                                                                     \
    ]\'                                                                       \
    --region "$region"                                                        \
    --bootstrap-actions Path=\""s3://$bucket/$bootstrap"\"                    \
    --steps \'[                                                               \
        {                                                                     \
            \"Name\": \"InfoFlow\",                                           \
            \"Jar\": \"command-runner.jar\",                                  \
            \"ActionOnFailure\": \"TERMINATE_CLUSTER\",                       \
            \"Args\": [                                                       \
                \"spark-submit\",                                             \
                \"--class\", \"InfoFlowMain\",                                \
                \""s3://$bucket/$jar"\",                                      \
                \""/home/hadoop/config.json"\"                                \
            ]                                                                 \
        }                                                                     \
    ]\'                                                                       \
    --auto-terminate                                                          \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION                        \
    | bash
