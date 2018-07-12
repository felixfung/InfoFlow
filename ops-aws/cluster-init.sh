#!/bin/bash

#=============================================================================
# Configuration variables
#=============================================================================

keyname=
region=
subnet=
slave_security_group=
master_security_group=

instance_type=
instance_count=
ebs_size=

bootstrap=
num_executors=
executor_cores=
driver_memory=
executor_memory=
bucket=
jar=

#=============================================================================
# Below is script
#=============================================================================

core_count=$(expr "$instance_count" - 1)
if [ $core_count -gt 0 ]; then
    core_node="{                                                              \
        \"InstanceCount\": $core_count,                                       \
        \"EbsConfiguration\": {                                               \
            \"EbsBlockDeviceConfigs\": [                                      \
                {                                                             \
                   \"VolumeSpecification\": {                                 \
                        \"SizeInGB\": $ebs_size,                              \
                        \"VolumeType\": \"gp2\"                               \
                    },                                                        \
                    \"VolumesPerInstance\": 1                                 \
                }                                                             \
            ]                                                                 \
        },                                                                    \
        \"InstanceGroupType\": \"CORE\",                                      \
        \"InstanceType\": \"$instance_type\",                                 \
        \"Name\": \"Core Instance Group\"                                     \
    },"
fi

echo aws emr create-cluster                                                   \
    --applications Name=Ganglia Name=Spark Name=Zeppelin                      \
    --ec2-attributes \'{                                                      \
        \"KeyName\": \""$keyname"\",                                          \
        \"InstanceProfile\": \"EMR_EC2_DefaultRole\",                         \
        \"SubnetId\": \"$subnet\",                                            \
        \"EmrManagedSlaveSecurityGroup\": \""$slave_security_group"\",        \
        \"EmrManagedMasterSecurityGroup\": \""$master_security_group"\"       \
    }\'                                                                       \
    --service-role EMR_DefaultRole                                            \
    --release-label emr-5.15.0                                                \
    --name 'InfoFlow'                                                         \
    --instance-groups \'[                                                     \
        "$core_node"                                                          \
        {                                                                     \
            \"InstanceCount\": 1,                                             \
            \"EbsConfiguration\": {                                           \
                \"EbsBlockDeviceConfigs\": [                                  \
                    {                                                         \
                        \"VolumeSpecification\": {                            \
                            \"SizeInGB\": $ebs_size,                          \
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
                \"--num-executors\", \"$num_executors\",                      \
                \"--executor-cores\", \"$executor_cores\",                    \
                \"--driver-memory\", \"$driver_memory\",                      \
                \"--executor-memory\", \"$executor_memory\",                  \
                \"--class\", \"InfoFlowMain\",                                \
                \""s3://$bucket/$jar"\",                                      \
                \""/home/hadoop/config.json"\"                                \
            ]                                                                 \
        }                                                                     \
    ]\'                                                                       \
    --auto-terminate                                                          \
    --scale-down-behavior TERMINATE_AT_TASK_COMPLETION                        \
    | bash
