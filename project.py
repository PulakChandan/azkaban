from gdpprojectbuilder import ProjectBuilder
from gdpprojectbuilder.models import RSeries2XlargeCluster
from consumer_marts import __version__
import json
import os

PROJECT_NAME = 'blackhawk_transaction'
PROJECT_MODULE = 'consumer_marts'
DEFAULT_CLUSTER_UPTIME_HOURS = 4

## Load config to get host server details
file_contents = {}
current_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'blackhawk_config.json')
with open(current_path, "r") as conf_file:
    file_contents = json.loads(conf_file.read())

for source, conf_dic in file_contents.iteritems():
    if source == "blackhawk_source_details":
        blackhawk_conf = conf_dic

username = blackhawk_conf["username"]
password = blackhawk_conf["password"]
host = blackhawk_conf["host"]
remote_dir = blackhawk_conf["remote_path"]

# PROJECT INITIAL CREATION AND SETUP
# create azkaban project instance
project = ProjectBuilder(PROJECT_NAME,
                         __file__,
                         job_module=PROJECT_MODULE,
                         job_module_version=__version__,
                         requirements=True,
                         team = "ETL-Consumer")

# add job specific project properties
project.add_project_properties('job.run.date', '$(new("org.joda.time.DateTime").minusDays(1).toString("yyyy-MM-dd"))')
project.add_project_properties('target_datetime', '${azkaban.flow.start.year}${azkaban.flow.start.month}${azkaban.flow.start.day}00')
project.add_project_properties('failure.emails', 'gdp-alerts@grubhub.com')
project.add_project_properties('job.deploy.mode',  'client')
project.add_project_properties('job.num.executors',  '10')
project.add_project_properties('job.executor.memory',  '19G')
project.add_project_properties('job.driver.memory',  '5G')
project.add_project_properties('job.executor.cores',  '6')
project.add_project_properties('job.host.username',  username)
project.add_project_properties('job.host.password',  password)
project.add_project_properties('job.host',  host)
project.add_project_properties('job.remote.dir',remote_dir)

project_spark_parameters = '--executor-memory ${job.executor.memory} ' \
                           '--num-executors ${job.num.executors} ' \
                           '--executor-cores ${job.executor.cores} ' \
                           '--driver-memory ${job.driver.memory} ' \
                           '--conf spark.executor.memoryOverhead=2G ' \
                           '--conf spark.dynamicAllocation.enabled=true ' \
                           '--conf spark.shuffle.service.enabled=true ' \
                           '--conf spark.sql.shuffle.partitions=1000 ' \
                           '--deploy-mode ${job.deploy.mode} '

job_parameters = "--env ${env} " \
                 "--created_date ${job.run.date} " \
                 "--host_user ${job.host.username} " \
                 "--host_password ${job.host.password} " \
                 "--host ${job.host} " \
                 "--remote_dir ${job.remote.dir} " \
                 "--action_type ${action_type} "

job_file_dir = PROJECT_NAME
project_cluster_name = PROJECT_NAME

project_dir = "/usr/local/azkaban_projects/" + PROJECT_MODULE + "/projects/" + "${job_file_dir}" + "/pyspark/"
spark_cmd = "spark-submit ${spark_parameters} " + project_dir + "${scriptname}" + " ${job_parameters}"

project.add_bootstrap_action(cluster_name=project_cluster_name,
                                         bootstrap_action_name="blackhawk_dependencies",
                                         bootstrap_script="blackhawk_transaction_dependencies.sh",
                                         bootstrap_argument_list=[])

# initialize project flow with initial cluster
begin_job_stage_cluster_main = project.initialize_flow(cluster_name=project_cluster_name,
                                                                         model=RSeries2XlargeCluster,
                                                                         cluster_type="etl",
                                                                         uptime=DEFAULT_CLUSTER_UPTIME_HOURS,
                                                                         node_count=8,
                                                                         bootstrap_actions=[])

external_job_entitlements = {'ext_dep.project': 'GreenTEA',
                                        'ext_dep.flow': 'GreenTEA-Critical-CP-incremental',
                                        'ext_dep.target_datetime': '${target_datetime}',
                                        'ext_dep.job': 'decoder-incremental-codes_data.entitlements',
                                        'ext_dep.hours_before': '0',
                                        'ext_dep.retries': '15',
                                        'ext_dep.retry_backoff': '300000',
                                        'target_azkaban_server': 'legacy'
                                      }


wait_for_entitlements = project.add_job(name="wait_for_entitlements",
                                                   cluster_name=project_cluster_name,
                                                   job_type="external_dependency",
                                                   **external_job_entitlements)

_auto_tasks = (project.add_subflow(name='_auto_tasks',
                                   subflow_name=begin_job_stage_cluster_main,
                                   clustername=project_cluster_name,
                                   dependencies=[wait_for_entitlements]))

get_data_sftp = project.add_job(name="get_data_sftp",
                            cluster_name=project_cluster_name,
                            job_type="gdpcommand",
                            command=spark_cmd,
                            spark_parameters=project_spark_parameters,
                            job_parameters=job_parameters,
                            job_file_dir=job_file_dir,
                            scriptname="load_blackhawk.py",
                            action_type="load_sftp_data",
                            retries=15,
                            retry_backoff=300000,
                            dependencies=[_auto_tasks])

get_data_s3 = project.add_job(name="get_data_s3",
                            cluster_name=project_cluster_name,
                            job_type="gdpcommand",
                            command=spark_cmd,
                            spark_parameters=project_spark_parameters,
                            job_parameters=job_parameters,
                            job_file_dir=job_file_dir,
                            scriptname="load_blackhawk.py",
                            action_type="load_s3_data",
                            retries=15,
                            retry_backoff=300000,
                            dependencies=[_auto_tasks])

blackhawk_etl = project.add_job(name="blackhawk_etl",
                            cluster_name=project_cluster_name,
                            job_type="gdpcommand",
                            command=spark_cmd,
                            spark_parameters=project_spark_parameters,
                            job_parameters=job_parameters,
                            job_file_dir=job_file_dir,
                            scriptname="load_blackhawk.py",
                            action_type="blackawk_transaction_etl",
                            retries=15,
                            retry_backoff=300000,
                            dependencies=[get_data_sftp, get_data_s3])

# PROJECT CLEANUP STEP
project.cleanup_flow(flow_name="blackhawk_transaction",
                     cluster_name=project_cluster_name,
                     dependencies=[blackhawk_etl])

# GENERATE PROJECT
project.generate_project()
