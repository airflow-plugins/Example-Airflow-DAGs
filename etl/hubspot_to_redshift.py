"""
Hubspot to Redshift

This files contains three dags:
    - A monthly backfill from Jan 1, 2010.
    - A daily backfill from Jan 1, 2018.
    - An ongoing hourly workflow.

Each DAG makes use of two custom operators:
    - HubspotToS3Operator
    https://github.com/airflow-plugins/hubspot_plugin/blob/master/operators/hubspot_to_s3_operator.py#L16
    - S3ToRedshiftOperator
    https://github.com/airflow-plugins/redshift_plugin/blob/master/operators/s3_to_redshift_operator.py#L13

This dag pulls the following endpoints and inserts data to the following
table/subtable based on the followings schedules:

NOTE: Only endpoints with the appropriate scope will be included in this dag.
The associated scope to the varius endpoints can be found in the "scope" field
within the endpoints array below.

The scope available to a given token can be found by passing the associated
token to: https://api.hubapi.com/oauth/v1/access-tokens/{OAUTH_TOKEN}

NOTE: The contacts table and associated subtables are built based on an
incrementing contact id that is stored as an Airflow Variable with the
naming convention "INCREMENTAL_KEY__{DAG_ID}_{TASK_ID}_vidOffset" at the end
of each run and then pulled on the next to be used as an offset. As such,
while accessing the Contacts endpoint, "max_active_runs" should be set to 1 to
avoid pulling the same incremental key offset and therefore pulling the same
data twice.

- Campaigns - Rebuild
- Companies - Rebuild
- Contacts - Append - Built based on incremental contact id
    - Form Submissions - Append
    - Identity Profiles - Append
    - List Memberships - Append
    - Merge Audits - Append
- Deals - rebuild
    - Associations_AssociatedVids - Append
    - Associations_AssociatedCompanyVids - Append
    - Associations_AssociatedDealIds - Append
- Deal Pipelines - Rebuild
- Engagments - Rebuild
    - Associations - Rebuild
    - Attachments - Rebuild
- Events - Append - Built based on incremental date
- Forms - Rebuild
    - Field Groups - Rebuild
- Keywords - Rebuild
- Lists - Rebuild
    - Filters - Rebuild
- Owners - Rebuild
    - Remote List - Rebuild
- Social - Rebuild
- Timeline - Append - Built based on incremental date
- Workflow - Rebuild
    - Persona Tag Ids - Rebuild
    - Contact List Ids Steps - Rebuild
"""

from datetime import datetime, timedelta
from os import path

from airflow import DAG
from airflow.hooks.http_hook import HttpHook
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import HubspotToS3Operator, S3ToRedshiftOperator
from HubspotPlugin.schemas import hubspot_schema


S3_CONN_ID = ''
S3_BUCKET = ''
HUBSPOT_CONN_ID = ''
REDSHIFT_SCHEMA = ''
REDSHIFT_CONN_ID = ''


endpoints = [{"name": "campaigns",
              "scope": "content",
              "hubspot_args": {},
              "load_type": "rebuild",
              "primary_key": "id",
              "subtables": []},
             {"name": "companies",
              "scope": "contacts",
              "hubspot_args": {},
              "load_type": "rebuild",
              "primary_key": "company_id",
              "subtables": []},
             {"name": "contacts",
              "scope": "contacts",
              "hubspot_args": {},
              "load_type": "append",
              "primary_key": "vid",
              "subtables": ["formsubmissions",
                            "identityprofiles",
                            "listmemberships",
                            "mergeaudits"]},
             {"name": "deals",
              "scope": "contacts",
              "hubspot_args": {},
              "load_type": "rebuild",
              "primary_key": "deal_id",
              "subtables": ["associations_associatedvids",
                            "associations_associatedcompanyvids",
                             "associations_associateddealids"]},
             {"name": "deal_pipelines",
              "scope": "contacts",
              "hubspot_args": {},
              "load_type": "rebuild",
              "subtables": ['stages']},
             {"name": "engagements",
              "scope": "contacts",
              "hubspot_args": {},
              "load_type": "rebuild",
              "subtables": ["associations",
                            "attachments"]},
             {"name": "events",
              "scope": "content",
              "hubspot_args": {
                "startTimestamp": "{{ execution_date }}",
                "endTimestamp": "{{ next_execution_date }}"},
              "load_type": "append",
              "primary_key": "id",
              "subtables": []},
             {"name": "forms",
              "scope": "forms",
              "hubspot_args": {},
              "load_type": "rebuild",
              "subtables": ["fieldgroups"]},
             {"name": "keywords",
              "scope": "reports",
              "hubspot_args": {},
              "load_type": "rebuild",
              "subtables": []},
             {"name": "lists",
              "scope": "contacts",
              "hubspot_args": {},
              "load_type": "rebuild",
              "primary_key": "internal_list_id",
              "subtables": ["filters"]},
             {"name": "owners",
              "scope": "contacts",
              "hubspot_args": {},
              "load_type": "rebuild",
              "primary_key": "owner_id",
              "subtables": ["remote_list"]},
             {"name": "social",
              "scope": "social",
              "hubspot_args": {},
              "load_type": "rebuild",
              "primary_key": "channel_guid",
              "subtables": []},
             {"name": "timeline",
              "scope": "timeline",
              "hubspot_args": {
                "startTimestamp": "{{ execution_date }}",
                "endTimestamp": "{{ next_execution_date }}"},
              "load_type": "append",
              "subtables": ["changes"]},
             {"name": "workflows",
              "scope": "automation",
              "hubspot_args": {},
              "load_type": "rebuild",
              "primary_key": "id",
              "subtables": ["persona_tag_ids",
                            "contact_list_ids_steps"]}]

hourly_id = '{}_to_redshift_hourly'.format(HUBSPOT_CONN_ID)
daily_id = '{}_to_redshift_daily_backfill'.format(HUBSPOT_CONN_ID)
monthly_id = '{}_to_redshift_monthly_backfill'.format(HUBSPOT_CONN_ID)

COPY_PARAMS = ["COMPUPDATE OFF",
               "STATUPDATE OFF",
               "JSON 'auto'",
               "TIMEFORMAT 'epochmillisecs'"
               "TRUNCATECOLUMNS",
               "region as 'us-east-1'"]


def create_dag(dag_id,
               schedule,
               hubspot_conn_id,
               redshift_conn_id,
               redshift_schema,
               default_args,
               catchup=False,
               max_active_runs=3):

    try:
        h = HttpHook(method='GET', http_conn_id=hubspot_conn_id)
        pw = h.get_connection(conn_id=hubspot_conn_id).password
        response = h.run('oauth/v1/access-tokens/{0}'.format(pw))
        scopes = response.json()['scopes']

        dag = DAG(dag_id,
                  default_args=default_args,
                  schedule_interval=schedule,
                  catchup=catchup,
                  max_active_runs=max_active_runs
                  )

        with dag:
            kick_off_dag = DummyOperator(task_id='kick_off_dag')
            kick_off_dag

            tables_to_build = []

            for endpoint in endpoints:
                if endpoint['scope'] in scopes:
                    if 'backfill' in dag_id and 'startTimestamp':
                        if endpoint['hubspot_args'].keys():
                            tables_to_build.append(endpoint)
                    else:
                        tables_to_build.append(endpoint)

            for table in tables_to_build:
                HUBSPOT_ARGS = table.get('hubspot_args', {})
                TABLE_NAME = table.get('name', '')
                LOAD_TYPE = table.get('load_type', '')

                PRIMARY_KEY = None
                INCREMENTAL_KEY = None

                if 'primary_key' in table.keys():
                    PRIMARY_KEY = table['primary_key']

                if 'incremental_key' in table.keys():
                    INCREMENTAL_KEY = table['incremental_key']

                S3_KEY = ('hubspot/{0}/{1}_{2}.json'.format(
                          redshift_schema,
                          TABLE_NAME,
                          "{{ ts_nodash }}"))

                split_key = path.splitext(S3_KEY)
                LOAD_KEY = '{0}_core'.format(split_key[0])

                h = HubspotToS3Operator(task_id='hubspot_{0}_data_to_s3'
                                                .format(TABLE_NAME),
                                        hubspot_conn_id=hubspot_conn_id,
                                        hubspot_object=TABLE_NAME,
                                        hubspot_args=HUBSPOT_ARGS,
                                        s3_conn_id=S3_CONN_ID,
                                        s3_bucket=S3_BUCKET,
                                        s3_key=S3_KEY)

                kick_off_dag >> h

                if table['name'] == 'timeline':
                    pass
                else:
                    r = S3ToRedshiftOperator(task_id='hubspot_{0}_to_redshift'
                                                    .format(TABLE_NAME),
                                             s3_conn_id=S3_CONN_ID,
                                             s3_bucket=S3_BUCKET,
                                             s3_key=LOAD_KEY,
                                             origin_schema=getattr(hubspot_schema,
                                                                  TABLE_NAME),
                                             origin_datatype='json',
                                             copy_params=COPY_PARAMS,
                                             load_type=LOAD_TYPE,
                                             primary_key=PRIMARY_KEY,
                                             incremental_key=INCREMENTAL_KEY,
                                             schema_location='local',
                                             redshift_schema=redshift_schema,
                                             table=TABLE_NAME,
                                             redshift_conn_id=redshift_conn_id)

                    h >> r

                if table['subtables']:
                    for subtable in table['subtables']:

                        SUBTABLE_LOAD_KEY = '{0}_{1}'.format(split_key[0],
                                                             subtable)
                        SUBTABLE_NAME = '{0}_{1}'.format(TABLE_NAME, subtable)
                        if SUBTABLE_NAME == 'timeline':
                            SUBTABLE_NAME = TABLE_NAME

                        s = S3ToRedshiftOperator(task_id='hubspot_{0}_{1}_to_redshift'
                                                        .format(TABLE_NAME,
                                                                subtable),
                                                 s3_conn_id=S3_CONN_ID,
                                                 s3_bucket=S3_BUCKET,
                                                 s3_key=SUBTABLE_LOAD_KEY,
                                                 origin_schema=getattr(hubspot_schema,
                                                                      '{0}_{1}'.format(TABLE_NAME,subtable)),
                                                 origin_datatype='json',
                                                 load_type=LOAD_TYPE,
                                                 schema_location='local',
                                                 copy_params=COPY_PARAMS,
                                                 redshift_schema=redshift_schema,
                                                 table=SUBTABLE_NAME,
                                                 redshift_conn_id=REDSHIFT_CONN_ID)
                        h >> s

        return dag
    except:
        pass


globals()[hourly_id] = create_dag(hourly_id,
                                  '@hourly',
                                  HUBSPOT_CONN_ID,
                                  REDSHIFT_CONN_ID,
                                  REDSHIFT_SCHEMA,
                                  {'start_date': datetime(2018, 2, 13),
                                   'retries': 2,
                                   'retry_delay': timedelta(minutes=5),
                                   'email': [],
                                   'email_on_failure': True},
                                  catchup=False,
                                  max_active_runs=1)

globals()[daily_id] = create_dag(daily_id,
                                 '@daily',
                                 HUBSPOT_CONN_ID,
                                 REDSHIFT_CONN_ID,
                                 REDSHIFT_SCHEMA,
                                 {'start_date': datetime(2018, 1, 1),
                                  'end_date': datetime(2018, 2, 13),
                                  'retries': 2,
                                  'retry_delay': timedelta(minutes=5),
                                  'email': [],
                                  'email_on_failure': True},
                                 catchup=True)

globals()[monthly_id] = create_dag(monthly_id,
                                   '@monthly',
                                   HUBSPOT_CONN_ID,
                                   REDSHIFT_CONN_ID,
                                   REDSHIFT_SCHEMA,
                                   {'start_date': datetime(2010, 1, 1),
                                    'end_date': datetime(2018, 1, 1),
                                    'retries': 2,
                                    'retry_delay': timedelta(minutes=5),
                                    'email': [],
                                    'email_on_failure': True},
                                   catchup=True)
