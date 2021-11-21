from googleapiclient.discovery import build


def start_dataflow(data, context):
    '''This function is used to trigger the Dataflow pipeline from Cloud Function
     when a new csv file is uploaded into the GCS bucket.
    '''
    if str(data['name']).endswith('a.csv'):
        # replace with your project ID
        project = "projectID"
        # set a unique job name
        job = project + " " + str(data['timeCreated'])
        # path to your template files stored in GCS
        template = "gs://BUCKET_NAME/templates/TEMPLATE_NAME"
        # the new file which is uploaded into the GCS bucket
        inputFile = "gs://" + str(data['bucket']) + "/" + str(data['name'])
        # the runtime parameter pass to the dataflow pipeline job
        parameters = {
            'input': inputFile,
        }
        # tempLocation is the path on GCS for writing temporaty files generated during the dataflow job
        environment = {'tempLocation': 'gs://BUCKET_NAME/temp/TEMP_NAME'}

        service = build('dataflow', 'v1b3', cache_discovery=False)
        # call the Dataflow REST APIs
        request = service.projects().locations().templates().launch(
            projectId=project,
            gcsPath=template,
            location='asia-east1',
            body={
                'jobName': job,
                'parameters': parameters,
                'environment': environment
            },
        )
        response = request.execute()
        print(str(response))
