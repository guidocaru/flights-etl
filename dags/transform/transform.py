def transform_data(**context):
    data_pulled = context["ti"].xcom_pull(key="my_key")
    print("data desde transform", data_pulled)
